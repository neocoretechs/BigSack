package com.neocoretechs.bigsack.hashmap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Stack;
import java.util.UUID;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.btree.StructureCallBackListener;
import com.neocoretechs.bigsack.io.Optr;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.keyvaluepages.KVIteratorIF;
import com.neocoretechs.bigsack.keyvaluepages.KeyPageInterface;
import com.neocoretechs.bigsack.keyvaluepages.KeySearchResult;
import com.neocoretechs.bigsack.keyvaluepages.KeyValue;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
import com.neocoretechs.bigsack.keyvaluepages.NodeInterface;
import com.neocoretechs.bigsack.keyvaluepages.RootKeyPageInterface;
import com.neocoretechs.bigsack.keyvaluepages.TraversalStackElement;

/*
* Copyright (c) 2003, NeoCoreTechs
* All rights reserved.
* Redistribution and use in source and binary forms, with or without modification, 
* are permitted provided that the following conditions are met:
*
* Redistributions of source code must retain the above copyright notice, this list of
* conditions and the following disclaimer. 
* Redistributions in binary form must reproduce the above copyright notice, 
* this list of conditions and the following disclaimer in the documentation and/or
* other materials provided with the distribution. 
* Neither the name of NeoCoreTechs nor the names of its contributors may be 
* used to endorse or promote products derived from this software without specific prior written permission. 
* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED 
* WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A 
* PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR 
* ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
* TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
* HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT 
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED 
* OF THE POSSIBILITY OF SUCH DAMAGE.
*
*/
/**
* Main HMap class.  Manipulates retrieval stack of hMapKeyPages and provides access
* to seek/add/delete functions.
* Important to note that the data is stored as arrays serialized out in key pages.
*  hash key for a given integer hash code.<p/>
* The method is to extract the first 3 bits as tablespace 0-7, then the next 5 bits (0-31) value then
* next 8 bits (256) , next 8 bits, and last 8 bits. keys to the index pages. Then the successive groups of bytes
* are used as indexes to sublists. <p/>
* The kev/value entries on the root page correspond to hash values at <tablespace>0 0 0 0 and links out form there are
* hash collisions of those values. The least significant bytes, if values at 0, are stored on the key pages, so 1F000000
* is stored in the first page of tablespace 0.
* @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
*/
public final class HMapMain implements KeyValueMainInterface {
	private static boolean DEBUG = true; // General debug, overrides other levels
	private static boolean DEBUGCURRENT = false; // alternate debug level to view current page assignment of KeyPageInterface
	private static boolean DEBUGSEARCH = false; // traversal debug
	private static boolean DEBUGCOUNT = false;
	private static boolean DEBUGDELETE = true;
	private static boolean DEBUGINSERT = true;
	private static boolean TEST = true; // Do a table scan and key count at startup
	private static boolean ALERT = true; // Info level messages
	private static boolean OVERWRITE = true; // flag to determine whether value data is overwritten for a key or its ignored
	private static final boolean DEBUGOVERWRITE = false; // notify of overwrite of value for key
	static int EOF = 2;
	static int NOTFOUND = 3;
	static int ALREADYEXISTS = 4;
	static int TREEERROR = 6;

	RootKeyPageInterface[] root = new HMapRootKeyPage[DBPhysicalConstants.DTABLESPACES];
	long numKeys = 0;

	private GlobalDBIO sdbio;
	private Stack<TraversalStackElement> stack = new Stack<TraversalStackElement>();
	private KeySearchResult lastInsertResult = null;
	/**
	 * Create the array of {@link HMap} instances for primary root pages
	 * @param globalDBIO
	 * @throws IOException
	 */
	public HMapMain(GlobalDBIO globalDBIO) throws IOException {
		if(DEBUG)
			System.out.printf("%s ctor%n",this.getClass().getName());
		this.sdbio = globalDBIO;
		// Append the worker name to thread pool identifiers, if there, dont overwrite existing thread group
		// Consistency check test, also needed to get number of keys
		// Performs full tree/table scan, tallys record count
		if( ALERT )
			System.out.println("Database "+globalDBIO.getDBName()+" ready with "+HMapRootKeyPage.MAXKEYSROOT+" keys per root page.");
	}
	
	
	@Override
	/**
	 * Populate the array of {@link RootKeyPageInterface} {@link HMapRootKeyPage}
	 * Gets a page from the pool via {@link BlockAccessIndex.getPageFromPool}. Called from {@link HMap.createRootNode}.
	 * This method attempts to link the {@link BlockAccessIndex} to the {@link KeyPageInterface} to 'this'
	 * then link the {@link HTNode} which was created by {@link HMap} and its pageId was set to 0L.<p/>
	 * We will create the array of root nodes with root pages at each tablespace.
	 * @throws IOException
	 */
	public RootKeyPageInterface createRootNode() throws IOException {
		for(int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			HMapRootKeyPage htk = GlobalDBIO.getHMapRootPageFromPool(sdbio, i);
			this.root[i] = htk;
		}
		if( DEBUG ) {
			StringBuilder sb = new StringBuilder();
			for(int i = 0; i < this.root.length; i++)
				sb.append(root[i].toString());
			System.out.printf("%s Root KeyPageInterface: %s%n",this.getClass().getName(),sb.toString());
		}
		return root[0];
	}
	
	@Override
	/**
	 * Used to create root node collision space page when coming from the inside out, NodeInterface to deep store, as in cases
	 * where the root is repositioned such as a BTree, as opposed to initial creation from initial deep store
	 * creation of buckets. So the 'root node of the root node' page is the collision space page.
	 * We acquire our BlockAccessIndex, which is rolled into our {@link HMapKeyPage} {@link HMapRootKeyPage}
	 * implementing {@link KeyPageInterface} which communicates 
	 * with our HTNode {@link NodeInterface}.
	 * @throws IOException
	 */
	public RootKeyPageInterface createRootNode(NodeInterface htNode) throws IOException {
		int tblsp = htNode.getTablespace();
		HMapKeyPage htk = GlobalDBIO.getHMapPageFromPool(sdbio, tblsp);
		htNode.setPageId(htk.getPageId());
		root[tblsp].setRootNode(htk.getBlockAccessIndex());
		if( DEBUG )
			System.out.printf("%s Root KeyPageInterface: %s%n",this.getClass().getName(),root[tblsp].toString());
		return root[tblsp];
	}
	
	@Override
	/**
	 * When a node is created from {@link HMap} a callback to this method
	 * will establish a new page from the pool
	 * by calling {@link BlockAccessIndex} overloaded static method getPageFromPool with
	 * the new node.
	 * @param hnode The new node called from HMap
	 * @return 
	 * @throws IOException
	 */
	public KeyPageInterface createNode(NodeInterface hnode) throws IOException {
		return GlobalDBIO.getHMapPageFromPool(sdbio, hnode);
	}
	
	@Override
	/**
	 * Get the BlockAccessIndex at the pageId, create an HMapKeyPage with it using the hNode
	 * with true to read the content of the block. Sets the currentPage with the retrieved page.
	 * @return 
	 */
	public KeyPageInterface getNode(NodeInterface hNode, long pageId) throws IOException {
		BlockAccessIndex bai = sdbio.findOrAddBlock(pageId);
		return new HMapKeyPage(this, bai , (HTNode) hNode, true);
	}
	
	public void test() throws IOException {
		if( TEST ) {
			// Attempt to retrieve last good key count
			long numKeys = 0;
			long tim = System.currentTimeMillis();
			numKeys = count();
			System.out.println("Consistency check for "+sdbio.getDBName()+" returned "+numKeys+" keys in "+(System.currentTimeMillis()-tim)+" ms.");
		}
	}
	
	/**
	 * Returns number of table scanned keys, sets numKeys field
	 * TODO: Alternate more efficient implementation that counts keys on pages
	 * This method scans all keys, thus verifying the structure.
	 * @throws IOException
	 */
	public synchronized long count() throws IOException {
		//System.out.println(found);
		long tim = System.currentTimeMillis();
		KVIteratorIF<Comparable, Object> iterImpl = new KVIteratorIF<Comparable,Object>() {
			@Override
			public boolean item(Comparable key, Object value) {
				++numKeys;
				return true;
			}
		};
		for(int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			retrieveEntriesInOrder( (HTNode<Comparable, Object>)((HMapKeyPage)root[i].getPage(i)).hTNode,iterImpl);
		}
		if( DEBUG || DEBUGCOUNT )
			System.out.println("Count for "+sdbio.getDBName()+" returned "+numKeys+" keys in "+(System.currentTimeMillis()-tim)+" ms.");
		// deallocate outstanding blocks in all tablespaces
		sdbio.deallocOutstanding();
		return numKeys;
	}
	
	  private KeyValue<Comparable, Object> retrieveEntriesInOrder(HTNode<Comparable, Object> kvNode, KVIteratorIF<Comparable, Object> iterImpl) throws IOException {
	        if ((kvNode == null) ||
	            (kvNode.getmCurrentKeyNum() == 0)) {
	            return null;
	        }
	        boolean bStatus;
	        KeyValue<Comparable, Object> keyVal;
	        int currentKeyNum = kvNode.getmCurrentKeyNum();
	        for (int i = 0; i < currentKeyNum; ++i) {
	            //retrieveEntriesInOrder((HTNode<K, V>) HTNode.getChildNodeAtIndex(treeNode, i), iterImpl);
	            keyVal = kvNode.getKeyValueArray(i);
	            if(keyVal == null)
	            	return null;
	            bStatus = iterImpl.item(keyVal.getmKey(), keyVal.getmValue());
	            if (bStatus) {
	                return keyVal;
	            }
	        }
	        return null;
	    }

	    //

	/**
	 * Determines if tree is empty by examining the root for the presence of any keys
	 * @return
	 */
	public synchronized boolean isEmpty() {
		try {
			return (count() == 0);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}
	/**
	* currentPage and currentIndex set by this seeker of a target object value.
	* The only physically possible way is an iteration through the entire collection until found or end.
	* @param targetObject The Object value to seek.
	* @return data Object if found. null otherwise.
	* @exception IOException if read failure
	*/
	@SuppressWarnings("rawtypes")
	public synchronized Object seekObject(Object targetObject) throws IOException {	
		//TreeSearchResult tsr = search(targetKey);
		//if (tsr.atKey) {
		//	setCurrent(tsr);
		//	return getCurrentObject();
		//} else {
		//	return null;
		//}
		//rewind();
		//if( currentPage != null ) {
			//setCurrent();
			// are we looking for first element?
			//if(currentObject.equals(targetObject))
				//return currentObject;
			//while (gotoNextKey() == 0) {
				//System.out.println(currentObject);
				//if(currentObject.equals(targetObject))
					//return currentObject;
			//}
		//}
		for(int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			search((Comparable) targetObject);
		}
		// deallocate outstanding blocks in all tablespaces
		sdbio.deallocOutstanding();
		//clearStack();
		return null;
	}
	/**
	* Seek the key, if we dont find it, leave the tree at it position closest greater than element.
	* If we do find it return true in atKey of result and leave at found key.
	* Calls search, which calls clearStack, repositionStack and setCurrent.
	* @param targetKey The Comparable key to seek
	* @return search result with key data
	* @exception IOException if read failure
	*/
	@SuppressWarnings("rawtypes")
	public synchronized KeySearchResult seekKey(Comparable targetKey) throws IOException {
		KeySearchResult tsr = search(targetKey);
		if( DEBUG || DEBUGSEARCH)
			System.out.println("SeekKeystate is targKey:"+targetKey);
		return tsr;
	}
	/**
	 * Called back from delete in BTNode to remove persistent data prior to in-memory update where the
	 * references would be lost.
	 * @param optr The pointer with virtual block and offset
	 * @param o The object that was previously present at that location
	 * @throws IOException
	 */
	public synchronized void delete(Optr optr, Object o) throws IOException {
		GlobalDBIO.deleteFromOptr(sdbio, optr, o);
	}
	
	/**
	 * Add to deep store, Set operation.
	 * @param key
	 * @return
	 * @throws IOException
	 */
	public synchronized int add(Comparable key) throws IOException {
		return add(key, null);
	}
	/**
	 * Add an object and/or key to the deep store. Traverse the colliion space pages for the insertion point and insert.<p/>
	 * We use the search of HMapKeyPage since we want to buffer the page in the KeySearchResult to retry on next insert.<p/>
	 * When we return from createKeypath we have the first keyvalue page that contains the NodeInterface HTNode, so we continue
	 * the search for an existing key from there.
	 * .<p/>
	 * @param key
	 * @param object
	 * @return 0 for key absent, 1 for key exists
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public synchronized int add(Comparable key, Object value) throws IOException {
		if(DEBUG)
			System.out.printf("%s insert key=%s value=%s%n", this.getClass().getName(), key, value);
		HMapNavigator hNav = new HMapNavigator(this, key.hashCode());
		int[] hashKeys = hNav.getHashKeys();
		HMapKeyPage lastPage = hNav.createKeypath(root[hashKeys[0]]);
		HMapKeyPage nPage = lastPage;
		KeySearchResult eligiblePage = null;
		lastInsertResult = null;
		while(nPage != null ) {
			lastInsertResult = nPage.search(key);
			if(lastInsertResult.atKey) {
				// set to update value, we have found the key
				update(lastInsertResult, key, value);
				return 1;
			}
			if(nPage.getNumKeys() < HMapKeyPage.MAXKEYS) {
				eligiblePage = lastInsertResult;
			}
			// didnt find the key, proceed to next page of collision space if any
			if(nPage.nextPage == null) {
				if(nPage.nextPageId != -1L) {
					nPage.nextPage = GlobalDBIO.getHMapPageFromPool(sdbio, nPage.nextPageId);
					lastPage = nPage;
					nPage = (HMapKeyPage) nPage.nextPage;
				} else {
					break; // end of search
				}
			}	
		}
		// if eligiblePage is null we never encountered an eligible page
		// if not null we we may have found a page that did not contain the key, yet had space to insert one
		if(eligiblePage != null) {
			lastInsertResult = update(eligiblePage.page, key, value);
			return 1;
		}
		lastInsertResult = update(lastPage, key, value);
		// have to link new page to last page;
		if(DEBUG)
			System.out.printf("%s insert exit key=%s value=%s%n", this.getClass().getName(), key, value);
		return 0;
	}

	/**
	 * Update the page and insert the key/value at the end of existing entries. We assume the collision space has been searched
	 * and the key was not found and we are left at a page that has space to insert the key/value.
	 * @param key The key to insert
	 * @param object the object data to insert, if null, ignore data put and return true. If OVERWRITE flag false, also ignore
	 * @return The KeySearchResult of the final insertion point
	 * @throws IOException
	 */
    private synchronized KeySearchResult update(KeyPageInterface sourcePage, Comparable key, Object value) throws IOException {
    	int nKeys = sourcePage.getNumKeys();
    	int insertPos = nKeys;
    	++nKeys;
    	KeyValue kv = new KeyValue(key, value, ((HMapKeyPage)sourcePage).hTNode);
    	kv.setKeyUpdated(true);
    	kv.setValueUpdated(true);
    	sourcePage.setNumKeys(nKeys);
    	((HMapKeyPage)sourcePage).hTNode.setKeyValueArray(insertPos, kv);
    	sourcePage.putPage();
        return new KeySearchResult(sourcePage, insertPos, true);
    }
    /**
     * Update based on key search result. We assume we are at the found key and KeySearchResult is atKey, we have the
     * page we searched to find the key and the index of that key in the page.
     * @param ksr The KeySearchResult
     * @param key Original key
     * @param value Value
     * @throws IOException
     */
    private synchronized void update(KeySearchResult ksr, Comparable key, Object value) throws IOException {
		if( !OVERWRITE ) {
			if(ALERT)
				throw new IOException("OVERWRITE flag set to false, so attempt to update existing value is ignored for key "+key);
			return;
		}
    	KeyValue keyValue = ksr.page.getKeyValueArray(ksr.insertPoint);
    	if( keyValue == null )
    		throw new IOException("Key/Value retrieval failed for previously searched page:"+ksr+" for key:"+key);
    	if(value != null && value.equals(keyValue.getmValue()) ) 
    		return;
    	if(!keyValue.getValueOptr().isEmptyPointer()) {
    	  	if( DEBUG || DEBUGOVERWRITE )
    			System.out.println("OVERWRITE value "+value+" for key "+key+" index["+ksr.insertPoint+"] page:"+ksr.pageId);
    		sdbio.delete_object(keyValue.getValueOptr(), GlobalDBIO.getObjectAsBytes(keyValue.getmValue()).length);
    		keyValue.setValueOptr(Optr.emptyPointer);
    	}
    	((HMapKeyPage) ksr.page).putDataToArray(value,ksr.insertPoint);
    	keyValue.setValueUpdated(true);
    	ksr.page.putPage();
    }
    
    /**
  	* set up keysearchresult for further operations
     * @param node
     * @param key
     * @return
     * @throws IOException
     */
    public synchronized KeySearchResult locate(Comparable key) throws IOException {
		int[] hashkeys;
		// element 0 is our tablespace, start there, find a collision space page to hold the new key
		HMapNavigator hNav = new HMapNavigator(this, key);
		hashkeys = hNav.getHashKeys();
		lastInsertResult = hNav.search(root[hashkeys[0]]);
		if(DEBUG)
			System.out.printf("%s insert exit key=%s value=%s%n", this.getClass().getName(), key);
		return lastInsertResult;
    }
  
    /**
     * Search the collision space as opposed to key space.
     * We come here when we have run out for keyspace and are forced to search collision space.
     * Locate the key on the index of passed page and search its children for the target.
     * @param key Key to search for
     * @param rootNode The root page containing child
     * @param i index on root page to start search
     * @return KeySearchResult of search indicating page, position of key, and success or failure (true or false in result).
     * @throws IOException 
     */
    private KeySearchResult locateChild(Comparable key, KeyPageInterface rootNode, int i) throws IOException {
		KeyPageInterface kpi = (KeyPageInterface) rootNode.getPage(i);
	 	if(kpi == null)
    		return new KeySearchResult(kpi, i, false);
	 	for(int j = 0; i < kpi.getNumKeys(); i++) {
	 		if(kpi.getKey(j).compareTo(key) == 0)
	 			return new KeySearchResult(kpi, j, true);
	 	}
	 	return new KeySearchResult(kpi, kpi.getNumKeys(), false);
	}

	/**
     * Move the data from source and source index to target and targetIndex for the two pages.
     * Optionally null the source at sourceIndex.
     * @param source
     * @param sourceIndex
     * @param target
     * @param targetIndex
     */
    public static void moveKeyData(KeyPageInterface source, int sourceIndex, KeyPageInterface target, int targetIndex, boolean nullify) {
        try {
            ((HMapKeyPage) target).copyKeyAndDataToArray((HMapKeyPage) source, sourceIndex, targetIndex);
			//target.setKey(targetIndex, source.getKey(sourceIndex));
		    //target.setKeyIdArray(targetIndex, source.getKeyId(sourceIndex));
		    //target.setKeyUpdatedArray(targetIndex, true); // move = updated
		    //target.dataArray[targetIndex] = source.dataArray[sourceIndex];
		    //target.setDataIdArray(targetIndex, source.getDataId(sourceIndex));
		    //target.dataUpdatedArray[targetIndex] = source.dataUpdatedArray[sourceIndex];
		    //target.setUpdated(true); // tell it the key in general is updated
		} catch (IOException e) { e.printStackTrace();}

        // We have taken care of target, now if we need to null the source fields, do so.
        if( nullify ) {
            	//source.setKey(sourceIndex, null);
            	//source.setKeyIdArray(sourceIndex, Optr.emptyPointer);
            	//source.setKeyUpdatedArray(sourceIndex, false);
            	//source.dataArray[sourceIndex] = null;
            	//source.setDataIdArray(sourceIndex, Optr.emptyPointer);
            	//source.dataUpdatedArray[sourceIndex] = false; // have not updated off-page data, just moved its pointer
        	((HMapKeyPage) source).nullKeyAndData(sourceIndex);
        }
    }
 
	
	/**
	* Remove key/data object.
	* Deletion from a B-tree is more complicated than insertion, because we can delete a key from any node, not 
	* just a leaf, and when we delete a key from an internal node, we will have to rearrange the nodes children.
	* As in insertion, we must make sure the deletion doesnt violate the B-tree properties. 
	* Just as we had to ensure that a node didnt get too big due to insertion, we must ensure that a node 
	* doesnt get too small during deletion (except that the root is allowed to have fewer than the minimum number t-1 of keys). 
	* Just as a simple insertion algorithm might have to back up if a node on the path to where the key was to be inserted was full, 
	* a simple approach to deletion might have to back up if a node (other than the root) along the path to where the key is to be 
	* deleted has the minimum number of keys.
	* The deletion procedure deletes the key k from the subtree rooted at x. 
	* This procedure guarantees that whenever it calls itself recursively on a node x, the number of keys in x is at least the minimum degree T.
	* Note that this condition requires one more key than the minimum required by the usual B-tree conditions, 
	* so that sometimes a key may have to be moved into a child node before recursion descends to that child. 
	* This strengthened condition allows us to delete a key from the tree in one downward pass without having to back up
	* (with one exception, to be explained). You should interpret the following specification for deletion from a B-tree 
	* with the understanding that if the root node x ever becomes an internal node having no keys 
	* (this situation can occur when we delete x, and x only child x.c1 becomes the new root of the tree), 
	* we decrease the height of the tree by one and preserve the property that the root of the tree contains at least one key. 
	* (unless the tree is empty).
	* Various cases of deleting keys from a B-tree:
	* 1. If the key k is in node x and x is a leaf, delete the key k from x.
	* 2. If the key k is in node x and x is an internal node, do the following:
    * a) If the child y that precedes k in node x has at least t keys, then find the predecessor k0 of k in the sub-tree rooted at y. 
    * Recursively delete k0, and replace k by k0 in x. (We can find k0 and delete it in a single downward pass.)
	* b) If y has fewer than t keys, then, symmetrically, examine the child z that follows k in node x. If z has at least t keys, 
	* then find the successor k0 of k in the subtree rooted at z. Recursively delete k0, and replace k by k0 in x. 
	* (We can find k0 and delete it in a single downward pass.)
    * c) Otherwise, if both y and z have only t-1 keys, merge k and all of z into y, so that x loses both k and the pointer to z, and y 
    * now contains 2t-1 keys. Then free z and recursively delete k from y.
	* 3. If the key k is not present in internal node x, determine the root x.c(i) of the appropriate subtree that must contain k, 
	* if k is in the tree at all. If x.c(i) has only t-1 keys, execute step 3a or 3b as necessary to guarantee that we descend to a 
	* node containing at least t keys. Then finish by recursing on the appropriate child of x.
	* a) If x.c(i) has only t-1 keys but has an immediate sibling with at least t keys, give x.c(i) an extra key by moving a key 
	* from x down into x.c(i), moving a key from x.c(i) immediate left or right sibling up into x, and moving the appropriate 
	* child pointer from the sibling into x.c(i).
    * b) If x.c(i) and both of x.c(i) immediate siblings have t-1 keys, merge x.c(i) with one sibling, which involves moving a key 
    * from x down into the new merged node to become the median key for that node.
	* Since most of the keys in a B-tree are in the leaves, deletion operations are most often used to delete keys from leaves. 
	* The recursive delete procedure then acts in one downward pass through the tree, without having to back up. 
	* When deleting a key in an internal node, however, the procedure makes a downward pass through the tree but may have to 
	* return to the node from which the key was deleted to replace the key with its predecessor or successor.
	* The KeyPageInterface contains most of the functionality and the following methods are unique to the deletion process:
	* 1) remove
    * 2) removeFromNonLeaf
    * =
		if( DEBUG || DEBUGDELETE ) System.out.println("BTreeMain.delete Just deleted "+newKey);
		return 0;
	}

	/**
	 * Rewind current position to beginning of tree. Sets up stack with pages and indexes
	 * such that traversal can take place. Remember to clear stack after these operations.
	 * @exception IOException If read fails
	 */
	public synchronized KeyValue rewind() throws IOException {
		return null;
	}

	/**
	 * Set current position to end of tree.Sets up stack with pages and indexes
	 * such that traversal can take place. Remember to clear stack after these operations. 
	 * @return 
	 * @exception IOException If read fails
	 */
	public synchronized KeyValue toEnd() throws IOException {
		rewind();
		return null;
	}

	/**
	* Seek to location of next key. Set current key and current object.
	* Attempt to advance the child index at the current node. If it advances beyond numKeys, a pop
	* is necessary to get us to the previous level. We repeat the process a that level, advancing index
	* and checking, and again repeating pop.
	* If we get to a currentIndex advanced to the proper index, we know we are not at a leaf since we
	* popped the node as a parent, so we know there is a subtree somewhere, so descend the subtree 
	* of the first key to the left that has a child until we reach a terminal 'leaf' node.
	* We are finished when we are at the root and can no longer traverse right. this is because we popped all the way up,
	* and there are no more to traverse.
	* Note that we dont deal with keys at all here, just child pointers.
	* @return 0 if ok, != 0 if error
	* @exception IOException If read fails
	*/
	public synchronized TraversalStackElement gotoNextKey(TraversalStackElement tse) throws IOException {
		if( DEBUG || DEBUGSEARCH ) {
			System.out.printf("%s.gotoNextKey index:%s%n",this.getClass().getName(),tse);
		}
		// If we are at a key, then advance the index

		// Pointer is null, we can return this index as we cant descend subtree
		if( DEBUG || DEBUGSEARCH) {
			System.out.printf("%s.gotoNextKey child index is null:%n",this.getClass().getName());
		}
		return null;
	}

	@Override
	/**
	* Go to location of previous key in tree
	* @return 0 if ok, <>0 if error
	* @exception IOException If read fails
	*/
	public synchronized TraversalStackElement gotoPrevKey(TraversalStackElement tse) throws IOException {
		if( DEBUG || DEBUGSEARCH ) {
			System.out.printf("%s.gotoPrevKey index:%s%n",this.getClass().getName(),tse);
		}
		return null;
	}
	/**
	 * Pop the stack until we reach a valid spot in traversal.
	 * The currentPage, currentChild are used, setCurrent() is called on exit;
	 * @param next Pop 'previous', or 'next' key. true for 'next'
	 * @return EOF If we reach root and cannot traverse right
	 * @throws IOException
	 */
	private synchronized int popUntilValid(boolean next) throws IOException {
		// If we are here we are at the end of the key range
		// cant move left, are we at root? If so, we must end
	
		// we have reached the end of keys
		// we must pop
		// pop to parent and fall through when we hit root or are not out of keys
		// go up 1 and right to the next key, if no right continue to pop until we have a right key
		// if we hit the root, protocol right. in any case we have to reassert
		// if there is a subtree follow it, else return the key
		while( pop() ) {
			if(DEBUG || DEBUGSEARCH) {
				System.out.println("popUntilValid POP index:");
			}
			// we know its not a leaf, we popped to it
			// If we pop, and we are at the end of key range, and our key is not valid, pop
			if( next ) {
		
			} else {

			}
			return 0;
		}
		// should be at position where we return key from which we previously descended
		// pop sets current indexes
		//setCurrent();
		//return 0;
		//
		// popped to the top and have to stop
		return EOF;
	}
	
	
	/**
	 * Utilize reposition to locate key. Set currentPage, currentIndex, currentKey, and currentChild.
	 * deallocOutstanding is called before exit.
	 * @param targetKey The key to position to in BTree
	 * @return TreeSearchResult containing page, insertion index, atKey = true for key found
	 * @throws IOException
	 */
	public synchronized KeySearchResult search(Comparable targetKey) throws IOException {
		KeySearchResult tsr = null;
		clearStack();
		tsr = locate(targetKey);
    	if( DEBUG || DEBUGSEARCH) {
    		System.out.printf("%s.search returning with currentPage:%s%n",this.getClass().getName(),tsr);
    	}
        return tsr;
}

	/**
	* Seeks to leftmost key in current subtree. Takes the currentChild and currentIndex from currentPage and uses the
	* child at currentChild to descend the subtree and gravitate left.
	*/
	private synchronized boolean seekLastKey() throws IOException {
		boolean foundPage = false;
		if( DEBUG || DEBUGSEARCH ) {
    		System.out.printf("%s.seekLastKey returning%n",this.getClass().getName());
		}
		return foundPage;
	}

	/** 
	 * Internal routine to push stack. Pushes a TraversalStackElement
	 * set keyPageStack[stackDepth] to currentPage
	 * set indexStack[stackDepth] to currentIndex
	 * Sets stackDepth up by 1
	 * @param  
	 * @return true if stackDepth not at MAXSTACK, false otherwise
	 */
	private synchronized void push() {
		//if (stackDepth == MAXSTACK)
		//	throw new RuntimeException("Maximum retrieval stack depth exceeded at "+stackDepth);
		//keyPageStack[stackDepth] = currentPage;
		//childStack[stackDepth] = currentChild;
		//indexStack[stackDepth++] = currentIndex;
		//stack.push(new TraversalStackElement(currentPage, currentIndex, currentChild));
		if( DEBUG ) {
			printStack();
		}
	}

	/** 
	 * Internal routine to pop stack. 
	 * sets stackDepth - 1, 
	 * currentPage to keyPageStack[stackDepth] and 
	 * currentIndex to indexStack[stackDepth]
	 * @return false if stackDepth reaches 0, true otherwise
	 * 
	 */
	private synchronized boolean pop() {
		//if (stackDepth == 0)
		//if( stack.isEmpty() )
		//	return (false);
		//stackDepth--;
		//currentPage = keyPageStack[stackDepth];
		//currentIndex = indexStack[stackDepth];
		//currentChild = childStack[stackDepth];
		//TraversalStackElement tse = stack.pop();
		//currentPage = (HMapKeyPage) tse.keyPage;
		//currentIndex = tse.index;
		//currentChild = tse.child;
		if( DEBUG ) {
			System.out.printf("%s.Pop:%n",this.getClass().getName());
			printStack();
		}
		return (true);
	}

	private synchronized void printStack() {
		//System.out.println("Stack Depth:"+stack.size());
		//for(int i = 0; i < stack.size(); i++) {
		//	TraversalStackElement tse = stack.get(i);
		//	System.out.println("index:"+i+" "+GlobalDBIO.valueOf(tse.keyPage.pageId)+" "+tse.index+" "+tse.child);
		//}
	}
	/**
	* Internal routine to clear references on stack. Just does stack.clear
	*/
	public synchronized void clearStack() {
		//for (int i = 0; i < MAXSTACK; i++)
		//	keyPageStack[i] = null;
		//stackDepth = 0;
		//stack.clear();
	}

	
	public synchronized GlobalDBIO getIO() {
		return sdbio;
	}

	public synchronized void setIO(GlobalDBIO sdbio) {
		this.sdbio = sdbio;
	}

	public synchronized RootKeyPageInterface[] getRoot() {
		return root;
	}
	
    // Inorder walk over the tree.
    synchronized void printHMap(KeyPageInterface node) throws IOException {
            if (node != null) {
                    	System.out.print("Leaf node:");
                            for (int i = 0; i < node.getNumKeys(); i++) {
                                    System.out.print("INDEX:"+i+" node:"+node.getKey(i) + ", ");
                            }
                            System.out.println("\n");                     
            }
    }
       
    
   synchronized ArrayList<Comparable> getKeys(HMapKeyPage node) throws IOException {
            ArrayList<Comparable> array = new ArrayList<Comparable>();
            if (node != null) {
            	for(int i = 0; i < node.getNumKeys(); i++) {
                            array.add(node.getKey(i));
            	}
            }
            return array;
    }

   @Override
   public void traverseStructure(StructureCallBackListener listener, KeyPageInterface node, long parent, int level)
		throws IOException {
	// TODO Auto-generated method stub
	
   }


   @Override
   public int delete(Comparable newKey) throws IOException {
	   // TODO Auto-generated method stub
	   return 0;
   }

   @Override
   public Comparable getKey(Optr keyLoc) throws IOException {
	   return (Comparable) sdbio.deserializeObject(keyLoc);
   }

   @Override
   public Object getValue(Optr valueLoc) throws IOException {
	   return sdbio.deserializeObject(valueLoc);
   }
 
   public static void main(String[] args) {
	   for(int i = 0; i < 10000; i++) {
		String key = String.valueOf(System.currentTimeMillis());//UUID.randomUUID().toString();
		//int[] hashkeys = HashKey.computeKey(String.valueOf(i).hashCode());//.hashCode(i));
		int[] hashkeys = HashKey.computeKey(key.hashCode());//.hashCode(i));
		System.out.printf("%d=HashKeys: %s for key:%s%n",i, Arrays.toString(hashkeys), key);
		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	   }
   }

}

