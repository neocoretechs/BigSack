package com.neocoretechs.bigsack.btree;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Stack;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.io.Optr;
import com.neocoretechs.bigsack.io.ThreadPoolManager;
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
* Main bTree class.  Manipulates retrieval stack of bTreeKeyPages and provides access
* to seek/add/delete functions.
* Important to note that the data is stored as arrays serialized out in key pages. Related to that
* is the concept of element 0 of those arrays being 'this', hence the special treatment in CRUD.
* Unlike a binary search tree, each node of a B-tree may have a variable number of keys and children.
* The keys are stored in ascending order. Each node either is a leaf node or
* it has some associated children that are the root nodes of subtrees.
* The left child node of a node's element contains all nodes (elements) with keys less than or equal to the node element's key
* but greater than the preceding node element's key.
* If a node becomes full, a split operation is performed during the insert operation.
 * The split operation transforms a full node with 2*T-1 elements into two nodes with T-1 elements each
 * and moves the median key of the two nodes into its parent node.
 * The elements left of the median (middle) element of the splitted node remain in the original node.
 * The new node becomes the child node immediately to the right of the median element that was moved to the parent node.
 * 
 * Example (T = 4):
 * 1.  R = | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
 * 
 * 2.  Add key 8
 *   
 * 3.  R =         | 4 |
 *                 /   \
 *     | 1 | 2 | 3 |   | 5 | 6 | 7 | 8 |
 *
* @author Groff Copyright (C) NeoCoreTechs 2015,2017
*/
public final class BTreeMain implements KeyValueMainInterface {
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

	private KeyPageInterface root;
	BTreeNavigator bTreeNavigator;
	long numKeys = 0;
	
	private Stack<TraversalStackElement> stack = new Stack<TraversalStackElement>();
	
	private CyclicBarrier nodeSplitSynch = new CyclicBarrier(3);
	private NodeSplitThread leftNodeSplitThread, rightNodeSplitThread;
	GlobalDBIO sdbio;
	static int T = (BTreeKeyPage.MAXKEYS/2)+1;

	public BTreeMain(GlobalDBIO globalDBIO) throws IOException {
		this.bTreeNavigator = new BTreeNavigator<Comparable, Object>(this);
		if(DEBUG)
			System.out.printf("%s ctor %s%n",this.getClass().getName(), this.bTreeNavigator);
		this.sdbio = globalDBIO;
		// Append the worker name to thread pool identifiers, if there, dont overwrite existing thread group
		ThreadPoolManager.init(new String[]{"NODESPLITWORKER"}, false);
		leftNodeSplitThread = new NodeSplitThread(nodeSplitSynch);
		rightNodeSplitThread = new NodeSplitThread(nodeSplitSynch);
		ThreadPoolManager.getInstance().spin(leftNodeSplitThread,"NODESPLITWORKER");
		ThreadPoolManager.getInstance().spin(rightNodeSplitThread,"NODESPLITWORKER");
		// Consistency check test, also needed to get number of keys
		// Performs full tree/table scan, tallys record count
		if( ALERT )
			System.out.println("Database "+globalDBIO.getDBName()+" ready with "+BTreeKeyPage.MAXKEYS+" keys per page.");
	}
	/**
	 * Gets a page from the pool via {@link BlockAccessIndex.getPageFromPool}. Sets currentPage and this.root
	 * to that value, then calls {@link HMap.getRootNode}.
	 * This method attempts to link the {@link BlockAccessIndex} to the {@link KeyPageInterface} to 'this'
	 * then link the {@link BTNode} generated by {@link BTreeNavigator}
	 * @return The BTnode generated by BTree
	 * @throws IOException
	 */
	@Override
	public RootKeyPageInterface createRootNode() throws IOException {
		this.root = sdbio.getBTreeRootPageFromPool();
		((BTNode)((BTreeKeyPage)(this.root)).bTNode).setmIsLeaf(true);
		if( DEBUG )
			System.out.printf("%s Root KeyPageInterface: %s%n",this.getClass().getName(),root);	
		//return (BTNode<Comparable, Object>) bTree.getRootNode();
		return this.root;
	}
	
	public void test() throws IOException {
		if( TEST ) {
			System.out.printf("MAXKEYS=%d T=%d%n", BTreeKeyPage.MAXKEYS,T);
			// Attempt to retrieve last good key count
			long numKeys = 0;
			long tim = System.currentTimeMillis();
			numKeys = count();
			System.out.println("Consistency check for "+sdbio.getDBName()+" returned "+numKeys+" keys in "+(System.currentTimeMillis()-tim)+" ms.");
		}
	}
	/**
	 * @return 
	 * @throws IOException
	 */
	@Override
	public RootKeyPageInterface createRootNode(NodeInterface btNode) throws IOException {
		this.root.setNode(btNode);
		return this.root;
	}
	/**
	 * When a node is created from {@link BTreeNavigator} a callback to this method
	 * will establish a new page from the pool, set it as current page and
	 * by calling {@link BlockAccessIndex} overloaded static method getPageFromPool with
	 * the new node.
	 * @param btnode The new node called from BTree
	 * @return 
	 * @throws IOException
	 */
	@Override
	public KeyPageInterface createNode(NodeInterface btnode) throws IOException {
		return sdbio.getBTreePageFromPool(btnode);
	}

	/**
	 * Returns number of table scanned keys, sets numKeys field
	 * TODO: Alternate more efficient implementation that counts keys on pages
	 * This method scans all keys, thus verifying the structure.
	 * @throws IOException
	 */
	@Override
	public synchronized long count() throws IOException {
		//System.out.println(found);
		long tim = System.currentTimeMillis();
		KVIteratorIF iterImpl = new KVIteratorIF() {
			@Override
			public boolean item(Comparable key, Object value) {
				++numKeys;
				return true;
			}
		};
		bTreeNavigator.list(iterImpl);
		if( DEBUG || DEBUGCOUNT )
			System.out.println("Count for "+sdbio.getDBName()+" returned "+numKeys+" keys in "+(System.currentTimeMillis()-tim)+" ms.");
		// deallocate outstanding blocks in all tablespaces
		sdbio.deallocOutstanding();
		return numKeys;
	}
	/**
	 * Determines if tree is empty by examining the root for the presence of any keys
	 * @return
	 */
	@Override
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
	@Override
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
		bTreeNavigator.search((Comparable) targetObject);
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
	@Override
	@SuppressWarnings("rawtypes")
	public synchronized KeySearchResult seekKey(Comparable targetKey) throws IOException {
		KeySearchResult tsr = search(targetKey);
		if( DEBUG || DEBUGSEARCH)
			System.out.println("SeekKey state is targKey:"+targetKey+" "+tsr);
		return tsr;
	}
	/**
	 * Called back from delete in BTNode to remove persistent data prior to in-memory update where the
	 * references would be lost.
	 * @param optr The pointer with virtual block and offset
	 * @param o The object that was previously present at that location
	 * @throws IOException
	 */
	@Override
	public synchronized void delete(Optr optr, Object o) throws IOException {
		GlobalDBIO.deleteFromOptr(sdbio, optr, o);
	}
	
	/**
	 * Add to deep store, Set operation.
	 * @param key
	 * @return
	 * @throws IOException
	 */
	@Override
	public synchronized int add(Comparable key) throws IOException {
		return add(key, null);
	}
	/**
	 * Add an object and/or key to the deep store. Traverse the BTree for the insertion point and insert. Map operation.
	 * @param key
	 * @param object
	 * @return 0 for key absent, 1 for key exists
	 * @throws IOException
	 */
	@Override
	@SuppressWarnings("unchecked")
	public synchronized int add(Comparable key, Object value) throws IOException {
		if(DEBUG)
			System.out.printf("%s insert key=%s value=%s%n", this.getClass().getName(), key, value);
		int result = bTreeNavigator.insert(key, value);
		if(result == 1) { // it existed, we have to update previous value, which was overwritten
		}
		if(DEBUG)
			System.out.printf("%s insert exit key=%s value=%s result=%d%n", this.getClass().getName(), key, value,result);
		return result;
	}

	
	/**
	 * Traverse the tree and insert object for key if we find the key.
	 * At each page descent, check the index and compare, if equal put data to array at the spot
	 * and return true. If we are a leaf node and find no match return false;
	 * If we are not leaf node and find no match, get child at key less than this. Find to where the
	 * KEY is LESS THAN the contents of the key array.
	 * @param key The key to search for
	 * @param object the object data to update, if null, ignore data put and return true. If OVERWRITE flag false, also ignore
	 * @return The index of the insertion point if < 0 else if >= 0 the found index point
	 * @throws IOException
	 */
    private synchronized KeySearchResult update(KeyPageInterface sourcePage, Comparable key, Object object) throws IOException {
    	int i = 0;
        while (sourcePage != null) {
                i = 0;
                while (i < sourcePage.getNumKeys() && key.compareTo(sourcePage.getKey(i)) > 0) {
                        i++;
                }
                if (i < sourcePage.getNumKeys() && key.compareTo(sourcePage.getKey(i)) == 0) {
                	// If its a set instead of map the the value data comes back null, else we
                	// deserialize, check to make sure we dont needlessly delete a value to replace it with its equal.
                	Object keyValue = sourcePage.getData(i);
                	if( keyValue != null ) {
                		if(object != null ) {
                			if( !object.equals(keyValue) ) {	
                				// dataArray at index not null and dataIdArray Optr at index not empty for delete to fire.
                				// So if you are using Sets vs Maps it should not happen.
                				if( OVERWRITE ) {
                     				if( DEBUG || DEBUGOVERWRITE )
                     					System.out.println("Preparing to OVERWRITE value "+object+" for key "+key+" index["+i+"]");
                    				((BTreeNavigator) sourcePage).delete(i);
                					((BTreeKeyPage) sourcePage).putDataToArray(object,i);
                					if( DEBUG || DEBUGOVERWRITE )
                     					System.out.println("OVERWRITE value "+object+" for key "+key+" index["+i+"] page:"+sourcePage);
                				} else {
                					if(ALERT)
                						System.out.println("OVERWRITE flag set to false, so attempt to update existing value is ignored for key "+key);
                				}
                			}
                			// wont put the data if here, object = keyValue, both not null
                		} else {
                			((BTreeKeyPage) sourcePage).putDataToArray(object,i);
                		}
                		// If we had a value already, and it wasnt null and not equal to previous we put the new data
                		// If it was null or the value equal to previous we bypassed and are here
                	} else {
                		((BTreeKeyPage) sourcePage).putDataToArray(object,i);
                	}
                	return new KeySearchResult(sourcePage, i, true);
                }
                if (((BTreeKeyPage) sourcePage).getmIsLeafNode()) {
                	if( DEBUG )
                		System.out.println("BTreeMain.update set to return index :"+i+" for "+sourcePage);
                        return new KeySearchResult((KeyPageInterface) sourcePage, i, false);
                } else {
                	KeyPageInterface targetPage  = (KeyPageInterface) sourcePage.getPage(i);// get the page at the index of the given page
                	if( targetPage == null )
                		break;
                	sourcePage = targetPage;
                }
        }
     	if( DEBUG )
    		System.out.println("BTreeMain.update set to return index :"+i+" on fallthrough for "+sourcePage);
        return new KeySearchResult((KeyPageInterface) sourcePage, i, false);
    }
    
    /**
     * Sets up the return KeyPageInterface similar to 'reposition' but this public method initializes root node etc.
     * The purpose is to provide a detached locate method to do intermediate key checks before insert, then use
     * 'add' with the KeyPageInterface in the TreeSearchResult returned from this method.
     * If the TreeSearchResult.insertPoint is > 0 then insertPoint - 1 points to the key that immediately
     * precedes the target key.
     * @param node
     * @param key
     * @return
     * @throws IOException
     */
    @Override
	public synchronized KeySearchResult locate(Comparable key) throws IOException {
        KeyPageInterface rootNode = getRoot()[0];
        return reposition(rootNode, key);
    }

	/**
	 * Same as update without the actual updating.
	 * Traverse the tree for the given key.
	 * At each page descent, check the index and compare, if equal put data to array at the spot
	 * and return true. If we are a leaf node and find no match return false;
	 * If we are not leaf node and find no match, get child at key less than this. Find to where the
	 * KEY is LESS THAN the contents of the key array.
	 * @param key The key to search for
	 * @param object the object data to update, if null, ignore data put and return true. If OVERWRITE flag false, also ignore
	 * @return The index of the insertion point if < 0 else if >= 0 the found index point
	 * @throws IOException
	 */
    synchronized KeySearchResult reposition(KeyPageInterface node, Comparable key) throws IOException {
    	int i = 0;
    	KeyPageInterface sourcePage = node;
        while (sourcePage != null) {
                i = 0;
                while (i < sourcePage.getNumKeys() && key.compareTo(sourcePage.getKey(i)) > 0) {
                        i++;
                }
                if (i < sourcePage.getNumKeys() && key.compareTo(sourcePage.getKey(i)) == 0) {
                 	if( DEBUG )
                		System.out.println("BTreeMain.reposition set to return index :"+i+" after locating key for "+sourcePage);
                	return new KeySearchResult((KeyPageInterface) sourcePage, i, true);
                }
                // Its a leaf node and we fell through, return the index but not 'atKey'
                if (((BTreeKeyPage) sourcePage).getmIsLeafNode()) {
                	if( DEBUG )
                		System.out.println("BTreeMain.reposition set to return index :"+i+" for leaf "+sourcePage);
                	return new KeySearchResult((KeyPageInterface) sourcePage, i, false);
                } else {
                	KeyPageInterface targetPage  = (KeyPageInterface) sourcePage.getPage(i);// get the page at the index of the given page
                	if( targetPage == null )
                		break;
                	sourcePage = targetPage;
                }
        }
      	if( DEBUG )
    		System.out.println("BTreeMain.reposition set to return index :"+i+" for fallthrough "+sourcePage);
        return new KeySearchResult((KeyPageInterface) sourcePage, i, false);
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
            ((BTreeKeyPage) target).copyKeyAndDataToArray((BTreeKeyPage) source, sourceIndex, targetIndex);
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
        	((BTreeKeyPage) source).nullKeyAndData(sourceIndex);
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
    * 3) getPred
    * 4) getSucc
    * 5) borrowFromPrev
    * 6) borrowFromNext
    * 7) merge
	* @param newKey The key to delete
	* @return 0 if ok, <> 0 if error
	* @exception IOException if seek or write failure
	*/
	@Override
	@SuppressWarnings("rawtypes")
	public synchronized int delete(Comparable newKey) throws IOException {
		if( DEBUG || DEBUGDELETE ) System.out.println("--ENTERING DELETE FOR "+newKey);
		bTreeNavigator.delete(newKey);
		if( DEBUG || DEBUGDELETE ) System.out.println("BTreeMain.delete Just deleted "+newKey);
		return 0;
	}


	/**
	 * Rewind current position to beginning of tree. Sets up stack with pages and indexes
	 * such that traversal can take place. Remember to clear stack after these operations.
	 * @exception IOException If read fails
	 */
	@Override
	public synchronized KeyValue rewind() throws IOException {
		clearStack();
		TraversalStackElement tse = seekLeftTree(new TraversalStackElement(root, 0, 0));
		if(tse == null)
			return null;
		return ((BTreeKeyPage)tse.keyPage).getKeyValueArray(0);
		//if( DEBUG )
		//	System.out.println("BTreeMain.rewind positioned at "+currentPage+" "+currentIndex+" "+currentChild);
	}

	/**
	 * Set current position to end of tree.Sets up stack with pages and indexes
	 * such that traversal can take place. Remember to clear stack after these operations. 
	 * @return 
	 * @exception IOException If read fails
	 */
	@Override
	public synchronized KeyValue toEnd() throws IOException {
		rewind();
		TraversalStackElement tse = seekRightTree(root, root.getNumKeys());
		KeyValue next = null;
		while (true) {
			TraversalStackElement tsex = gotoNextKey(tse);
			if(tsex == null)
				break;
			tse = tsex;
		}
		return ((KeyPageInterface)tse.keyPage).getKeyValueArray(tse.index);
	}
	/**
	 * Same as reposition but we populate the stack 
	 * Traverse the tree for the given key.
	 * At each page descent, check the index and compare, if equal put data to array at the spot
	 * and return true. If we are a leaf node and find no match return false;
	 * If we are not leaf node and find no match, get child at key less than this. Find to where the
	 * KEY is LESS THAN the contents of the key array.
	 * @param key The key to search for
	 * @param object the object data to update, if null, ignore data put and return true. If OVERWRITE flag false, also ignore
	 * @return The index of the insertion point if < 0 else if >= 0 the found index point
	 * @throws IOException
	 */
    private synchronized KeySearchResult repositionStack(KeyPageInterface node, Comparable key) throws IOException {
    	int i = 0;
    	KeyPageInterface sourcePage = node;
    	if( DEBUG || DEBUGSEARCH) {
    		System.out.println("BTreeMain.repositionStack key:"+key+" node:"+node);
    	}
        while (sourcePage != null) {
                i = 0;
                while (i < sourcePage.getNumKeys() && sourcePage.getKey(i).compareTo(key) < 0) {
                        i++;
                }
                if (i < sourcePage.getNumKeys() && sourcePage.getKey(i).compareTo(key) == 0) {	
                 	if( DEBUG || DEBUGSEARCH )
                		System.out.println("BTreeMain.repositionStack set to return index :"+i+" after locating key for "+sourcePage);
                	return new KeySearchResult((KeyPageInterface) sourcePage, i, true);
                }
                if (((BTreeKeyPage) sourcePage).getmIsLeafNode()) {
                	// we are at leaf, we pop or return having not found
                	if( DEBUG || DEBUGSEARCH)
                		System.out.println("BTreeMain.repositionStack set to return index :"+i+" for leaf "+sourcePage);
                	// If our key has run off the end of page or will do so, pop to subtree right in parent, we are at leaf still
                	if( i >= sourcePage.getNumKeys() || sourcePage.getKey(i).compareTo(key) < 0 ) {
                		TraversalStackElement tse = popUntilValid(true);
                		return new KeySearchResult((KeyPageInterface) tse.keyPage, tse.index, (tse.child == 0));
                	}
                	// didnt run off end or key on page all > key and we are at leaf, key must not exist
                	//return new TreeSearchResult(sourcePage, i, true);
             		return new KeySearchResult((KeyPageInterface) sourcePage, i, false);
                } else {
                	// non leaf
                	KeyPageInterface targetPage  = (KeyPageInterface) sourcePage.getPage(i);// get the page at the index of the given page
                	if( DEBUG || DEBUGSEARCH) {
                		System.out.println("BTreeMain.repositionStack traverse next page for key:"+key+" page:"+targetPage);
                	}
                	if( targetPage == null )
                		break;
                	TraversalStackElement tse = new TraversalStackElement((BTreeKeyPage) sourcePage, i, i);
                	stack.push(tse);
                	sourcePage = targetPage;
                }
        }
      	if( DEBUG || DEBUGSEARCH)
    		System.out.println("BTreeMain.repositionStack set to return index :"+i+" for fallthrough "+sourcePage);
        return new KeySearchResult((KeyPageInterface) sourcePage, i, false);
    }
	/**
	* Seek to location of next key in tree. Set current key and current object.
	* Attempt to advance the child index at the current node. If it advances beyond numKeys, a pop
	* is necessary to get us to the previous level. We repeat the process a that level, advancing index
	* and checking, and again repeating pop.
	* If we get to a currentIndex advanced to the proper index, we know we are not at a leaf since we
	* popped the node as a parent, so we know there is a subtree somewhere, so descend the subtree 
	* of the first key to the left that has a child until we reach a terminal 'leaf' node.
	* We are finished when we are at the root and can no longer traverse right. this is because we popped all the way up,
	* and there are no more subtrees to traverse.
	* Note that we dont deal with keys at all here, just child pointers.
	* @return 0 if ok, != 0 if error
	* @exception IOException If read fails
	*/
	@Override
	public synchronized TraversalStackElement gotoNextKey(TraversalStackElement tse) throws IOException {
		if( DEBUG || DEBUGSEARCH ) {
			System.out.println("BTreeMain.gotoNextKey "/*page:"+currentPage+*/+" index "+tse);
		}
		int currentIndex = tse.index+1;
		// If we are at a key, then advance the index
		if (currentIndex < tse.keyPage.getNumKeys()) {
			tse.index = currentIndex; // use advanced index
			tse.child = currentIndex; // left
			return tse;
		} else {
			// get index to right and seek the left subtree of that
			return seekLeftTree(new TraversalStackElement(tse.keyPage, currentIndex, currentIndex));
		}
	}

	/**
	* Go to location of previous key in tree
	* @return 0 if ok, <>0 if error
	* @exception IOException If read fails
	*/
	@Override
	public synchronized TraversalStackElement gotoPrevKey(TraversalStackElement tse) throws IOException {
		if( DEBUG || DEBUGSEARCH ) {
			System.out.println("BTreeMain.gotoNextKey "/*page:"+currentPage+*/+" index "+tse);
		}
		// If we are at a key, then reduce the index
	
		
		return popUntilValid(false);
	}
	/**
	 * Pop the stack until we reach a valid spot in traversal.
	 * The currentPage, currentChild are used, setCurrent() is called on exit;
	 * @param next Pop 'previous', or 'next' key. true for 'next'
	 * @return EOF If we reach root and cannot traverse right
	 * @throws IOException
	 */
	private synchronized TraversalStackElement popUntilValid(boolean next) throws IOException {
		while( pop() != null ) {
			if(DEBUG || DEBUGSEARCH) {
				System.out.println("BTreeMain.popUntilValid POP index:");
			}
			// we know its not a leaf, we popped to it
			// If we pop, and we are at the end of key range, and our key is not valid, pop

		}
		// should be at position where we return key from which we previously descended
		// pop sets current indexes
		//setCurrent();
		//return 0;
		//
		// popped to the top and have to stop
		return null;
	}

	/**
	 * Utilize reposition to locate key. Set currentPage, currentIndex, currentKey, and currentChild.
	 * deallocOutstanding is called before exit.
	 * @param targetKey The key to position to in BTree
	 * @return TreeSearchResult containing page, insertion index, atKey = true for key found
	 * @throws IOException
	 */
	@Override
	public synchronized KeySearchResult search(Comparable targetKey) throws IOException {
		KeySearchResult tsr = null;
		clearStack();
        tsr = repositionStack(null, targetKey);        
    	if( DEBUG || DEBUGSEARCH) {
    		System.out.println("BTreeMain.search returning with currentPage:");
    	}
        return tsr;
	}

	/**
	* Seeks to leftmost key in current subtree. Takes the currentChild and currentIndex from currentPage and uses the
	* child at currentChild to descend the subtree and gravitate left.
	*/
	private synchronized TraversalStackElement seekLeftTree(TraversalStackElement tse) throws IOException {
		KeyPageInterface tPage = (KeyPageInterface) tse.keyPage.getPage(tse.index);
		TraversalStackElement tsex = null;
		while (tPage != null) {
			tse = new TraversalStackElement(tPage, 0, 0);
			push(tse);
			if( DEBUG || DEBUGSEARCH)
				System.out.println("BTreeMain.seekLeftTree PUSH using "+tse);
			tPage = (KeyPageInterface) tPage.getPage(0);
		}
		return tse;
	}


	/**
	* Seeks to rightmost key in current subtree
	*/
	private synchronized TraversalStackElement seekRightTree(RootKeyPageInterface currentPage, int currentChild) throws IOException {
		KeyPageInterface tPage = (KeyPageInterface) currentPage.getPage(currentChild);
		TraversalStackElement tse = null;
		while (tPage != null) {
			tse = new TraversalStackElement(currentPage, currentPage.getNumKeys()-1, currentPage.getNumKeys());
			push(tse);
			if( DEBUG || DEBUGSEARCH)
				System.out.println("BTreeMain.seekRightTree PUSH using "+tse);
			tPage = (KeyPageInterface) currentPage.getPage(currentPage.getNumKeys());
		}
		return tse;
	}

	/** 
	 * Internal routine to push stack. Pushes a TraversalStackElement
	 * set keyPageStack[stackDepth] to currentPage
	 * set indexStack[stackDepth] to currentIndex
	 * Sets stackDepth up by 1
	 * @param  
	 * @return true if stackDepth not at MAXSTACK, false otherwise
	 */
	private synchronized void push(TraversalStackElement tse) {
		//if (stackDepth == MAXSTACK)
		//	throw new RuntimeException("Maximum retrieval stack depth exceeded at "+stackDepth);
		if( tse == null )
			throw new RuntimeException("BTreeMain.push cant push a null page to stack");
		stack.push(tse);
		if( DEBUG ) {
			System.out.print("BTreeMain.Push:");
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
	private synchronized TraversalStackElement pop() {
		if( stack.isEmpty() )
			return null;
		TraversalStackElement tse = stack.pop();
		if( DEBUG ) {
			System.out.print("BTreeMain.Pop:");
			printStack();
		}
		return tse;
	}

	private synchronized void printStack() {
		System.out.println("Stack Depth:"+stack.size());
		//for(int i = 0; i < stack.size(); i++) {
		//	TraversalStackElement tse = stack.get(i);
		//	System.out.println("index:"+i+" "+GlobalDBIO.valueOf(tse.keyPage.pageId)+" "+tse.index+" "+tse.child);
		//}
	}
	/**
	* Internal routine to clear references on stack. Just does stack.clear
	*/
	@Override
	public synchronized void clearStack() {
		//for (int i = 0; i < MAXSTACK; i++)
		//	keyPageStack[i] = null;
		//stackDepth = 0;
		stack.clear();
	}

	
	@Override
	public synchronized GlobalDBIO getIO() {
		return sdbio;
	}

	@Override
	public synchronized void setIO(GlobalDBIO sdbio) {
		this.sdbio = sdbio;
	}

	@Override
	public synchronized KeyPageInterface[] getRoot() {
		return new KeyPageInterface[] {root};
	}

	
    // Inorder walk over the tree.
    synchronized void printBTree(KeyPageInterface node) throws IOException {
            if (node != null) {
                    if (((BTreeKeyPage) node).getmIsLeafNode()) {
                    	System.out.print("Leaf node:");
                            for (int i = 0; i < node.getNumKeys(); i++) {
                                    System.out.print("INDEX:"+i+" node:"+node.getKey(i) + ", ");
                            }
                            System.out.println("\n");
                    } else {
                    	System.out.print("NonLeaf node:");
                            int i;
                            for (i = 0; i < node.getNumKeys(); i++) {
                            	KeyPageInterface btk = (KeyPageInterface) node.getPage(i);
                                printBTree(btk);
                                System.out.print("INDEX:"+i+" node:"+ node.getKey(i) + ", ");
                            }
                            // get last far right node
                            printBTree((KeyPageInterface) node.getPage(i));
                            System.out.println("\n");
                    }                       
            }
    }
       
    synchronized void validate() throws Exception {
            ArrayList<Comparable> array = getKeys(getRoot()[0]);
            for (int i = 0; i < array.size() - 1; i++) {            
                    if (array.get(i).compareTo(array.get(i + 1)) >= 0) {
                            throw new Exception("B-Tree invalid: " + array.get(i)  + " greater than " + array.get(i + 1));
                    }
        }           
    }
    
    // Inorder walk over the tree.
   synchronized ArrayList<Comparable> getKeys(KeyPageInterface node) throws IOException {
            ArrayList<Comparable> array = new ArrayList<Comparable>();
            if (node != null) {
                    if (((BTreeKeyPage) node).getmIsLeafNode()) {
                            for (int i = 0; i < node.getNumKeys(); i++) {
                                    array.add(node.getKey(i));
                            }
                    } else {
                            int i;
                            for (i = 0; i < node.getNumKeys(); i++) {
                                    array.addAll(getKeys((KeyPageInterface) node.getPage(i)));
                                    array.add(node.getKey(i));
                            }
                            array.addAll(getKeys((KeyPageInterface) node.getPage(i)));
                    }                       
            }
            return array;
    }
   	/**
   	 * Walk the tree calling back the method to deliver structure data to some process
   	 * such as graphical display.
   	 * @throws IOException
   	 */
   	@Override
	public synchronized void traverseStructure(StructureCallBackListener listener, KeyPageInterface node, long parent, int level) throws IOException {
       if (node != null) {
               if (((BTreeKeyPage) node).getmIsLeafNode()) {
            	   listener.call(level, node.getPageId(), parent, node.getKey(0), node.getKey(node.getNumKeys()-1), ((BTreeKeyPage) node).getmIsLeafNode(), node.getNumKeys());          
               } else {
                       int i;
                       for (i = 0; i < node.getNumKeys(); i++) {
                    	   listener.call(level, node.getPageId(), parent, node.getKey(0), node.getKey(node.getNumKeys()-1), ((BTreeKeyPage) node).getmIsLeafNode(), node.getNumKeys());
                    	   traverseStructure(listener, (KeyPageInterface) node.getPage(i), node.getPageId(), level+1);
                       }
                       listener.call(level+1, node.getPageId(), parent, node.getKey(0), node.getKey(node.getNumKeys()-1), ((BTreeKeyPage) node).getmIsLeafNode(), node.getNumKeys());
                       traverseStructure(listener, (KeyPageInterface) node.getPage(i), node.getPageId(), level+1);
               }                       
       }
   }
   	
  /**
   * Key/value instances call here for their deserialized key
   */
   @Override
   public Comparable getKey(Optr keyLoc) throws IOException {
   		return (Comparable) sdbio.deserializeObject(keyLoc);
   }

   /**
    * Key/value instances call here for their deserialized key
    */
   	@Override
   	public Object getValue(Optr valueLoc) throws IOException {
   		return sdbio.deserializeObject(valueLoc);
   	}
   	 


}

