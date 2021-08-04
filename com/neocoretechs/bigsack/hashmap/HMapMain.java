package com.neocoretechs.bigsack.hashmap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.btree.StructureCallBackListener;
import com.neocoretechs.bigsack.io.Optr;
import com.neocoretechs.bigsack.io.ThreadPoolManager;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.stream.PageIteratorIF;
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
	private static boolean DEBUG = false; // General debug, overrides other levels
	private static boolean DEBUGCURRENT = false; // alternate debug level to view current page assignment of KeyPageInterface
	private static boolean DEBUGSEARCH = false; // traversal debug
	private static boolean DEBUGCOUNT = false;
	private static boolean DEBUGDELETE = false;
	private static boolean DEBUGINSERT = false;
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
	private String[] hMapWorkerNames = new String[DBPhysicalConstants.DTABLESPACES];
	private Stack<TraversalStackElement> stack = new Stack<TraversalStackElement>();
	private KeySearchResult lastInsertResult = null;
	private Object result = null; // result of object seek,etc.
	private long count = 0L; // result of count
	private Object mutex = new Object();
	private HMapNavigator iteratorSupport;
	/**
	 * Create the array of {@link HMap} instances for primary root pages
	 * @param globalDBIO
	 * @throws IOException
	 */
	public HMapMain(GlobalDBIO globalDBIO) throws IOException {
		if(DEBUG)
			System.out.printf("%s ctor%n",this.getClass().getName());
		this.sdbio = globalDBIO;
		// Initialize the thread pool group NAMES to spin new threads in controllable batches
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			hMapWorkerNames[i] = String.format("%s%s%d", "HMAPWORKER",globalDBIO.getDBName(),i);
		}
		ThreadPoolManager.init(hMapWorkerNames, false);
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
			HMapRootKeyPage htk = sdbio.getHMapRootPageFromPool(i);
			this.root[i] = htk;
		}
		if( DEBUG ) {
			StringBuilder sb = new StringBuilder();
			for(int i = 0; i < this.root.length; i++)
				sb.append(root[i].toString());
			System.out.printf("%s.createRootNode Root KeyPageInterface: %s%n",this.getClass().getName(),sb.toString());
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
		HMapKeyPage htk = sdbio.getHMapPageFromPool(tblsp);
		htNode.setPageId(htk.getPageId());
		root[tblsp].setRootNode(htk.getBlockAccessIndex());
		if( DEBUG )
			System.out.printf("%s.createRootNode htNode=%s Root KeyPageInterface: %s%n",this.getClass().getName(),htNode,root[tblsp].toString());
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
		return sdbio.getHMapPageFromPool(hnode);
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
	
	public Callable<Object> callCount(PageIteratorIF<KeyPageInterface> iterImpl, int i) { 
		return () -> {
			for(int j = 0; j < root[i].getNumKeys(); j++) {
				if(root[i].getPageId(j) != -1L)
					HMapNavigator.retrievePagesInOrder(this, root[i].getPage(j), iterImpl);
			}
			return true;
		};
	}
	@Override
	/**
	 * Returns number of table scanned keys, sets numKeys field
	 * TODO: Alternate more efficient implementation that counts keys on pages
	 * This method scans all keys, thus verifying the structure.
	 * @throws IOException
	 */
	public synchronized long count() throws IOException {
		long tim = System.currentTimeMillis();
		PageIteratorIF<KeyPageInterface> iterImpl = new PageIteratorIF<KeyPageInterface>() {
			@Override
			public void item(KeyPageInterface page) throws IOException {
				HMapKeyPage nPage = (HMapKeyPage) page;
				synchronized(mutex) {
					count += nPage.getNumKeys();
				}
			}
		};
		Future<?>[] futureArray = new Future<?>[DBPhysicalConstants.DTABLESPACES];
		// queue to each tablespace
		try {
			for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				futureArray[i] = ThreadPoolManager.getInstance().spin(callCount(iterImpl, i),hMapWorkerNames[i]);
			}
			for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				futureArray[i].get();
				if(DEBUG)
					System.out.printf("%s next count for tablespace %d%n", this.getClass().getName(),i);
			}
		} catch (InterruptedException | ExecutionException e) {
			throw new IOException(e);
		}
		//for(int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
		//	for(int j = 0; j < root[i].getNumKeys(); j++) {
		//		if(root[i].getPageId(j) != -1L)
		//			HMapNavigator.retrievePagesInOrder(this, root[i].getPage(j), iterImpl);
		//	}
		//}
		if( DEBUG || DEBUGCOUNT )
			System.out.println("Count for "+sdbio.getDBName()+" returned "+count+" keys in "+(System.currentTimeMillis()-tim)+" ms.");
		return count;
	}
	
	  
	@Override
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
	
	@Override
	/**
	* currentPage and currentIndex set by this seeker of a target object value.
	* The only physically possible way is an iteration through the entire collection until found or end.
	* @param targetObject The Object value to seek.
	* @return data Object if found. null otherwise.
	* @exception IOException if read failure
	*/
	@SuppressWarnings("rawtypes")
	public synchronized Object seekObject(Object targetObject) throws IOException {	
		long tim = System.currentTimeMillis();
		KVIteratorIF<Comparable, Object> iterImpl = new KVIteratorIF<Comparable,Object>() {
			@Override
			public boolean item(Comparable key, Object value) {
				if(value.equals(targetObject)) {
					result = key;
					return true; // 'index' in page is set
				}
				return false;
			}
		};
		PageIteratorIF<KeyPageInterface> iterImplp = new PageIteratorIF<KeyPageInterface>() {
				@Override
				public void item(KeyPageInterface page) throws IOException {
					page.retrieveEntriesInOrder(iterImpl);
				}
		};
		for(int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			HMapNavigator.retrievePagesInOrder(this, root[i].getPage(i), iterImplp);
			if(result != null)
				return result;
		}
		if( DEBUG || DEBUGCOUNT )
			System.out.println("Count for "+sdbio.getDBName()+" returned "+numKeys+" keys in "+(System.currentTimeMillis()-tim)+" ms.");
		return null;
	}
	
	@Override
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
					nPage.nextPage = sdbio.getHMapPageFromPool(nPage.nextPageId);
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
    
    @Override
    /**
     * Main search method.<p/>
  	 * Set up keysearchresult for further operations.<p/>
     * @param key
     * @return KeySearhcResult of locate operation
     * @throws IOException
     */
    public synchronized KeySearchResult locate(Comparable key) throws IOException {
		int[] hashkeys;
		// element 0 is our tablespace, start there, find a collision space page to hold the new key
		HMapNavigator hNav = new HMapNavigator(this, key);
		hashkeys = hNav.getHashKeys();
		lastInsertResult = hNav.search(root[hashkeys[0]]);
		if(DEBUG)
			System.out.printf("%s exit key=%s lastInsertResult=%s%n", this.getClass().getName(), key, lastInsertResult);
		return lastInsertResult;
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
 
    @Override
	/**
	 * Rewind current position to beginning of tree. Sets up stack with pages and indexes
	 * such that traversal can take place. Remember to clear stack after these operations.
	 * @exception IOException If read fails
	 */
	public synchronized KeyValue rewind() throws IOException {
		iteratorSupport = new HMapNavigator(this, null);
		int lastRoot = -1;
		for(int i = 0; i < root.length; i++) {
			if(root[i].getNumKeys() > 0)
				lastRoot = i;
		}
		if(lastRoot == -1)
			return null;
		KeyPageInterface kpi = iteratorSupport.firstPage(root[lastRoot]);
		if(kpi == null || kpi.getNumKeys() == 0)
			return null;
		return kpi.getKeyValueArray(0);
	}

    @Override
	/**
	 * Set current position to end of tree.Sets up stack with pages and indexes
	 * such that traversal can take place. Remember to clear stack after these operations. 
	 * @return 
	 * @exception IOException If read fails
	 */
	public synchronized KeyValue toEnd() throws IOException {
		iteratorSupport = new HMapNavigator(this, null);
		int lastRoot = -1;
		for(int i = 0; i < root.length; i++) {
			if(root[i].getNumKeys() > 0)
				lastRoot = i;
		}
		if(lastRoot == -1)
			return null;
		KeyPageInterface kpi = iteratorSupport.firstPage(root[lastRoot]);
		if(kpi == null || kpi.getNumKeys() == 0)
			return null;
		KeyPageInterface kpx = null;
    	while((kpx = iteratorSupport.nextPage()) != null) {
    		kpi = kpx;
    	}
		while(((HMapKeyPage)kpi).nextPage != null) {
			kpi = ((HMapKeyPage)kpi).nextPage;
		}
		if(kpi.getNumKeys() == 0)
			return null;
		return kpi.getKeyValueArray(kpi.getNumKeys()-1);
	}

    @Override
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
		int currentIndex = tse.index+1;
		// If we are at a key, then advance the index
		if (currentIndex < tse.keyPage.getNumKeys()) {
			tse.index = currentIndex; // use advanced index
			tse.child = currentIndex; // left
			return tse;
		} else {
			// If we are at a key, then advance the index
			KeyPageInterface kpi = (KeyPageInterface) tse.keyPage;
			if(((HMapKeyPage)kpi).nextPage != null) {
				kpi = ((HMapKeyPage)kpi).nextPage;
			} else {
				kpi = iteratorSupport.nextPage();
			}
			if(kpi.getNumKeys() == 0)
				return null;
			// Pointer is null, we can return this index as we cant descend subtree
			if( DEBUG || DEBUGSEARCH) {
				System.out.printf("%s.gotoNextKey index %s%n",this.getClass().getName(), kpi);
			}
			return new TraversalStackElement(kpi, 0, 0);
		}
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
		throw new IOException("not supported");
	}

	@Override
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
	
	@Override
	/**
	* Internal routine to clear references on stack. Just does stack.clear
	*/
	public synchronized void clearStack() {
		//for (int i = 0; i < MAXSTACK; i++)
		//	keyPageStack[i] = null;
		//stackDepth = 0;
		//stack.clear();
	}

	@Override
	/**
	 * {@link KeyValueMainInterface}
	 */
	public GlobalDBIO getIO() {
		return sdbio;
	}
	
	@Override
	public synchronized void setIO(GlobalDBIO sdbio) {
		this.sdbio = sdbio;
	}
	
	@Override
	public synchronized RootKeyPageInterface[] getRoot() {
		return root;
	}
	
    // Walk over the collision page.
    synchronized void printHMap(KeyPageInterface node) throws IOException {
            if (node != null) {
                    System.out.print("Terminal node:");
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
   public void traverseStructure(StructureCallBackListener listener, KeyPageInterface node, long parent, int level) throws IOException {
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

