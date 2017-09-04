package com.neocoretechs.bigsack.btree;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.Stack;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.Props;
import com.neocoretechs.bigsack.io.Optr;
import com.neocoretechs.bigsack.io.ThreadPoolManager;
import com.neocoretechs.bigsack.io.pooled.ObjectDBIO;
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
public final class BTreeMain {
	private static boolean DEBUG = false; // General debug, overrides other levels
	private static boolean DEBUGCURRENT = false; // alternate debug level to view current page assignment of BTreeKeyPage
	private static boolean DEBUGSEARCH = false; // traversal debug
	private static boolean DEBUGCOUNT = false;
	private static boolean TEST = false; // Do a table scan and key count at startup
	private static boolean ALERT = true; // Info level messages
	private static boolean OVERWRITE = false; // flag to determine whether value data is overwritten for a key or its ignored
	static int BOF = 1;
	static int EOF = 2;
	static int NOTFOUND = 3;
	static int ALREADYEXISTS = 4;
	static int STACKERROR = 5;
	static int TREEERROR = 6;
	static int MAXSTACK = 1024;

	private BTreeKeyPage root;
	//private long numKeys;
	
	// the 'current' items reflect the current status of the tree
	BTreeKeyPage currentPage;
	int currentIndex;
	int currentChild;
	@SuppressWarnings("rawtypes")
	private Comparable currentKey;
	private Object currentObject;
	
	private Stack<TraversalStackElement> stack = new Stack<TraversalStackElement>();
	
	private CyclicBarrier nodeSplitSynch = new CyclicBarrier(3);
	private NodeSplitThread leftNodeSplitThread, rightNodeSplitThread;
	private ObjectDBIO sdbio;
	static int T = (BTreeKeyPage.MAXKEYS/2)+1;

	public BTreeMain(ObjectDBIO sdbio) throws IOException {
		this.sdbio = sdbio;
		currentPage = setRoot(BTreeKeyPage.getPageFromPool(sdbio, 0L));
		if( DEBUG ) System.out.println("Root BTreeKeyPage: "+currentPage);
		currentIndex = 0;
		currentChild = 0;
		// Append the worker name to thread pool identifiers, if there, dont overwrite existing thread group
		ThreadPoolManager.init(new String[]{"NODESPLITWORKER"}, false);
		leftNodeSplitThread = new NodeSplitThread(nodeSplitSynch);
		rightNodeSplitThread = new NodeSplitThread(nodeSplitSynch);
		ThreadPoolManager.getInstance().spin(leftNodeSplitThread,"NODESPLITWORKER");
		ThreadPoolManager.getInstance().spin(rightNodeSplitThread,"NODESPLITWORKER");
		
		// Consistency check test, also needed to get number of keys
		// Performs full tree/table scan, tallys record count
		if( TEST ) {
			// Attempt to retrieve last good key count
			long numKeys = 0;
			long tim = System.currentTimeMillis();
			numKeys = count();
			System.out.println("Consistency check for "+sdbio.getDBName()+" returned "+numKeys+" keys in "+(System.currentTimeMillis()-tim)+" ms.");
		}
		if( ALERT )
			System.out.println("Database "+sdbio.getDBName()+" ready with "+BTreeKeyPage.MAXKEYS+" keys per page.");

	}
	/**
	 * Returns number of table scanned keys, sets numKeys field
	 * @throws IOException
	 */
	public synchronized long count() throws IOException {
		rewind();
		int i;
		long numKeys = 0;
		long tim = System.currentTimeMillis();
		if( currentPage != null ) {
			++numKeys;
			while ((i = gotoNextKey()) == 0) {
			if( DEBUG || DEBUGCOUNT)
				System.out.println("gotoNextKey returned: "+currentPage.getKey(currentIndex));
				++numKeys;
			}
		}
		if( DEBUG || DEBUGCOUNT )
			System.out.println("Count for "+sdbio.getDBName()+" returned "+numKeys+" keys in "+(System.currentTimeMillis()-tim)+" ms.");
		// deallocate outstanding blocks in all tablespaces
		sdbio.deallocOutstanding();
		clearStack();
		return numKeys;
	}
	
	public synchronized boolean isEmpty() {
		return (getRoot().getNumKeys() == 0);
	}
	/**
	* currentPage and currentIndex set.
	* @param targetKey The Comparable key to seek
	* @return data Object if found. Null otherwise.
	* @exception IOException if read failure
	*/
	@SuppressWarnings("rawtypes")
	public synchronized Object seekObject(Comparable targetKey) throws IOException {
		TreeSearchResult tsr = search(targetKey);
		if (tsr.atKey) {
			currentPage = tsr.page;
			currentIndex = tsr.insertPoint;
			currentChild = tsr.insertPoint;
			setCurrent();
			return getCurrentObject();
		} else {
			return null;
		}
	}
	/**
	* Seek the key, if we dont find it leave the tree at it position closest greater than element.
	* If we do find it return true in atKey of result and leave at found key.
	* Calls search, which calls clearStack, repositionStack and setCurrent.
	* @param targetKey The Comparable key to seek
	* @return search result with key data
	* @exception IOException if read failure
	*/
	@SuppressWarnings("rawtypes")
	public synchronized TreeSearchResult seekKey(Comparable targetKey) throws IOException {
		TreeSearchResult tsr = search(targetKey);
		if( DEBUG || DEBUGSEARCH)
			System.out.println("SeekKey  state is currentIndex:"+currentIndex+" targKey:"+targetKey+" "+currentPage);
		return tsr;
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
	 * Add an object and/or key to the deep store. Traverse the BTree for the insertion point and insert. Map operation.
	 * @param key
	 * @param object
	 * @return 0 for key absent, 1 for key exists
	 * @throws IOException
	 */
	public synchronized int add(Comparable key, Object object) throws IOException {
        BTreeKeyPage rootNode = getRoot();
        TreeSearchResult usr = null;
        // On the offhand chance all nodes deleted or no inserts
        if( rootNode.getNumKeys() > 0)
        	usr = update(rootNode, key, object);
        else {
        	usr = new TreeSearchResult(rootNode, 0, false);
        	if(DEBUG)
    			System.out.println("BTreeMain.update page "+rootNode+" has NO keys, returning with insert point 0");
        }
        if (!usr.atKey) {
        		BTreeKeyPage targetNode = usr.page;
                if (rootNode.getNumKeys() == (2 * T - 1)) {
                       splitNodeBalance(rootNode);
                       // re position insertion point after split
                       if( DEBUG )
                    	   System.out.println("BTreeMain.add calling reposition after splitRootNode for key:"+key+" node:"+rootNode);
                       TreeSearchResult repos = reposition(rootNode, key);
                       targetNode = repos.page;
                } 
                insertIntoNode(targetNode, key, object); // Insert the key into the B-Tree with root rootNode.
        }
        return (usr.atKey ? 1 : 0);
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
    private synchronized TreeSearchResult update(BTreeKeyPage node, Comparable key, Object object) throws IOException {
    	int i = 0;
    	BTreeKeyPage sourcePage = node;

        while (sourcePage != null) {
                i = 0;
                while (i < sourcePage.getNumKeys() && key.compareTo(sourcePage.getKey(i)) > 0) {
                        i++;
                }
                if (i < sourcePage.getNumKeys() && key.compareTo(sourcePage.getKey(i)) == 0) {
                	if( object != null && OVERWRITE) {
                		if(DEBUG)
                			System.out.println("BTreeMain.update DELETING and REPLACING object data for index "+i+" in "+sourcePage);
            			sourcePage.deleteData(i);
                        sourcePage.putDataToArray(object,i);
                	}
                	return new TreeSearchResult(sourcePage, i, true);
                }
                if (sourcePage.getmIsLeafNode()) {
                	if( DEBUG )
                		System.out.println("BTreeMain.update set to return index :"+i+" for "+sourcePage);
                        return new TreeSearchResult(sourcePage, i, false);
                } else {
                	BTreeKeyPage targetPage  = sourcePage.getPage(i);// get the page at the index of the given page
                	if( targetPage == null )
                		break;
                	sdbio.deallocOutstanding(sourcePage.pageId);
                	sourcePage = targetPage;
                }
        }
     	if( DEBUG )
    		System.out.println("BTreeMain.update set to return index :"+i+" on fallthrough for "+sourcePage);
        return new TreeSearchResult(sourcePage, i, false);
    }
    
    /**
     * Sets up the return BTreeKeyPage similar to 'reposition' but this public method initializes root node etc.
     * The purpose is to provide a detached locate method to do intermediate key checks before insert, then use
     * 'add' with the BTreeKeyPage in the TreeSearchResult returned from this method.
     * If the TreeSearchResult.insertPoint is > 0 then insertPoint - 1 points to the key that immediately
     * precedes the target key.
     * @param node
     * @param key
     * @return
     * @throws IOException
     */
    public synchronized TreeSearchResult locate(Comparable key) throws IOException {
        BTreeKeyPage rootNode = getRoot();
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
    synchronized TreeSearchResult reposition(BTreeKeyPage node, Comparable key) throws IOException {
    	int i = 0;
    	BTreeKeyPage sourcePage = node;
        while (sourcePage != null) {
                i = 0;
                while (i < sourcePage.getNumKeys() && key.compareTo(sourcePage.getKey(i)) > 0) {
                        i++;
                }
                if (i < sourcePage.getNumKeys() && key.compareTo(sourcePage.getKey(i)) == 0) {
                 	if( DEBUG )
                		System.out.println("BTreeMain.reposition set to return index :"+i+" after locating key for "+sourcePage);
                	return new TreeSearchResult(sourcePage, i, true);
                }
                // Its a leaf node and we fell through, return the index but not 'atKey'
                if (sourcePage.getmIsLeafNode()) {
                	if( DEBUG )
                		System.out.println("BTreeMain.reposition set to return index :"+i+" for leaf "+sourcePage);
                	return new TreeSearchResult(sourcePage, i, false);
                } else {
                	BTreeKeyPage targetPage  = sourcePage.getPage(i);// get the page at the index of the given page
                	if( targetPage == null )
                		break;
                	sdbio.deallocOutstanding(sourcePage.pageId);
                	sourcePage = targetPage;
                }
        }
      	if( DEBUG )
    		System.out.println("BTreeMain.reposition set to return index :"+i+" for fallthrough "+sourcePage);
        return new TreeSearchResult(sourcePage, i, false);
    }

    /**
     * Split a node of the B-Tree into 3 nodes with the 2 children balanced
     * This method will only be called if root node is full, or if a leaf fills on insert; it leaves original root in place
     * The method forms 2 requests that are spun off to 2 node split threads. The logic in this method
     * synchronizes via a cyclicbarrier and monitor to manipulate the parent node properly after
     * the worker threads are done. One of the the threads will have inserted the key to one of the
     * new children upon completion. The request hold the position.
     * @param parentNode The node we are splitting, old root, becomes parent of both nodes at root
     * @throws IOException 
     */
    
    synchronized void splitNodeBalance(BTreeKeyPage parentNode) throws IOException { 
        if( DEBUG )
        	System.out.println("BTreeMain.splitNodeBalance :"+parentNode);
        NodeSplitRequest lnsr = new NodeSplitRequest(sdbio, parentNode, NodeSplitRequest.NODETYPE.NODE_LEFT);
        NodeSplitRequest rnsr = new NodeSplitRequest(sdbio, parentNode, NodeSplitRequest.NODETYPE.NODE_RIGHT);
        try {
        	leftNodeSplitThread.queueRequest(lnsr);
        	rightNodeSplitThread.queueRequest(rnsr);
        	// await the other nodes
			nodeSplitSynch.await();
			// once we synch, we have 2 nodes, one each in the requests
			synchronized(parentNode) {
				int nullKeys = 0;
				int goodKeys = 0;
				// find the blanks in indexes, we only remove beginning blanks where keys were removed
				for(int j = 0; j < BTreeKeyPage.MAXKEYS; j++) {
					if(parentNode.getKey(j) == null ) 
						++nullKeys;
					else
						break;
				}
				if( DEBUG ) {
					System.out.println("BTreeMain.splitNodeBalance nullKeys:"+nullKeys);
				}
				// find the number of good keys from the nulls forward
				for(int j = nullKeys; j < BTreeKeyPage.MAXKEYS; j++) {
					if(parentNode.getKey(j) != null )
						++goodKeys;
					else
						break;
				}
				// set as non leaf so proper insertion and compaction occurs
				parentNode.setmIsLeafNode(false);
				
				if( DEBUG ) {
					System.out.println("BTreeMain.splitNodeBalance goodKeys:"+goodKeys+" parent:"+parentNode);
				}
				if( nullKeys > 0 ) {
					for(int j = nullKeys; j < goodKeys+nullKeys; j++) {
						if( DEBUG ) {
							System.out.println("BTreeMain.splitNode moveKeyData:"+j+" nulls:"+nullKeys);
						}
						moveKeyData(parentNode, j, parentNode, j-nullKeys, true);
						if( DEBUG ) {
							System.out.println("BTreeMain.splitNodeBalance moveChildData:"+j+" nulls:"+nullKeys);
						}
						moveChildData(parentNode, j, parentNode, j-nullKeys, true);
					}
					if( DEBUG ) {
						System.out.println("BTreeMain.splitNodeBalance moveKeyData parent source:"+String.valueOf(nullKeys+goodKeys+1)+" to:"+goodKeys);
					}
					// get the node at position+1, the rightmost key pointer
					moveChildData(parentNode, nullKeys+goodKeys, parentNode, goodKeys, true);
				}
				parentNode.setNumKeys(goodKeys); // sets parent node updated
				if( DEBUG ) {
					System.out.println("BTreeMain.spliNodeBalance moving to putPage:"+currentPage);
				}
				//
				// Write the three new pages back to deep store and log them along the way
				// Deallocate (unlatch) them after the write completes.
				//
				parentNode.putPage();
				((BTreeKeyPage)lnsr.getObjectReturn()).putPage();
				((BTreeKeyPage)rnsr.getObjectReturn()).putPage();
				//sdbio.deallocOutstanding(parentNode.pageId);
				//sdbio.deallocOutstanding(((BTreeKeyPage)lnsr.getObjectReturn()).pageId);
				//sdbio.deallocOutstanding(((BTreeKeyPage)rnsr.getObjectReturn()).pageId);
			}
		} catch (InterruptedException | BrokenBarrierException e) {
			return; // executor shutdown
		}
    }
    
    
    /**
     * Insert into BTree node . The node is checked for leaf status, and if so, the key is inserted in to that key.
     * To ensure that the key is slot is available a split may be performed beforehand.
     * If the node is a non leaf we have to look for the key that is least and in addition has a right pointer since
     * the only way we add nodes is through a split
     * @param node
     * @param key
     * @param object
     * @throws IOException
     */
   synchronized void insertIntoNode(BTreeKeyPage node, Comparable key, Object object) throws IOException {
    	if( DEBUG )
    		System.out.println("BTreeMain.insertIntoNode:"+key+" "+object+" Node:"+node);
            int i = node.getNumKeys() - 1;
            if (node.getmIsLeafNode()) {
            	// If node is full, initiate split, as we do with a full root, we are going to split this
            	// node by pulling the center key, and creating 2 new balanced children
            	if (node.getNumKeys() == BTreeKeyPage.MAXKEYS) {
            		splitNodeBalance(node);
            		insertIntoNode(reposition(node, key).page, key, object);
            		return;
            	}
                // If node is not a full node insert the new element into its proper place within node.
                while (i >= 0 && key.compareTo(node.getKey(i)) < 0) {
                    	moveKeyData(node, i, node, i+1, false);
                        i--;
                }
                i++;
                node.setKey(i, key);
                // leaf, no child data
                if( object != null ) {
                    	node.dataArray[i] = object;
                    	node.setDataIdArray(i, Optr.emptyPointer); // sets dataUpdatedArray true
                }
                node.setNumKeys(node.getNumKeys() + 1); // sets 'updated' for node
                node.putPage();
            } else {
            		// x is not a leaf node. We can't just stick k in because it doesn't have any children; 
            		// children are really only created when we split a node, so we don't get an unbalanced tree. 
            		// We find a child of x where we can (recursively) insert k. We read that child in from disk. 
            		// If that child is full, we split it and figure out which one k belongs in. 
            		// Then we recursively insert k into this child (which we know is non-full, because if it were, 
            		// we would have split it).
            		//
                    // Move back from the last key of node until we find the child pointer to the node
                    // that is the root node of the subtree where the new element should be placed.
            		// look for the one to the right of target where an entry has a child and newKey is greater (or equal)
            		i = node.getNumKeys()-1;
            		int childPos = i + 1;
            		// keep going backward checking while key is < keyArray or no child pointer
            		// if not found, the key was > all pointers
                    while (i >= 0 && (node.getPageId(childPos) == -1 || key.compareTo(node.getKey(i)) < 0)) {
                            i--;
                            childPos--;
                    }
                    // If our index value went to -1, 
                    // most likely a tree fault. We have recursively entered, searching for a leaf node, instead,
                    // we wound up here where we got a far left index indicating our range went out of the min and
                    // max that started the tree traversal.
                    if( i == -1 ) {
                    	//++i;
                    	//++childPos;
                    	throw new IOException("BTreeMain.insertNonLeafNode bad index result");
                    }
                    if( DEBUG )
                    	System.out.println("BTreeMain.insertIntoNonLeafNode index:"+i+" child:"+childPos);
                    BTreeKeyPage npage = node.getPage(childPos);
                    if( npage != null ) {
                    	// check to see if intended child node insertion point is full 
                    	if (npage.getNumKeys() == BTreeKeyPage.MAXKEYS) {
                    		// yes, split the node at insertion point, RECURSE!
                            splitChildNode(node, i, childPos, npage);
                            // repeat search to find child pointer to node that is root of insertion subtree
                            TreeSearchResult res = reposition(node, key);
                            npage = res.page;
                            if( DEBUG )
                            	System.out.println("BTreeMain.insertIntoNonLeafNode reposition result:"+res.insertPoint+" "+res.page);
                    	}
                    } else
                    	throw new IOException("non leaf node target page indvalid at index "+i+" child pos:"+childPos+" inserting key "+key+" for node "+node);
                    insertIntoNode(npage, key, object);       
            }
    }
    
    /**
     * Split the node, node, of a B-Tree into two nodes that both contain T-1 elements and 
     * move node's median key up to the parentNode.
     * This method will only be called if node is full; node is the i-th child of parentNode.
     * Note that this is the only time we ever create a child. Doing a split doesn't increase the
     * height of a tree, because we only add a sibling to existing keys at the same level. 
     * Thus, the only time the height of the tree ever increases is when we split the root. 
     * So we satisfy the part of the definition that says "each leaf must occur at the same depth."
     * if index is negative insert to the left of 0, else to the right of index
     * childIndex is the location in the parent where the new node pointer will go.
     * @param parentNode
     * @param keyIndex
     * @param childIndex
     * @param node
     * @throws IOException
     */
    synchronized void splitChildNode(BTreeKeyPage parentNode, int keyIndex, int childIndex,  BTreeKeyPage node) throws IOException {
    	if( DEBUG )
    		System.out.println("BTreeMain.splitChildNode index:"+keyIndex+" childIndex:"+childIndex+"parent:"+parentNode+" target:"+node);
            BTreeKeyPage newNode =  BTreeKeyPage.getPageFromPool(sdbio); // will set up blank page with updated set
            newNode.setmIsLeafNode(node.getmIsLeafNode());
            newNode.setNumKeys(T - 1);
            for (int j = 0; j < T - 1; j++) { // Copy the last T-1 elements of node into newNode
            	 moveKeyData(node, j + T, newNode, j, false);
            }
            if (!newNode.getmIsLeafNode()) {
                    for (int j = 0; j < T; j++) { // Copy the last T pointers of node into newNode
                    	moveChildData(node, j + T, newNode, j, false);
                        //newNode.mChildNodes[j] = node.mChildNodes[j + T];
                    }
                    for (int j = T; j <= node.getNumKeys(); j++) {
                    	node.nullPageArray(j);
                        //node.mChildNodes[j] = null;
                    }
            }
            for (int j = T; j < node.getNumKeys(); j++) {
                    node.setKey(j, null);
                    node.dataArray[j] = null;
                    node.setDataIdArray(j, Optr.emptyPointer);
            }
            
            node.setNumKeys(T - 1);
            //
            // The logic above only dealt with hardwired rules on interchange
            // now we deal with the variable index manipulation involving our key and child pointers
            // in most cases the child will be key+1 for a right insert, but, it may be
            // 0, 0 for an insert at the beginning where the new node is to the left
            // Insert a (child) pointer to node newNode into the parentNode, moving other keys and pointers as necessary.
            // First, move the child key page data down 1 slot to make room for new page pointer
            for (int j = parentNode.getNumKeys(); j >= childIndex; j--) {
            	moveChildData(parentNode, j, parentNode, j+1, false);
                //parentNode.mChildNodes[j + 1] = parentNode.mChildNodes[j];
            }
            // insert the new node as a child at the designated page index in the parent node, 
            parentNode.pageArray[childIndex] = newNode;
            //  also insert its Id from page pool
            parentNode.setPageIdArray(childIndex, newNode.pageId); // sets parent node updated
            // clear the keyIndex slot in the parent node by moving everything down 1 in reverse
            for (int j = parentNode.getNumKeys() - 1; j >= keyIndex; j--) {
            	moveKeyData(parentNode, j, parentNode, j+1, false);
                //parentNode.mKeys[j + 1] = parentNode.mKeys[j];
                //parentNode.mObjects[j + 1] = parentNode.mObjects[j];
            }  
            // insert the mid key (T-1, middle key number adjusted to index) data to the designated spot in the parent node
            // 
            //parentNode.setKey(keyIndex, node.getKey(T - 1));
            //parentNode.dataArray[keyIndex] = node.dataArray[T - 1];
            //parentNode.setDataIdArray(keyIndex, node.getDataId(T - 1)); // takes care of updated fields
            //parentNode.dataUpdatedArray[keyIndex] = true,  NOT node.dataUpdatedArray[T - 1];
            //
            // copy the key from 'node' at T-1 to keyIndex of parentNode, getting key and preserving location
            parentNode.copyKeyAndDataToArray(node, T-1, keyIndex);
            // zero the old node mid key, its moved
            node.nullKeyAndData(T-1);
            //node.setKey(T - 1,  null);
            //node.dataArray[T - 1] = null;
            //node.setDataIdArray(T - 1, null); null was a bug, should be Optr.empty // sets node dataId update
            // bump the parent node key count
            parentNode.setNumKeys(parentNode.getNumKeys() + 1); // sets parent node updated
            // they both were updated
            // put to log and deep store
            node.putPage();
            parentNode.putPage();
           	if( DEBUG )
        		System.out.println("BTreeMain.splitChildNode EXIT parent:"+parentNode+" index:"+keyIndex+" target:"+node+" new:"+newNode);
    }
    
    /**
     * Move the data from source and source index to target and targetIndex for the two pages.
     * Optionally null the source.
     * @param source
     * @param sourceIndex
     * @param target
     * @param targetIndex
     */
    public static void moveKeyData(BTreeKeyPage source, int sourceIndex, BTreeKeyPage target, int targetIndex, boolean nullify) {
        try {
            target.copyKeyAndDataToArray(source, sourceIndex, targetIndex);
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
        	source.nullKeyAndData(sourceIndex);
        }
    }
    /**
     * Move the data from source and source index to target and targetIndex for the two pages.
     * Optionally null the source.
     * @param source
     * @param sourceIndex
     * @param target
     * @param targetIndex
     */
    public static void moveChildData(BTreeKeyPage source, int sourceIndex, BTreeKeyPage target, int targetIndex, boolean nullify) {
        target.pageArray[targetIndex] = source.pageArray[sourceIndex];
        target.setPageIdArray(targetIndex, source.getPageId(sourceIndex));
        if( nullify ) {
        	source.nullPageArray(sourceIndex);
        }
    }
	
	/**
	* Remove key/data object.
	* @param newKey The key to delete
	* @return 0 if ok, <>0 if error
	* @exception IOException if seek or write failure
	*/
	@SuppressWarnings("rawtypes")
	public synchronized int delete(Comparable newKey) throws IOException {
		BTreeKeyPage tpage;
		int tindex;
		BTreeKeyPage leftPage;
		BTreeKeyPage rightPage;

		// Is the key there?
		if(!(search(newKey)).atKey)
			return (NOTFOUND);
		if( DEBUG ) System.out.println("--ENTERING DELETE LOOP FOR "+newKey+" with currentPage "+currentPage);
		while (true) {
			// If either left or right pointer
			// is null, we're at a leaf or a
			// delete is percolating up the tree
			// Collapse the page at the key
			// location
			if ((currentPage.getPage(currentIndex) == null) || 
				(currentPage.getPage(currentIndex + 1) == null)) {
				if( Props.DEBUG ) System.out.println("Delete loop No left/right pointer on "+currentPage);
				// Save non-null pointer
				if (currentPage.pageArray[currentIndex] == null)
					tpage = currentPage.getPage(currentIndex + 1);
				else
					tpage = currentPage.getPage(currentIndex);
				if( Props.DEBUG ) System.out.println("Delete selected non-null page is "+tpage);
				// At leaf - delete the key/data
				currentPage.deleteData(currentIndex);
				currentPage.delete(currentIndex);
				if( Props.DEBUG ) System.out.println("Just deleted "+currentPage+" @ "+currentIndex);
				// Rewrite non-null pointer
				currentPage.putPageToArray(tpage, currentIndex);
				// If we've deleted the last key from the page, eliminate the
				// page and pop up a level. Null the page pointer
				// at the index we've popped to.
				// If we can't pop, all the keys have been eliminated (or, they should have).
				// Null the root, clear the keycount
				if (currentPage.getNumKeys() == 0) {
					// Following guards against leaks
					if( DEBUG ) System.out.println("Delete found numKeys 0");
					currentPage.nullPageArray(0);
					if (pop()) { // Null pointer to node
						// just deleted
						if( DEBUG ) System.out.println("Delete popped "+currentPage+" nulling and decrementing key count");
						currentPage.nullPageArray(currentIndex);
						//--numKeys;
						//if( DEBUG ) System.out.println("Key count now "+numKeys+" returning");
						// Perform re-seek to re-establish location
						//search(newKey);
						return (0);
					}
					if( DEBUG ) System.out.println("Cant pop, clear root");
					// Can't pop -- clear the root
					// if root had pointer make new root
					if (tpage != null) {
						if( DEBUG ) System.out.println("Delete tpage not null, setting page Id/current page root "+tpage);
						tpage.pageId = 0L;
						setRoot(tpage);
						currentPage = tpage;
					} else {
						// no more keys
						if( DEBUG ) System.out.println("Delete No more keys, setting new root page");
						setRoot(new BTreeKeyPage(sdbio, 0L, true));
						getRoot().setUpdated(true);
						currentPage = null;
						//numKeys = 0;
					}
					//atKey = false;
					if( DEBUG ) System.out.println("Delete returning");
					return (0);
				}
				// If we haven't deleted the last key, see if we have few enough
				// keys on a sibling node to coalesce the two. If the
				// keycount is even, look at the sibling to the left first;
				// if the keycount is odd, look at the sibling to the right first.
				if( DEBUG ) System.out.println("Have not deleted last key");
				if (stack.size() == 0) { // At root - no siblings
					if( DEBUG ) System.out.println("Delete @ root w/no siblings");
					//--numKeys;
					//search(newKey);
					//if( DEBUG ) System.out.println("Delete returning after numKeys set to "+numKeys);
					return (0);
				}
				// Get parent page and index
				if( DEBUG ) System.out.println("Delete get parent page and index");
				tpage = (stack.get(stack.size() - 1)).keyPage;
				tindex =(stack.get(stack.size() - 1)).index;
				if( DEBUG ) System.out.println("Delete tpage now "+tpage+" and index now "+tindex+" with stack depth "+stack.size());
				// Get sibling pages
				if( DEBUG ) System.out.println("Delete Getting sibling pages");
				if (tindex > 0) {
					leftPage = tpage.getPage(tindex - 1);
					if( DEBUG ) System.out.println("Delete tindex > 0 @ "+tindex+" left page "+leftPage);
				} else {
					leftPage = null;
					if( DEBUG ) System.out.println("Delete tindex not > 0 @ "+tindex+" left page "+leftPage);
				}
				if (tindex < tpage.getNumKeys()) {
					rightPage = tpage.getPage(tindex + 1);
				} else {
					rightPage = null;
				}
				if( DEBUG ) System.out.println("Delete tindex "+tindex+" tpage.numKeys "+tpage.getNumKeys()+" right page "+rightPage);
				// Decide which sibling
				if( DEBUG ) System.out.println("Delete find sibling from "+leftPage+" -- " + rightPage);
				//if (numKeys % 2 == 0)
				if( new Random().nextInt(2) == 0 )
					if (leftPage == null)
						leftPage = currentPage;
					else
						rightPage = currentPage;
				else 
					if (rightPage == null)
						rightPage = currentPage;
					else
						leftPage = currentPage;
				if( DEBUG ) System.out.println("Delete found sibling from "+leftPage+" -- " + rightPage);

				// assertion check
				if (leftPage == null || rightPage == null) {
					if( DEBUG ) System.out.println("ASSERTION CHECK FAILED, left/right page null in delete");
					return (TREEERROR);
				}
				// Are the siblings small enough to coalesce
				// address coalesce later
				//if (leftPage.numKeys + rightPage.numKeys + 1 > BTreeKeyPage.MAXKEYS) {
					// Coalescing not possible, exit
					//--numKeys;
					//search(newKey);
					//if( DEBUG ) System.out.println("Cant coalesce, returning with keys="+numKeys);
					return (0);
				/*	
				} else {
					// Coalescing is possible. Grab the parent key, build a new
					// node. Discard the old node.
					// (If sibling is left page, then the new page is left page,
					// plus parent key, plus original page. New page is old left
					// page. If sibling is right page, then the new page
					// is the original page, the parent key, plus the right
					// page.
					// Once the new page is created, delete the key on the
					// parent page. Then cycle back up and delete the parent key.)
					coalesce(tpage, tindex, leftPage, rightPage);
					if( Props.DEBUG ) System.out.println("Coalesced "+tpage+" -- "+tindex+" -- "+leftPage+" -- "+rightPage);
					// Null right page of parent
					tpage.nullPageArray(tindex + 1);
					// Pop up and delete parent
					pop();
					if( Props.DEBUG ) System.out.println("Delete Popped, now continuing loop");
					continue;
				}
				*/
			} else {
				// Not at a leaf. Get a successor or predecessor key.
				// Copy the predecessor/successor key into the deleted key's slot, then "move
				// down" to the leaf and do a delete there.
				// Note that doing the delete could cause a "percolation" up the tree.
				// Save current page and  and index
				if( Props.DEBUG ) System.out.println("Delete Not a leaf, find next key");
				tpage = currentPage;
				tindex = currentIndex;
				if (currentPage.getPage(currentIndex) != null) {
					// Get predecessor if possible
					if( DEBUG ) System.out.println("Delete Seeking right tree");
					seekRightTree();	
				} else { // Get successor
					if (currentPage.getPage(currentIndex + 1) == null) {
						if( DEBUG ) System.out.println("Delete cant get successor, returning");
						return (TREEERROR);
					}
					currentIndex++;
					seekLeftTree();
					if( DEBUG ) System.out.println("Delete cant seek left tree, returning");
					return (STACKERROR);	
				}
				// Replace key/data with successor/predecessor
				tpage.putKeyToArray(currentPage.getKey(currentIndex), tindex);
				tpage.putDataToArray(
					currentPage.getDataFromArray(currentIndex),
					tindex);
				if( DEBUG ) System.out.println("Delete re-entring loop to delete key on leaf of tpage "+tpage);
				// Reenter loop to delete key on leaf
			}
		}
	}

	/** 
	* Coalesce a node
	* Combine the left page, the parent key (at offset index) and the right 
	* page contents.
	* The contents of the right page are nulled. Note that it's the job of the
	* caller to delete the parent.
	* @param parent The parent page
	* @param index The index on the parent page to move to end of left key
	* @param left The left page
	* @param right The right page
	* @exception IOException If seek/writes fail
	*/
	private synchronized void coalesce (
		BTreeKeyPage parent,
		int index,
		BTreeKeyPage left,
		BTreeKeyPage right)
		throws IOException {
		int i, j;

		// Append the parent key to the end of the left key page
		left.putKeyToArray(parent.getKey(index), left.getNumKeys());
		left.putDataToArray(parent.getDataFromArray(index), left.getNumKeys());

		// Append the contents of the right
		// page onto the left key page
		j = left.getNumKeys() + 1;
		for (i = 0; i < right.getNumKeys(); i++) {
			left.putPageToArray(right.getPage(i), j);
			left.putDataToArray(right.getDataFromArray(i), j);
			left.setKey(j, right.getKey(i));
			j++;
		}
		left.putPageToArray(
			right.getPage(right.getNumKeys()),
			left.getNumKeys() + right.getNumKeys() + 1);
		left.setNumKeys(left.getNumKeys() + (right.getNumKeys() + 1));

		// Null the right page (no leaks)
		for (i = 0; i < right.getNumKeys(); i++) {
			right.nullPageArray(i);
			right.setKey(i, null);
			right.putDataToArray(null, i);
		}
		right.nullPageArray(right.getNumKeys());
	}

	/**
	 * Rewind current position to beginning of tree. Sets up stack with pages and indexes
	 * such that traversal can take place. Remember to clear stack after these operations.
	 * @exception IOException If read fails
	 */
	public synchronized void rewind() throws IOException {
		currentPage = getRoot();
		currentIndex = 0;
		currentChild = 0;
		clearStack();
		seekLeftTree();
	}

	/**
	 * Set current position to end of tree.Sets up stack with pages and indexes
	 * such that traversal can take place. Remember to clear stack after these operations. 
	 * @exception IOException If read fails
	 */
	public synchronized void toEnd() throws IOException {
		rewind();
		while (gotoNextKey() == 0);
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
    private synchronized TreeSearchResult repositionStack(BTreeKeyPage node, Comparable key) throws IOException {
    	int i = 0;
    	BTreeKeyPage sourcePage = node;
    	if( DEBUG || DEBUGSEARCH) {
    		System.out.println("BTreeMain.repositionStack key:"+key+" node:"+node);
    	}
        while (sourcePage != null) {
                i = 0;
                //while (i < sourcePage.numKeys && key.compareTo(sourcePage.keyArray[i]) > 0) {
                while (i < sourcePage.getNumKeys() && sourcePage.getKey(i).compareTo(key) < 0) {
                        i++;
                }
                //if (i < sourcePage.numKeys && key.compareTo(sourcePage.keyArray[i]) == 0) {
                if (i < sourcePage.getNumKeys() && sourcePage.getKey(i).compareTo(key) == 0) {	
                 	if( DEBUG || DEBUGSEARCH )
                		System.out.println("BTreeMain.repositionStack set to return index :"+i+" after locating key for "+sourcePage);
                	return new TreeSearchResult(sourcePage, i, true);
                }
                if (sourcePage.getmIsLeafNode()) {
                	if( DEBUG || DEBUGSEARCH)
                		System.out.println("BTreeMain.repositionStack set to return index :"+i+" for leaf "+sourcePage);
                	if( i >= sourcePage.getNumKeys() || sourcePage.getKey(i).compareTo(key) < 0 ) {
                		currentPage = sourcePage;
                		popUntilValid(true);
                		return new TreeSearchResult(currentPage, currentIndex, true);
                	}
             		return new TreeSearchResult(sourcePage, i, true);
                } else {
                	BTreeKeyPage targetPage  = sourcePage.getPage(i);// get the page at the index of the given page
                	if( DEBUG || DEBUGSEARCH) {
                		System.out.println("BTreeMain.repositionStack traverse next page for key:"+key+" page:"+targetPage);
                	}
                	if( targetPage == null )
                		break;
                	TraversalStackElement tse = new TraversalStackElement(sourcePage, i, i);
                	stack.push(tse);
                  	sdbio.deallocOutstanding(sourcePage.pageId);
                	sourcePage = targetPage;
                }
        }
      	if( DEBUG || DEBUGSEARCH)
    		System.out.println("BTreeMain.repositionStack set to return index :"+i+" for fallthrough "+sourcePage);
        return new TreeSearchResult(sourcePage, i, false);
    }
	/**
	* Seek to location of next key in tree.
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
	public synchronized int gotoNextKey() throws IOException {
		if( DEBUG || DEBUGSEARCH ) {
			System.out.println("BTreeMain.gotoNextKey "/*page:"+currentPage+*/+" index "+currentIndex);
		}
		// If we are at a key, then advance the index
		if (currentChild < currentPage.getNumKeys()) {
				currentChild++;
				// If its a leaf and we just bumped we can return if its in range
				if( currentPage.getmIsLeafNode()) {	
					if(currentChild != currentPage.getNumKeys())  {
						currentIndex = currentChild;
						setCurrent();
						return 0;
					}
				}
				// Otherwise, we have to decide the return value, descend subtree
				// Not a leaf node with valid index, so we have to either return the index or find a subtree to descend
				// should be positioned at key to which we descend left subtree
				// 
				BTreeKeyPage tPage = currentPage.getPage(currentChild);
				if (tPage  == null) {
					// Pointer is null, we can return this index as we cant descend subtree
					if( DEBUG || DEBUGSEARCH) {
						System.out.println("BTreeMain.gotoNextKey index "+currentChild+" pointer is null");
					}
					//
					// No child pointer. If the current pointer is at end of key range we have to pop
					// otherwise we can return this key
					if(currentChild != currentPage.getNumKeys())  {
							currentIndex = currentChild;
							setCurrent();
							return 0;
					}	
				} else {
					// There is a child pointer
					// seek to "leftmost" key in currentPage subtree,the one we just tested, using currentChild
					seekLeftTree();
					setCurrent();
					return (0);	
				}
		}
		// If we are here we are at the end of the key range
		// cant move right, are we at root? If so, we must end
		// we have reached the end of keys
		// we must pop
		// pop to parent and fall through when we hit root or are not out of keys
		// go up 1 and right to the next key, if no right continue to pop until we have a right key
		// if we hit the root, protocol right. in any case we have to reassert
		// if there is a subtree follow it, else return the key
		return popUntilValid(true);
	}

	/**
	* Go to location of previous key in tree
	* @return 0 if ok, <>0 if error
	* @exception IOException If read fails
	*/
	public synchronized int gotoPrevKey() throws IOException {
		if( DEBUG || DEBUGSEARCH ) {
			System.out.println("BTreeMain.gotoNextKey "/*page:"+currentPage+*/+" index "+currentIndex);
		}
		// If we are at a key, then reduce the index
		if (currentChild > 0) {
				--currentChild;
				// If its a leaf and we just bumped we can return if its in range
				if( currentPage.getmIsLeafNode()) {	
						currentIndex = currentChild;
						setCurrent();
						return 0;
				}
				// Otherwise, we have to decide the return value, descend subtree
				// Not a leaf node with valid index, so we have to either return the index or find a subtree to descend
				// should be positioned at key to which we descend left subtree
				// 
				BTreeKeyPage tPage = currentPage.getPage(currentChild);
				if (tPage  == null) {
					// Pointer is null, we can return this index as we cant descend subtree
					if( DEBUG || DEBUGSEARCH) {
						System.out.println("BTreeMain.gotoPrevKey index "+currentChild+" pointer is null, returning");
					}
					//
					// No child pointer. If the current pointer is at end of key range we have to pop
					// otherwise we can return this key
					if(currentChild != 0)  {
							currentIndex = currentChild;
							setCurrent();
							return 0;
					}	
				} else {
					// There is a child pointer
					// seek to "rightmost" key in currentPage subtree,the one we just tested, using currentChild
					seekRightTree();
					setCurrent();
					return (0);	
				}
		}
		return popUntilValid(false);
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
		if( currentPage.pageId == 0L)
			return EOF;
		// we have reached the end of keys
		// we must pop
		// pop to parent and fall through when we hit root or are not out of keys
		// go up 1 and right to the next key, if no right continue to pop until we have a right key
		// if we hit the root, protocol right. in any case we have to reassert
		// if there is a subtree follow it, else return the key
		Comparable key = currentPage.getKey(currentIndex);
		while( pop() ) {
			if(DEBUG || DEBUGSEARCH) {
				System.out.println("BTreeMain.popUntilValid POP index:"+currentIndex+" child:"+currentChild+" page:"+currentPage);
			}
			// we know its not a leaf, we popped to it
			// If we pop, and we are at the end of key range, and our key is not valid, pop
			if( next ) {
				if(currentIndex >= currentPage.getNumKeys() || currentPage.getKey(currentIndex).compareTo(key) < 0) {	
					continue; // on a non-leaf, at an index heading right (ascending)
				}
			} else {
				if(currentIndex <= 0 || currentPage.getKey(currentIndex).compareTo(key) > 0) {
					continue; // on a non-leaf, at an index heading left (descending)
				}
			}
			// appears that a valid key is encountered
			//break;
			setCurrent();
			return 0;
		}
		// should be at position where we return key from which we previously descended
		// pop sets current indexes
		//setCurrent();
		//return 0;
		// popped to the top and have to stop
		return EOF;
	}
	/**
	* Set the current object and key based on value of currentPage.
	* and currentIndex
	*/
	public synchronized void setCurrent() throws IOException {
		if( DEBUG || DEBUGCURRENT)
			System.out.println("BTreeMain.setCurrent page:["+ currentIndex +"] "+currentPage);
		setCurrentKey(currentPage.getKey(currentIndex));
		setCurrentObject(currentPage.getDataFromArray(currentIndex));

	}
	/**
	* Set the current object and key based on value of currentPage.
	* and currentIndex
	*/
	public synchronized Comparable setCurrentKey() throws IOException {
		if( DEBUG || DEBUGCURRENT)
			System.out.println("BTreeMain.setCurrentKey page:"+currentPage+" index:"+currentIndex);
		setCurrentKey(currentPage.getKey(currentIndex));
		return currentPage.getKey(currentIndex);
	}
	/**
	 * Utilize reposition to locate key. Set currentPage, currentIndex, currentKey, and currentChild.
	 * deallocOutstanding is called before exit.
	 * @param targetKey The key to position to in BTree
	 * @return TreeSearchResult containing page, insertion index, atKey = true for key found
	 * @throws IOException
	 */
	public synchronized TreeSearchResult search(Comparable targetKey) throws IOException {
		currentPage = getRoot();
		currentIndex = 0;
		currentChild = 0;
		TreeSearchResult tsr = null;
		clearStack();
        if (currentPage != null) {
        	tsr = repositionStack(currentPage, targetKey);
        	setCurrent(tsr);
        }
    	if( DEBUG || DEBUGSEARCH) {
    		System.out.println("BTreeMain.search returning with currentPage:"+currentPage+" index:"+currentIndex+" child:"+currentChild);
    	}
        sdbio.deallocOutstanding(currentPage.pageId);
        return tsr;
}

	/**
	* Seeks to leftmost key in current subtree. Takes the currentChild and currentIndex from currentPage and uses the
	* child at currentChild to descend the subtree and gravitate left.
	*/
	private synchronized void seekLeftTree() throws IOException {
		if( DEBUG || DEBUGSEARCH ) {
			System.out.println("BTreeMain.seekLeftTree page:"+currentPage+" index:"+currentIndex+" child:"+currentChild);
		}
		BTreeKeyPage tPage = currentPage.getPage(currentChild);
		while (tPage != null) {
			// Push the 'old' value of currentPage, currentIndex etc as a TraversalStackElement since we are
			// intent on descending. After push and verification that tPage is not null, set page to tPage.
			push();
			if( DEBUG || DEBUGSEARCH )
				System.out.println("BTreeMain.seekLeftTree PUSH using "+currentPage+" index:"+currentIndex+" child:"+currentChild+" new entry:"+tPage);
			currentIndex = 0;
			currentChild = 0;
			currentPage = tPage;
			setCurrentKey(currentPage.getKey(currentIndex));
			tPage = currentPage.getPage(currentChild);
		}
	}

	/**
	* Seeks to rightmost key in current subtree
	*/
	private synchronized void seekRightTree() throws IOException {
		BTreeKeyPage tPage = currentPage.getPage(currentChild);
		while (tPage != null) {
			push();
			if( DEBUG || DEBUGSEARCH)
				System.out.println("BTreeMain.seekRightTree PUSH using "+currentPage+" index:"+currentIndex+" child:"+currentChild+" new entry:"+tPage);
			currentIndex = currentPage.getNumKeys() - 1;
			currentChild = currentPage.getNumKeys();
			currentPage = tPage;
			setCurrentKey(currentPage.getKey(currentIndex));
			tPage = currentPage.getPage(currentChild);
		}
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
		if( currentPage == null )
			throw new RuntimeException("BTreeMain.push cant push a null page to stack");
		//keyPageStack[stackDepth] = currentPage;
		//childStack[stackDepth] = currentChild;
		//indexStack[stackDepth++] = currentIndex;
		stack.push(new TraversalStackElement(currentPage, currentIndex, currentChild));
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
	private synchronized boolean pop() {
		//if (stackDepth == 0)
		if( stack.isEmpty() )
			return (false);
		//stackDepth--;
		//currentPage = keyPageStack[stackDepth];
		//currentIndex = indexStack[stackDepth];
		//currentChild = childStack[stackDepth];
		TraversalStackElement tse = stack.pop();
		currentPage = tse.keyPage;
		currentIndex = tse.index;
		currentChild = tse.child;
		if( DEBUG ) {
			System.out.print("BTreeMain.Pop:");
			printStack();
		}
		return (true);
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
	public synchronized void clearStack() {
		//for (int i = 0; i < MAXSTACK; i++)
		//	keyPageStack[i] = null;
		//stackDepth = 0;
		stack.clear();
	}

	/**
	 * Set the currentPage, currentIndex, currentChild to TreeSearchResult values.
	 * then calls setCurrent() to populate current key and Object data values.
	 * @param tsr
	 * @throws IOException
	 */
	public synchronized void setCurrent(TreeSearchResult tsr) throws IOException {
		currentPage = tsr.page;
		currentIndex = tsr.insertPoint;
		currentChild = tsr.insertPoint;
		setCurrent(); // sets up currenKey and currentObject;
	}
	
	public synchronized Object getCurrentObject() {
		return currentObject;
	}

	public synchronized void setCurrentObject(Object currentObject) {
		this.currentObject = currentObject;
	}

	@SuppressWarnings("rawtypes")
	public synchronized Comparable getCurrentKey() {
		return currentKey;
	}

	@SuppressWarnings("rawtypes")
	public synchronized void setCurrentKey(Comparable currentKey) {
		this.currentKey = currentKey;
	}

	public synchronized ObjectDBIO getIO() {
		return sdbio;
	}

	public synchronized void setIO(ObjectDBIO sdbio) {
		this.sdbio = sdbio;
	}

	public synchronized BTreeKeyPage getRoot() {
		return root;
	}
	/**
	 * Set the root node value, return it as fluid pattern
	 * @param root
	 * @return
	 */
	public synchronized BTreeKeyPage setRoot(BTreeKeyPage root) {
		this.root = root;
		return root;
	}
	
    // Inorder walk over the tree.
    synchronized void printBTree(BTreeKeyPage node) throws IOException {
            if (node != null) {
                    if (node.getmIsLeafNode()) {
                    	System.out.print("Leaf node:");
                            for (int i = 0; i < node.getNumKeys(); i++) {
                                    System.out.print("INDEX:"+i+" node:"+node.getKey(i) + ", ");
                            }
                            System.out.println("\n");
                    } else {
                    	System.out.print("NonLeaf node:");
                            int i;
                            for (i = 0; i < node.getNumKeys(); i++) {
                            	BTreeKeyPage btk = node.getPage(i);
                                printBTree(btk);
                                System.out.print("INDEX:"+i+" node:"+ node.getKey(i) + ", ");
                            }
                            // get last far right node
                            printBTree(node.getPage(i));
                            System.out.println("\n");
                    }                       
            }
    }
    
    
    synchronized void validate() throws Exception {
            ArrayList<Comparable> array = getKeys(getRoot());
            for (int i = 0; i < array.size() - 1; i++) {            
                    if (array.get(i).compareTo(array.get(i + 1)) >= 0) {
                            throw new Exception("B-Tree invalid: " + array.get(i)  + " greater than " + array.get(i + 1));
                    }
        }           
    }
    
    // Inorder walk over the tree.
   synchronized ArrayList<Comparable> getKeys(BTreeKeyPage node) throws IOException {
            ArrayList<Comparable> array = new ArrayList<Comparable>();
            if (node != null) {
                    if (node.getmIsLeafNode()) {
                            for (int i = 0; i < node.getNumKeys(); i++) {
                                    array.add(node.getKey(i));
                            }
                    } else {
                            int i;
                            for (i = 0; i < node.getNumKeys(); i++) {
                                    array.addAll(getKeys(node.getPage(i)));
                                    array.add(node.getKey(i));
                            }
                            array.addAll(getKeys(node.getPage(i)));
                    }                       
            }
            return array;
    }
  


}
