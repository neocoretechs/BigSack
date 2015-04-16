package com.neocoretechs.bigsack.btree;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
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
* @author Groff Copyright (C) NeoCoreTechs 2015
*/
public final class BTreeMain {
	private static boolean DEBUG = false; // General debug
	private static boolean DEBUGCURRENT = false; // alternate debug level to view current page assignment of BTreeKeyPage
	private static boolean TEST = true; // Do a table scan and key count at startup
	private static boolean ALERT = true; // Info level messages
	private static boolean OVERWRITE = false; // flag to determine whether value data is overwritten for a key or its ignored
	static int BOF = 1;
	static int EOF = 2;
	static int NOTFOUND = 3;
	static int ALREADYEXISTS = 4;
	static int STACKERROR = 5;
	static int TREEERROR = 6;
	static int MAXSTACK = 64;

	private BTreeKeyPage root;
	private long numKeys;
	BTreeKeyPage currentPage;
	int currentIndex;
	@SuppressWarnings("rawtypes")
	private Comparable currentKey;
	private Object currentObject;
	BTreeKeyPage[] keyPageStack;
	int[] indexStack;
	int stackDepth;
	private CyclicBarrier nodeSplitSynch = new CyclicBarrier(3);
	private NodeSplitThread leftNodeSplitThread, rightNodeSplitThread;
	private ObjectDBIO sdbio;
	static int T = (BTreeKeyPage.MAXKEYS/2)+1;

	public BTreeMain(ObjectDBIO sdbio) throws IOException {
		this.sdbio = sdbio;
		keyPageStack = new BTreeKeyPage[MAXSTACK];
		indexStack = new int[MAXSTACK];
		currentPage = setRoot(BTreeKeyPage.getPageFromPool(sdbio, 0L));
		if( DEBUG ) System.out.println("Root BTreeKeyPage: "+currentPage);
		currentIndex = 0;
		// Attempt to retrieve last good key count
		numKeys = 0;//sdbio.getKeycountfile().getKeysCount();
		// Append the worker name to thread pool identifiers, if there, dont overwrite existing thread group
		ThreadPoolManager.init(new String[]{"NODESPLITWORKER"}, false);
		leftNodeSplitThread = new NodeSplitThread(nodeSplitSynch);
		rightNodeSplitThread = new NodeSplitThread(nodeSplitSynch);
		ThreadPoolManager.getInstance().spin(leftNodeSplitThread,"NODESPLITWORKER");
		ThreadPoolManager.getInstance().spin(rightNodeSplitThread,"NODESPLITWORKER");
		
		// Consistency check test, also needed to get number of keys
		// Performs full tree/table scan, tallys record count
		if( TEST ) {
			long tim = System.currentTimeMillis();
			printBTree(getRoot());
			count();
			System.out.println("Consistency check for "+sdbio.getDBName()+" returned "+numKeys+" keys in "+(System.currentTimeMillis()-tim)+" ms.");
			// deallocate outstanding blocks in all tablespaces
			sdbio.deallocOutstanding();
			//if( Props.DEBUG ) System.out.println("Records: " + numKeys);
		}
		if( ALERT )
			System.out.println("Database "+sdbio.getDBName()+" ready.");

	}
	/**
	 * Returns number of table scanned keys, sets numKeys field
	 * @throws IOException
	 */
	public synchronized long count() throws IOException {
		rewind();
		int i;
		numKeys = 0;
		long tim = System.currentTimeMillis();
		while ((i = gotoNextKey()) == 0) {
			//if( DEBUG )
				System.out.println("gotoNextKey returned: "+i+" "+currentPage.keyArray[i]);
			++numKeys;
		}
		if( DEBUG )
		System.out.println("Count for "+sdbio.getDBName()+" returned "+numKeys+" keys in "+(System.currentTimeMillis()-tim)+" ms.");
		// deallocate outstanding blocks in all tablespaces
		sdbio.deallocOutstanding();
		return numKeys;
	}
	
	public synchronized boolean isEmpty() {
		return (getRoot().numKeys == 0);
	}
	/**
	* currentPage and currentIndex set.
	* @param targetKey The Comparable key to seek
	* @return data Object if found. Null otherwise.
	* @exception IOException if read failure
	*/
	@SuppressWarnings("rawtypes")
	public synchronized Object seekObject(Comparable targetKey) throws IOException {
		if (search(targetKey).atKey) {
			setCurrent();
			return getCurrentObject();
		} else {
			// See if the page is on overflow
			if ((currentPage.numKeys) == BTreeKeyPage.MAXKEYS) {
				do {
					if (currentIndex < BTreeKeyPage.MAXKEYS) {
						setCurrent();
						return getCurrentObject();
					}
				} while(pop());
			}
			return null;
		}
	}
	/**
	* Seek the key, if we dont find it return false and leave the tree at it position
	* If we do find it return true and leave at found key
	* @param targetKey The Comparable key to seek
	* @return data Object if found. Null otherwise.
	* @exception IOException if read failure
	*/
	@SuppressWarnings("rawtypes")
	public synchronized boolean seekKey(Comparable targetKey) throws IOException {
		if (search(targetKey).atKey) {
			setCurrentKey();
			if( DEBUG )
				System.out.println("SeekKey SUCCESS and state is currentIndex:"+currentIndex+" targKey:"+targetKey+" "+currentPage);
			return true;
		} else {
			if( DEBUG )
				System.out.println("SeekKey MISSED and state is currentIndex:"+currentIndex+" targKey:"+targetKey+" "+currentPage);
			return false;
		}
	}
	
	public int add(Comparable key) throws IOException {
		return add(key, null);
	}
	/**
	 * Add an object and/or key to the deep store. Traverse the BTree for the insertion point and insert.
	 * @param key
	 * @param object
	 * @throws IOException
	 */
	public int add(Comparable key, Object object) throws IOException {
		/*
         TreeSearchResult tar = update(key, object);
         //TreeSearchResult tsr = update(key, object); 
         if (!tar.atKey) { // value false indicates insertion spot
                 if (currentPage.numKeys == BTreeKeyPage.MAXKEYS) {
                        // Split rootNode and move its two outer keys down to a left and right subnode.
                	 	// perform the insertion on the new key/value at the insertion point translated to
                	 	// whichever subnode is appropriate. The subnode inserted can be retrieved by obtaining the
                	 	// two requests issued to the NodeSplitThreads and examining their wasNodeInserted() and 
                	 	// getInsertionPoint() methods
                        splitChildNode(currentPage, tar.insertPoint, key, object);
                 } else {
                        insertIntoNonFullNode(currentPage, tar.insertPoint, key, object); // Insert the key into the B-Tree with root rootNode.
                 }
         }
     	 // deallocate the old buffers before we get another page
     	 //sdbio.deallocOutstanding(currentPage.pageId);
         return tar.insertPoint;
         */
        BTreeKeyPage rootNode = getRoot();
        TreeSearchResult usr = update(rootNode, key, object);
        if (!usr.atKey) {
        		BTreeKeyPage targetNode = usr.page;
                if (rootNode.numKeys == (2 * T - 1)) {
                       splitNodeBalance(rootNode);
                       // re position insertion point after split
                       if( DEBUG )
                    	   System.out.println("BTreeMain.add calling reposition after splitRootNode for key:"+key+" node:"+rootNode);
                       TreeSearchResult repos = reposition(rootNode, key);
                       targetNode = repos.page;
                } 
                insertIntoNode(targetNode, key, object); // Insert the key into the B-Tree with root rootNode.
        }
        return 0;
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
    private TreeSearchResult update(BTreeKeyPage node, Comparable key, Object object) throws IOException {
    	int i = 0;
    	BTreeKeyPage sourcePage = node;
        while (sourcePage != null) {
                i = 0;
                while (i < sourcePage.numKeys && key.compareTo(sourcePage.keyArray[i]) > 0) {
                        i++;
                }
                if (i < sourcePage.numKeys && key.compareTo(sourcePage.keyArray[i]) == 0) {
                	if( object != null && OVERWRITE) {
            			sourcePage.deleteData(sdbio, i);
                        sourcePage.putDataToArray(object,i);
                	}
                	if( DEBUG && object != null && OVERWRITE)
                		System.out.println("BTreeMain.update set to return index :"+i+" AFTER UPDATE for "+sourcePage);
                	else
                    	System.out.println("BTreeMain.update set to return index :"+i+" sans update for "+sourcePage);
                	return new TreeSearchResult(sourcePage, i, true);
                }
                if (sourcePage.mIsLeafNode) {
                	if( DEBUG )
                		System.out.println("BTreeMain.update set to return index :"+i+" for "+sourcePage);
                        return new TreeSearchResult(sourcePage, i, false);
                } else {
                	BTreeKeyPage targetPage  = sourcePage.getPage(sdbio, i);// get the page at the index of the given page
                	if( targetPage == null )
                		break;
                	sdbio.deallocOutstanding(node.pageId);
                	sourcePage = targetPage;
                }
        }
     	if( DEBUG )
    		System.out.println("BTreeMain.update set to return index :"+i+" on fallthrough for "+sourcePage);
        return new TreeSearchResult(sourcePage, i, false);
    }
	/**
	 * Same as update without the actual updating.
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
    private TreeSearchResult reposition(BTreeKeyPage node, Comparable key) throws IOException {
    	int i = 0;
    	BTreeKeyPage sourcePage = node;
        while (sourcePage != null) {
                i = 0;
                while (i < sourcePage.numKeys && key.compareTo(sourcePage.keyArray[i]) > 0) {
                        i++;
                }
                if (i < sourcePage.numKeys && key.compareTo(sourcePage.keyArray[i]) == 0) {
                 	if( DEBUG )
                		System.out.println("BTreeMain.reposition set to return index :"+i+" after locating key for "+sourcePage);
                	return new TreeSearchResult(sourcePage, i, true);
                }
                if (sourcePage.mIsLeafNode) {
                	if( DEBUG )
                		System.out.println("BTreeMain.reposition set to return index :"+i+" for leaf "+sourcePage);
                        return new TreeSearchResult(sourcePage, i, false);
                } else {
                	BTreeKeyPage targetPage  = sourcePage.getPage(sdbio, i);// get the page at the index of the given page
                	if( targetPage == null )
                		break;
                	sdbio.deallocOutstanding(node.pageId);
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
    
    void splitNodeBalance(BTreeKeyPage parentNode) throws IOException { 
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
					if(parentNode.keyArray[j] == null ) 
						++nullKeys;
					else
						break;
				}
				if( DEBUG ) {
					System.out.println("BTreeMain.splitNodeBalance nullKeys:"+nullKeys);
				}
				// find the number of good keys from the nulls forward
				for(int j = nullKeys; j < BTreeKeyPage.MAXKEYS; j++) {
					if(parentNode.keyArray[j] != null )
						++goodKeys;
					else
						break;
				}
				// set as non leaf so proper insertion and compaction occurs
				parentNode.mIsLeafNode = false;
				
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
				parentNode.numKeys = goodKeys;
				parentNode.setUpdated(true);
				if( DEBUG ) {
					System.out.println("BTreeMain.spliNodeBalance moving to putPage:"+currentPage);
				}
				//
				// Write the three new pages back to deep store and log them along the way
				// Deallocate (unlatch) them after the write completes.
				//
				parentNode.putPage(sdbio);
				((BTreeKeyPage)lnsr.getObjectReturn()).putPage(sdbio);
				((BTreeKeyPage)rnsr.getObjectReturn()).putPage(sdbio);
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
    void insertIntoNode(BTreeKeyPage node, Comparable key, Object object) throws IOException {
    	if( DEBUG )
    		System.out.println("BTreeMain.insertIntoNode:"+key+" "+object+" Node:"+node);
            int i = node.numKeys - 1;
            if (node.mIsLeafNode) {
            	// If node is full, initiate split, as we do with a full root, we are going to split this
            	// node by pulling the center key, and creating 2 new balanced children
            	if (node.numKeys == BTreeKeyPage.MAXKEYS) {
            		splitNodeBalance(node);
            		insertIntoNode(reposition(node, key).page, key, object);
            		return;
            	}
                // If node is not a full node insert the new element into its proper place within node.
                while (i >= 0 && key.compareTo(node.keyArray[i]) < 0) {
                    	moveKeyData(node, i, node, i+1, false);
                        i--;
                }
                i++;
                node.keyArray[i] = key;
                // leaf, no child data
                if( object != null ) {
                    	node.dataArray[i] = object;
                    	node.dataIdArray[i] = Optr.emptyPointer;
                    	node.dataUpdatedArray[i] = true;
                }
                node.numKeys++;
                node.setUpdated(true);
                node.putPage(sdbio);
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
            		i = node.numKeys-1;
            		int childPos = i + 1;
            		// keep going backward checking while key is < keyArray or no child pointer
            		// if not found the key was > all pointers
                    while (i >= 0 && (node.pageIdArray[childPos] == -1 || key.compareTo(node.keyArray[i]) < 0)) {
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
                    BTreeKeyPage npage = node.getPage(sdbio,childPos);
                    if( npage != null ) {
                    	// check to see if intended child node insertion point is full 
                    	if (npage.numKeys == BTreeKeyPage.MAXKEYS) {
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
    
    // Split the node, node, of a B-Tree into two nodes that both contain T-1 elements and 
    // move node's median key up to the parentNode.
    // This method will only be called if node is full; node is the i-th child of parentNode.
    // Note that this is the only time we ever create a child. Doing a split doesn't increase the
    // height of a tree, because we only add a sibling to existing keys at the same level. 
    // Thus, the only time the height of the tree ever increases is when we split the root. 
    // So we satisfy the part of the definition that says "each leaf must occur at the same depth."
    // if index is negative insert to the left of 0, else to the right of index
    // childIndex is the location in the parent where the new node pointer will og
    void splitChildNode(BTreeKeyPage parentNode, int keyIndex, int childIndex,  BTreeKeyPage node) throws IOException {
    	if( DEBUG )
    		System.out.println("BTreeMain.splitChildNode index:"+keyIndex+" childIndex:"+childIndex+"parent:"+parentNode+" target:"+node);
    	//if( i == -1 )
    		//throw new IOException("Invalid index -1 for splitChildNode with parent:"+parentNode+" target:"+node);
            BTreeKeyPage newNode =  BTreeKeyPage.getPageFromPool(sdbio);
            newNode.mIsLeafNode = node.mIsLeafNode;
            newNode.numKeys = T - 1;
            for (int j = 0; j < T - 1; j++) { // Copy the last T-1 elements of node into newNode
            	 moveKeyData(node, j + T, newNode, j, false);
            }
            if (!newNode.mIsLeafNode) {
                    for (int j = 0; j < T; j++) { // Copy the last T pointers of node into newNode
                    	moveChildData(node, j + T, newNode, j, false);
                        //newNode.mChildNodes[j] = node.mChildNodes[j + T];
                    }
                    for (int j = T; j <= node.numKeys; j++) {
                    	node.nullPageArray(j);
                        //node.mChildNodes[j] = null;
                    }
            }
            for (int j = T; j < node.numKeys; j++) {
                    node.keyArray[j] = null;
                    node.dataArray[j] = null;
                    node.dataIdArray[j] = Optr.emptyPointer;
            }
            
            node.numKeys = T - 1;
            //
            // The logic above only dealt with hardwired rules on interchange
            // now we deal with the variable index manipulation involving our key and child pointers
            // in most cases the child will be key+1 for a right insert, but, it may be
            // 0, 0 for an insert at the beginning where the new node is to the left
            // Insert a (child) pointer to node newNode into the parentNode, moving other keys and pointers as necessary.
            // First, move the child key page data down 1 slot to make room for new page pointer
            for (int j = parentNode.numKeys; j >= childIndex; j--) {
            	moveChildData(parentNode, j, parentNode, j+1, false);
                //parentNode.mChildNodes[j + 1] = parentNode.mChildNodes[j];
            }
            // insert the new node as a child at the designated page index in the parent node, 
            parentNode.pageArray[childIndex] = newNode;
            //  also insert its Id from page pool
            parentNode.pageIdArray[childIndex] = newNode.pageId;
            // set it to updated for write
            parentNode.setUpdated(true);
            // clear the keyIndex slot in the parent node by moving everything down 1 in reverse
            for (int j = parentNode.numKeys - 1; j >= keyIndex; j--) {
            	moveKeyData(parentNode, j, parentNode, j+1, false);
                //parentNode.mKeys[j + 1] = parentNode.mKeys[j];
                //parentNode.mObjects[j + 1] = parentNode.mObjects[j];
            }  
            // insert the mid key (T-1, middle key number adjusted to index) data to the designated spot in the parent node
            // 
            parentNode.keyArray[keyIndex] = node.keyArray[T - 1];
            parentNode.dataArray[keyIndex] = node.dataArray[T - 1];
            parentNode.dataIdArray[keyIndex] = node.dataIdArray[T - 1];
            // the data has not been updated, it does not need rewritten, the pointer has just traveled and is written with key
            parentNode.dataUpdatedArray[keyIndex] = false;
            // zero the old node mid key, its moved
            node.keyArray[T - 1] = null;
            node.dataArray[T - 1] = null;
            node.dataIdArray[T - 1] = null;
            // bump the parent node key count
            parentNode.numKeys++; 
            // they both were updated
            node.setUpdated(true);
            parentNode.setUpdated(true);
            // put to log and deep store
            node.putPage(sdbio);
            parentNode.putPage(sdbio);
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
        	target.keyArray[targetIndex] = source.keyArray[sourceIndex];
        	target.dataArray[targetIndex] = source.dataArray[sourceIndex];
        	target.dataIdArray[targetIndex] = source.dataIdArray[sourceIndex];
        	target.dataUpdatedArray[targetIndex] = source.dataUpdatedArray[sourceIndex];
     
        if( nullify ) {
            	source.keyArray[sourceIndex] = null;
            	source.dataArray[sourceIndex] = null;
            	source.dataIdArray[sourceIndex] = Optr.emptyPointer;
            	source.dataUpdatedArray[sourceIndex] = false; // have not updated off-page data, just moved its pointer
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
        target.pageIdArray[targetIndex] = source.pageIdArray[sourceIndex];
        if( nullify ) {
        	source.nullPageArray(sourceIndex);
        }
    }
	
    /*
	public synchronized int add(Comparable newKey) throws IOException {
		if( !search(newKey).atKey)
			return addx(newKey);
		return -1;
	}
	*/

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
			if ((currentPage.getPage(getIO(), currentIndex) == null) || 
				(currentPage.getPage(getIO(), currentIndex + 1) == null)) {
				if( Props.DEBUG ) System.out.println("Delete loop No left/right pointer on "+currentPage);
				// Save non-null pointer
				if (currentPage.pageArray[currentIndex] == null)
					tpage = currentPage.getPage(getIO(), currentIndex + 1);
				else
					tpage = currentPage.getPage(getIO(), currentIndex);
				if( Props.DEBUG ) System.out.println("Delete selected non-null page is "+tpage);
				// At leaf - delete the key/data
				currentPage.deleteData(getIO(), currentIndex);
				currentPage.delete(currentIndex);
				if( Props.DEBUG ) System.out.println("Just deleted "+currentPage+" @ "+currentIndex);
				// Rewrite non-null pointer
				currentPage.putPageToArray(tpage, currentIndex);
				// If we've deleted the last key from the page, eliminate the
				// page and pop up a level. Null the page pointer
				// at the index we've popped to.
				// If we can't pop, all the keys have been eliminated (or, they should have).
				// Null the root, clear the keycount
				if (currentPage.numKeys == 0) {
					// Following guards against leaks
					if( DEBUG ) System.out.println("Delete found numKeys 0");
					currentPage.nullPageArray(0);
					if (pop()) { // Null pointer to node
						// just deleted
						if( DEBUG ) System.out.println("Delete popped "+currentPage+" nulling and decrementing key count");
						currentPage.nullPageArray(currentIndex);
						--numKeys;
						if( DEBUG ) System.out.println("Key count now "+numKeys+" returning");
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
						setRoot(new BTreeKeyPage(0L));
						getRoot().setUpdated(true);
						currentPage = null;
						numKeys = 0;
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
				if (stackDepth == 0) { // At root - no siblings
					if( DEBUG ) System.out.println("Delete @ root w/no siblings");
					--numKeys;
					//search(newKey);
					if( DEBUG ) System.out.println("Delete returning after numKeys set to "+numKeys);
					return (0);
				}
				// Get parent page and index
				if( DEBUG ) System.out.println("Delete get parent page and index");
				tpage = keyPageStack[stackDepth - 1];
				tindex = indexStack[stackDepth - 1];
				if( DEBUG ) System.out.println("Delete tpage now "+tpage+" and index now "+tindex+" with stack depth "+stackDepth);
				// Get sibling pages
				if( DEBUG ) System.out.println("Delete Getting sibling pages");
				if (tindex > 0) {
					leftPage = tpage.getPage(getIO(), tindex - 1);
					if( DEBUG ) System.out.println("Delete tindex > 0 @ "+tindex+" left page "+leftPage);
				} else {
					leftPage = null;
					if( DEBUG ) System.out.println("Delete tindex not > 0 @ "+tindex+" left page "+leftPage);
				}
				if (tindex < tpage.numKeys) {
					rightPage = tpage.getPage(getIO(), tindex + 1);
				} else {
					rightPage = null;
				}
				if( DEBUG ) System.out.println("Delete tindex "+tindex+" tpage.numKeys "+tpage.numKeys+" right page "+rightPage);
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
					--numKeys;
					//search(newKey);
					if( DEBUG ) System.out.println("Cant coalesce, returning with keys="+numKeys);
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
				if (currentPage.getPage(getIO(), currentIndex) != null) {
					// Get predecessor if possible
					if( DEBUG ) System.out.println("Delete Seeking right tree");
					if (!seekRightTree()) {
						if( DEBUG ) System.out.println("Delete cant seek right tree, returning");
						return (STACKERROR);
					}
				} else { // Get successor
					if (currentPage.getPage(getIO(), currentIndex + 1) == null) {
						if( DEBUG ) System.out.println("Delete cant get successor, returning");
						return (TREEERROR);
					}
					currentIndex++;
					if (!seekLeftTree()) {
						if( DEBUG ) System.out.println("Delete cant seek left tree, returning");
						return (STACKERROR);
					}
				}
				// Replace key/data with successor/predecessor
				tpage.putKeyToArray(currentPage.keyArray[currentIndex], tindex);
				tpage.putDataToArray(
					currentPage.getDataFromArray(getIO(), currentIndex),
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
	private void coalesce (
		BTreeKeyPage parent,
		int index,
		BTreeKeyPage left,
		BTreeKeyPage right)
		throws IOException {
		int i, j;

		// Append the parent key to the end of the left key page
		left.putKeyToArray(parent.keyArray[index], left.numKeys);
		left.putDataToArray(parent.getDataFromArray(getIO(), index), left.numKeys);

		// Append the contents of the right
		// page onto the left key page
		j = left.numKeys + 1;
		for (i = 0; i < right.numKeys; i++) {
			left.putPageToArray(right.getPage(getIO(), i), j);
			left.putDataToArray(right.getDataFromArray(getIO(), i), j);
			left.keyArray[j] = right.keyArray[i];
			j++;
		}
		left.putPageToArray(
			right.getPage(getIO(), right.numKeys),
			left.numKeys + right.numKeys + 1);
		left.numKeys += right.numKeys + 1;

		// Null the right page (no leaks)
		for (i = 0; i < right.numKeys; i++) {
			right.nullPageArray(i);
			right.keyArray[i] = null;
			right.putDataToArray(null, i);
		}
		right.nullPageArray(right.numKeys);
	}

	/**
	 * Rewind current position to beginning of tree
	 * @exception IOException If read fails
	 */
	public synchronized void rewind() throws IOException {
		currentPage = getRoot();
		currentIndex = 0;
		clearStack();
		if (currentPage.getPage(getIO(), currentIndex) != null) {
			if( DEBUG )
				System.out.println("BTreeMain.rewind about to seek left tree using "+currentPage);
			seekLeftTree();
		}
		//atKey = false;
	}

	/**
	 * Set current position to end of tree
	 * @exception IOException If read fails
	 */
	public synchronized void toEnd() throws IOException {
		currentPage = getRoot();
		if (currentPage.numKeys != 0) {
			clearStack();
			currentIndex = currentPage.numKeys;
			if (currentPage.getPage(getIO(), currentIndex) != null) {
				seekRightTree();
				currentIndex++;
			}
		} else
			currentIndex = 0;
		//atKey = false;
	}

	/**
	* Seek to location of next key in tree
	* @return 0 if ok, != 0 if error
	* @exception IOException If read fails
	*/
	public synchronized int gotoNextKey() throws IOException {

		//if (getNumKeys() == 0)
		//	return (EOF);

		// If we are at a key, then advance the index
		if (currentPage.keyArray[currentIndex] != null)
			currentIndex++;
		
		if( DEBUG ) {
			System.out.println("BTreeMain.gotoNextKey "/*page:"+currentPage+*/+" index "+currentIndex);
		}
		// If we are not at a key, then see if the pointer is null.
		if (currentPage.getPage(getIO(), currentIndex) == null) {
			// Pointer is null, is it the last one on the page?
			if( DEBUG ) {
				System.out.println("BTreeMain.gotoNextKey index "+currentIndex+" pointer is null");
			}
			if (currentIndex == currentPage.numKeys) {
				// Last pointer on page. We have to pop up
				while (pop()) {
					if( DEBUG ) {
						System.out.println("BTreeMain.gotoNextKey POP index "+currentIndex+/*" "+currentPage+*/" working toward keys:"+currentPage.numKeys);
					}
					if (currentIndex != currentPage.numKeys) {
						setCurrent();
						return (0);
					}
				}
				return (EOF);
			} else { // Not last pointer on page.
				// Skip to next key
				setCurrent();
				return (0);
			}
		}
		// Pointer not null, seek to "leftmost" key in current subtree
		if (seekLeftTree()) {
			setCurrent();
			return (0);
		}
		return (EOF);
	}

	/**
	* Go to location of previous key in tree
	* @return 0 if ok, <>0 if error
	* @exception IOException If read fails
	*/
	public synchronized int gotoPrevKey() throws IOException {
		//if (getNumKeys() == 0)
		//	return (BOF);
		// If we are at a key, then simply back up the index
		// If we are not at a key, then see if
		// the pointer is null.
		if (currentPage.getPage(getIO(), currentIndex) == null)
			// Pointer is null, is it the first one on the page?
			if (currentIndex == 0) {
				// First pointer on page. We have to pop up
				while (pop())
					if (currentIndex != 0) {
						currentIndex--;
						setCurrent();
						return (0);
					}
				return (BOF);
			} else {
				// Not first pointer on page. Skip to previous key
				currentIndex--;
				setCurrent();
				return (0);
			}
		// Pointer not null, seek to "rightmost" key in current subtree
		if (seekRightTree()) {
			setCurrent();
			return (0);
		}
		return (EOF);
	}

	/**
	* Set the current object and key based on value of currentPage
	* and currentIndex
	*/
	public synchronized void setCurrent() throws IOException {
		if( DEBUG || DEBUGCURRENT)
			System.out.println("BTreeMain.setCurrent page:"+currentPage+" index:"+currentIndex);
		setCurrentKey(currentPage.keyArray[currentIndex]);
		setCurrentObject(currentPage.getDataFromArray(getIO(), currentIndex));
	}
	/**
	* Set the current object and key based on value of currentPage
	* and currentIndex
	*/
	public synchronized Comparable setCurrentKey() throws IOException {
		if( DEBUG || DEBUGCURRENT)
			System.out.println("BTreeMain.setCurrentKey page:"+currentPage+" index:"+currentIndex);
		setCurrentKey(currentPage.keyArray[currentIndex]);
		return currentPage.keyArray[currentIndex];
	}
	/**
	* 
	* search method used by seek, insert, and delete etc.
	* @param targetKey The key to search for
	* @return true if found
	* @exception IOException If read fails
	*/
	@SuppressWarnings("rawtypes")
	public synchronized TreeSearchResult search(Comparable targetKey) throws IOException {
		// Search - start at root
		clearStack();
		currentPage = getRoot();
		// File empty?
		if (currentPage.numKeys == 0) {
			if( DEBUG ) System.out.println("*** NO KEYS! ***");
			return new TreeSearchResult(0, false);
		}
		do {
			TreeSearchResult tsr = currentPage.search(targetKey);
			currentIndex = tsr.insertPoint;
			if( DEBUG )
				System.out.println("Search loop "+currentIndex+" "+currentPage);
			if (tsr.atKey) // Key found
				break;
			// dont back it up at 0
			if( currentIndex > 0 )
				--currentIndex; // go left
			if( currentPage.mIsLeafNode ) {
				return new TreeSearchResult(currentIndex, false);
			}	
			BTreeKeyPage targetPage = currentPage.getPage(getIO(), currentIndex);
			if ( targetPage == null) {
				return new TreeSearchResult(currentIndex, false);
			}
			/*
			 * Internal routine to push stack. If we are at MAXSTACK just return
			 * set keyPageStack[stackDepth] to currentPage
			 * set indexStack[stackDepth] to currentIndex
			 * Sets stackDepth up by 1
			 * return true if stackDepth not at MAXSTACK, false otherwise
			 */
			if (!push()) {
				return new TreeSearchResult(currentIndex, false);
			}
			sdbio.deallocOutstanding(currentPage.pageId);
			currentPage = targetPage;
		} while (true);
		return new TreeSearchResult(currentIndex, true);
	}

	/**
	* Seeks to leftmost key in current subtree
	*/
	private boolean seekLeftTree() throws IOException {
		if( DEBUG ) {
			System.out.println("BTreeMain.seekLeftTree page:"+currentPage+" index "+currentIndex);
		}
		while (push()) {
			currentPage = currentPage.getPage(getIO(), currentIndex);
			if( DEBUG )
				System.out.println("BTreeMain.seekLeftTree PUSH using "+currentPage+" index "+currentIndex);
			currentIndex = 0;
			if (currentPage.getPage(getIO(), currentIndex) == null)
				return (true);
		}
		return (false);
	}

	/**
	* Seeks to rightmost key in current subtree
	*/
	private boolean seekRightTree() throws IOException {
		while (push()) {
			currentPage = currentPage.getPage(getIO(), currentIndex);
			currentIndex = currentPage.numKeys;
			if (currentPage.getPage(getIO(), currentIndex) == null) {
				currentIndex--;
				return (true);
			}
		}
		return (false);
	}

	/** 
	 * Internal routine to push stack. If we are at MAXSTACK just return
	 * set keyPageStack[stackDepth] to currentPage
	 * set indexStack[stackDepth] to currentIndex
	 * Sets stackDepth up by 1
	 * @return true if stackDepth not at MAXSTACK, false otherwise
	 */
	private boolean push() {
		if (stackDepth == MAXSTACK)
			return (false);
		keyPageStack[stackDepth] = currentPage;
		indexStack[stackDepth++] = currentIndex;
		return (true);
	}

	/** 
	 * Internal routine to pop stack. 
	 * sets stackDepth - 1, 
	 * currentPage to keyPageStack[stackDepth] and 
	 * currentIndex to indexStack[stackDepth]
	 * @return false if stackDepth reaches 0, true otherwise
	 * 
	 */
	private boolean pop() {
		if (stackDepth == 0)
			return (false);
		stackDepth--;
		currentPage = keyPageStack[stackDepth];
		currentIndex = indexStack[stackDepth];
		return (true);
	}

	/**
	* Internal routine to clear references on stack
	*/
	private void clearStack() {
		for (int i = 0; i < MAXSTACK; i++)
			keyPageStack[i] = null;
		stackDepth = 0;
	}

	public Object getCurrentObject() {
		return currentObject;
	}

	public void setCurrentObject(Object currentObject) {
		this.currentObject = currentObject;
	}

	@SuppressWarnings("rawtypes")
	public Comparable getCurrentKey() {
		return currentKey;
	}

	@SuppressWarnings("rawtypes")
	public void setCurrentKey(Comparable currentKey) {
		this.currentKey = currentKey;
	}

	public ObjectDBIO getIO() {
		return sdbio;
	}

	public void setIO(ObjectDBIO sdbio) {
		this.sdbio = sdbio;
	}

	public BTreeKeyPage getRoot() {
		return root;
	}
	/**
	 * Set the root node value, return it as fluid pattern
	 * @param root
	 * @return
	 */
	public BTreeKeyPage setRoot(BTreeKeyPage root) {
		this.root = root;
		return root;
	}
	
    // Inorder walk over the tree.
    void printBTree(BTreeKeyPage node) throws IOException {
            if (node != null) {
                    if (node.mIsLeafNode) {
                            for (int i = 0; i < node.numKeys; i++) {
                                    System.out.print(node.keyArray[i] + ", ");
                            }
                    } else {
                            int i;
                            for (i = 0; i < node.numKeys; i++) {
                                    printBTree(node.getPage(sdbio,i));
                                    System.out.print( node.keyArray[i] + ", ");
                            }
                            System.out.println();
                            printBTree(node.getPage(sdbio,i));
                    }                       
            }
    }
    
    
    void validate() throws Exception {
            ArrayList<Comparable> array = getKeys(getRoot());
            for (int i = 0; i < array.size() - 1; i++) {            
                    if (array.get(i).compareTo(array.get(i + 1)) >= 0) {
                            throw new Exception("B-Tree invalid: " + array.get(i)  + " greater than " + array.get(i + 1));
                    }
        }           
    }
    
    // Inorder walk over the tree.
    ArrayList<Comparable> getKeys(BTreeKeyPage node) throws IOException {
            ArrayList<Comparable> array = new ArrayList<Comparable>();
            if (node != null) {
                    if (node.mIsLeafNode) {
                            for (int i = 0; i < node.numKeys; i++) {
                                    array.add(node.keyArray[i]);
                            }
                    } else {
                            int i;
                            for (i = 0; i < node.numKeys; i++) {
                                    array.addAll(getKeys(node.getPage(sdbio,i)));
                                    array.add(node.keyArray[i]);
                            }
                            array.addAll(getKeys(node.getPage(sdbio,i)));
                    }                       
            }
            return array;
    }
    /**
	* Add key object to tree. If we locate it return that position, else write it
	* @param newKey The new key to add
	* @return 0 if ok, <> 0 if error
	* @exception IOException If write fails 
	*/
	@SuppressWarnings("rawtypes")
	public synchronized int addx(Comparable newKey) throws IOException {
		int i, j, k;
		Comparable saveKey = null;
		//Object saveObject = null;
		BTreeKeyPage savePagePointer = null;
		BTreeKeyPage leftPagePtr = null;
		BTreeKeyPage rightPagePtr = null;

		// If tree is empty, make a root
		//if (getNumKeys() == 0) {
		//	currentPage = getRoot();
		//	currentIndex = 0;
		//	currentPage.insert(newKey, null, currentIndex);
		//	++numKeys;
		//	return 0;
		//}
		// Determine whether data is present
		// we need a search call regardless to set up our target insertion point for later should we decide to do so
		if( search(newKey).atKey ) {
			// found it,
			if( DEBUG ) System.out.println("--Add found key "+newKey+" in current page "+currentPage);
			return 0;
		}
		if( DEBUG ) {
			System.out.println("Adding key "+newKey);
		}
		do {
			// About to insert key. See if the page is going to overflow
			// If the current index is at end, dont insert, proceed to split
			if ((i = currentPage.numKeys) == BTreeKeyPage.MAXKEYS) {
				if( DEBUG )
					System.out.println("MAX KEYS REACHED for:"+currentPage);
				// Save rightmost key/data/pointer
				--i;
				if (currentIndex == BTreeKeyPage.MAXKEYS) {
					saveKey = newKey;
					//saveObject = null;
					savePagePointer = rightPagePtr;
				} else {
					saveKey = currentPage.keyArray[i];
					//saveObject = currentPage.getDataFromArray(getIO(), i);
					savePagePointer = currentPage.getPage(sdbio, i + 1);
					//currentIndex = currentPage.insert(newKey, null, currentIndex, false);
					currentPage.putPageToArray(leftPagePtr, currentIndex);
					currentPage.putPageToArray(rightPagePtr, currentIndex + 1);
				}

				// Split has occurred. Pull the middle key out
				i = T;
				newKey = currentPage.keyArray[i];
				currentPage.putKeyToArray(null, i);
				//newObject = currentPage.getDataFromArray(getIO(), i);
				currentPage.putDataToArray(null, i);
				leftPagePtr = currentPage;
				// Create new page for the right half of the old page and move right half in
				rightPagePtr = BTreeKeyPage.getPageFromPool(sdbio);
				rightPagePtr.mIsLeafNode = currentPage.mIsLeafNode;
				// move the right half of the old key into new node, the new right
				k = 0;
				for (j = i + 1; j < BTreeKeyPage.MAXKEYS; j++) {
					moveKeyData(currentPage, j, rightPagePtr, k, true);
					// if not leaf move child nodes
					if( !currentPage.mIsLeafNode) {
						moveChildData(currentPage, j, rightPagePtr, k, true);
					}
					++k;
				}
				rightPagePtr.putPageToArray(currentPage.getPage(getIO(), BTreeKeyPage.MAXKEYS),k);
				rightPagePtr.keyArray[k] = saveKey;
				rightPagePtr.putDataToArray(null, k);
				rightPagePtr.putPageToArray(savePagePointer, k + 1);
				rightPagePtr.numKeys = BTreeKeyPage.MAXKEYS - i;

				leftPagePtr.numKeys = i;
				leftPagePtr.setUpdated(true);
				leftPagePtr.putPage(sdbio);
				rightPagePtr.putPage(sdbio);
			} else {
				// Insert key/object at current location
				//currentIndex = currentPage.insert(newKey, null, currentIndex, false);
				currentPage.putPageToArray(leftPagePtr, currentIndex);
				currentPage.putPageToArray(rightPagePtr, currentIndex + 1);
				currentPage.putPage(sdbio);
				return 0;
			}
			// Try to pop. If we can't pop, make a new root
		} while (pop());
		splitNodeBalance(getRoot());
		return 0;
	}


}
