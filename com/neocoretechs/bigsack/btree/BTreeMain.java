package com.neocoretechs.bigsack.btree;
import java.io.IOException;
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
* The split operation transforms a full node with MAXKEYS elements into 3 nodes with MAXKEYS/3 elements each
* and inserts the new node in the proper spot in a leaf. In this way the number of elements remains somewhat constant per node.
* The elements comprising the middle of the split node remain in the original node.
* 
* Example (MAXKEYS = 4):
* 1.  K =   | 1 | 2 | 3 | 4 |
*             / 
*          | 0 |
* 
* 2.  Add key 5
*   
* 3.  k =       | 2 | 3 |
*                 /   \
*           | 1 |       | 4 | 5 |
*             /
*          | 0 |
* @author Groff
*/
public final class BTreeMain {
	private static boolean DEBUG = false; // General debug
	private static boolean DEBUGCURRENT = true; // alternate debug level to view current page assignment of BTreeKeyPage
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
	boolean atKey;
	private CyclicBarrier nodeSplitSynch = new CyclicBarrier(3);
	private NodeSplitThread leftNodeSplitThread, rightNodeSplitThread;
	private ObjectDBIO sdbio;

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
			if( DEBUG ) System.out.println("gotoNextKey returned: "+i);
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
		atKey = true;
		if (search(targetKey).atKey) {
			setCurrentKey();
			System.out.println("SeekKey SUCCESS and state is currentIndex:"+currentIndex+" targKey:"+targetKey+" "+currentPage);
			return true;
		} else {
			System.out.println("SeekKey missed and state is currentIndex:"+currentIndex+" targKey:"+targetKey+" "+currentPage);
			// See if the page is on overflow
			if ((currentPage.numKeys) == BTreeKeyPage.MAXKEYS) {
				do {
					int index = currentPage.search(targetKey);
					if (index < BTreeKeyPage.MAXKEYS) {
						setCurrentKey();
						return true;
					}
				// pop sets stackDepth - 1, 
				// currentPage to keyPageStack[stackDepth] and 
				// currentIndex to indexStack[stackDepth]
				// returns false if stackDepth reaches 0, true otherwise
					System.out.println("seekKey popping currentIndex:"+currentIndex+" targKey:"+targetKey+" "+currentPage);
				} while(pop());
				System.out.println("Popped to the top but dropped "+targetKey+" "+currentPage);
			}
			System.out.println("Search missed and state is currentIndex:"+currentIndex+" targKey:"+targetKey+" "+currentPage);
			return false;
		}
	}
	/**
	 * Add an object and/or key to the deep store. Traverse the BTree for the insertion point and insert.
	 * @param key
	 * @param object
	 * @throws IOException
	 */
	public int add(Comparable key, Object object) throws IOException {
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
    private TreeSearchResult update(Comparable key, Object object) throws IOException {
    	int i = 0;
        while (true) {
                i = 0;
                while (i < currentPage.numKeys && key.compareTo(currentPage.keyArray[i]) > 0) {
                        i++;
                }
                if (i < currentPage.numKeys && key.compareTo(currentPage.keyArray[i]) == 0) {
                	if( object != null && OVERWRITE) {
            			currentPage.deleteData(sdbio, i);
                        currentPage.putDataToArray(object,i);
                	}
                	return new TreeSearchResult(i, true);
                }
                if (currentPage.mIsLeafNode) {
                	if( DEBUG )
                		System.out.println("BTreeMain.update set to insert at :"+i+" for "+currentPage);
                        return new TreeSearchResult(i, false);
                } else {
                	BTreeKeyPage targetPage  = currentPage.getPage(sdbio, i);// get the page at the index of the given page
                	if( targetPage == null )
                		break;
                	sdbio.deallocOutstanding(currentPage.pageId);
                	currentPage = targetPage;
                }
        }
        return new TreeSearchResult(i, false);
    }

    /**
     * Split a node of the B-Tree into 3 nodes that both contain MAXKEYS/3 elements approximately
     * with the balance filling the parent. insert the node in appropriate leaf.
     * Adjust all child pointers down to leaves and link new nodes to old parent.
     * This method will only be called if node is full; node is the i-th child of parentNode.
     * The method forms 2 requests that are spun off to 2 node split threads. The logic in this method
     * synchronizes via a cyclicbarrier and monitor to manipulate the parent node properly after
     * the worker threads are done. One of the the threads will have inserted the key to one of the
     * new children upon completion. The request hold the position.
     * @param parentNode The node we are splitting, becomes parent of both nodes
     * @param i The index of split in old node
     * @throws IOException 
     */
    void splitChildNode(BTreeKeyPage parentNode, int i, Comparable key, Object object) throws IOException { 
        if( DEBUG )
        	System.out.println("BTreeMain.splitChildNode  parent:"+parentNode+" node insert:"+i+" key:"+key+" object:"+object);
        LeftNodeSplitRequest lnsr = new LeftNodeSplitRequest(sdbio, parentNode, key, object, i);
        RightNodeSplitRequest rnsr = new RightNodeSplitRequest(sdbio, parentNode, key, object, i);
        try {
        	leftNodeSplitThread.queueRequest(lnsr);
        	rightNodeSplitThread.queueRequest(rnsr);
        	// await the other nodes
			nodeSplitSynch.await();
			// once we synch, we have 2 nodes, one each in the requests
        	// if neither one has our insert, we need to put it in the old root.
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
					System.out.println("BTreeMain.splitChildNode nullKeys:"+nullKeys);
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
					System.out.println("BTreeMain.splitChildNode goodKeys:"+goodKeys+" parent:"+parentNode);
				}
				if( nullKeys > 0 ) {
					for(int j = nullKeys; j < goodKeys+nullKeys; j++) {
						if( DEBUG ) {
							System.out.println("BTreeMain.splitChildNode moveKeyData:"+j+" nulls:"+nullKeys);
						}
						moveKeyData(parentNode, j, parentNode, j-nullKeys, true);
						if( DEBUG ) {
							System.out.println("BTreeMain.splitChildNode moveChildData:"+j+" nulls:"+nullKeys);
						}
						moveChildData(parentNode, j, parentNode, j-nullKeys, true);
					}
					if( DEBUG ) {
						System.out.println("BTreeMain.splitChildNode moveKeyData parent source:"+String.valueOf(nullKeys+goodKeys+1)+" to:"+goodKeys);
					}
					// get the node at position+1, the rightmost key pointer
					moveChildData(parentNode, nullKeys+goodKeys, parentNode, goodKeys, true);
				}
				parentNode.numKeys = goodKeys;
				if( !lnsr.wasNodeInserted() && !rnsr.wasNodeInserted() ) {
					// No insertion during split, must need insertion here
					if( DEBUG )
						System.out.println("BTreeMain.splitChildNode moving to insert key "+key+" TO PARENT:"+parentNode+" At insert position:"+i);
					parentNode.insert(key, object, i-nullKeys, false);
					if( DEBUG )
						System.out.println("BTreeMain.splitChildNode KEY INSERTED: "+key+" TO PARENT:"+parentNode+" At insert position:"+i);
				}
				parentNode.setUpdated(true);
				if( DEBUG ) {
					System.out.println("BTreeMain.splitChildNode moving to putPage:"+currentPage);
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
     * Insert an element into a B-Tree. (The element will ultimately be inserted into a leaf node).
     * @param node
     * @param insertionPoint
     * @param key
     * @param object
     * @throws IOException
     */
    void insertIntoNonFullNode(BTreeKeyPage node, int insertionPoint, Comparable key, Object object) throws IOException {
    	node.insert(key,  object,  insertionPoint, false);
    	node.putPage(sdbio);
		//sdbio.deallocOutstanding(node.pageId);
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
	

	public synchronized int add(Comparable newKey) throws IOException {
		return add(newKey, null);
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
					atKey = false;
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
		atKey = false;
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
		atKey = false;
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
		if (atKey)
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
				atKey = false;
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
		atKey = false;
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
				atKey = false;
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
		atKey = false;
		return (EOF);
	}

	/**
	* Set the current object and key based on value of currentPage
	* and currentIndex
	*/
	public synchronized void setCurrent() throws IOException {
		if( DEBUG || DEBUGCURRENT)
			System.out.println("BTreeMain.setCurrent page:"+currentPage+" index:"+currentIndex);
		atKey = true;
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
		atKey = true;
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
			currentIndex = currentPage.search(targetKey);
			if (currentIndex >= 0) // Key found
				break;
			currentIndex = (-currentIndex);
			if( currentPage.mIsLeafNode ) {
				atKey = false;
				return new TreeSearchResult(currentIndex, false);
			}
				
			BTreeKeyPage targetPage = currentPage.getPage(getIO(), currentIndex);
			if ( targetPage == null) {
				atKey = false;
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
				atKey = false;
				return new TreeSearchResult(currentIndex, false);
			}
			sdbio.deallocOutstanding(currentPage.pageId);
			currentPage = targetPage;
		} while (true);
		atKey = true;
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

}
