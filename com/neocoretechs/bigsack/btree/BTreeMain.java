package com.neocoretechs.bigsack.btree;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Stack;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

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
	private static boolean DEBUGDELETE = false;
	private static boolean TEST = false; // Do a table scan and key count at startup
	private static boolean ALERT = true; // Info level messages
	private static boolean OVERWRITE = true; // flag to determine whether value data is overwritten for a key or its ignored
	private static final boolean DEBUGOVERWRITE = false; // notify of overwrite of value for key
	static int EOF = 2;
	static int NOTFOUND = 3;
	static int ALREADYEXISTS = 4;
	static int TREEERROR = 6;

	private BTreeKeyPage root;
	
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
	 * TODO: Alternate more efficient implementation that counts keys on pages
	 * This method scans all keys, thus verifying the structure.
	 * @throws IOException
	 */
	public synchronized long count() throws IOException {
		boolean found = rewind();
		//System.out.println(found);
		long numKeys = 0;
		long tim = System.currentTimeMillis();
		if( found && currentPage != null ) {
			++numKeys;
			while (gotoNextKey() == 0) {
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
	/**
	 * Determines if tree is empty by examining the root for the presence of any keys
	 * @return
	 */
	public synchronized boolean isEmpty() {
		return (getRoot().getNumKeys() == 0);
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
		rewind();
		if( currentPage != null ) {
			setCurrent();
			// are we looking for first element?
			if(currentObject.equals(targetObject))
				return currentObject;
			while (gotoNextKey() == 0) {
				//System.out.println(currentObject);
				if(currentObject.equals(targetObject))
					return currentObject;
			}
		}
		// deallocate outstanding blocks in all tablespaces
		sdbio.deallocOutstanding();
		clearStack();
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
        // If we are not at a key, if key was not present on search, then we insert
        // and possibly split
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
        // If 'update' method returned an atKey true, it has performed the replacement of data element for key
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
                    				sourcePage.deleteData(i);
                					sourcePage.putDataToArray(object,i);
                					if( DEBUG || DEBUGOVERWRITE )
                     					System.out.println("OVERWRITE value "+object+" for key "+key+" index["+i+"] page:"+sourcePage);
                				} else {
                					if(ALERT)
                						System.out.println("OVERWRITE flag set to false, so attempt to update existing value is ignored for key "+key);
                				}
                			}
                			// wont put the data if here, object = keyValue, both not null
                		} else {
                			sourcePage.putDataToArray(object,i);
                		}
                		// If we had a value already, and it wasnt null and not equal to previous we put the new data
                		// If it was null or the value equal to previous we bypassed and are here
                	} else {
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
                node.putDataToArray(object, i);
                
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
     * Optionally null the source at sourceIndex.
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
	* The BTreeKeyPage contains most of the functionality and the following methods are unique to the deletion process:
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
	@SuppressWarnings("rawtypes")
	public synchronized int delete(Comparable newKey) throws IOException {
		if( DEBUG || DEBUGDELETE ) System.out.println("--ENTERING DELETE FOR "+newKey);
		// Call the remove function starting at the root node
		// make sure it has some keys
		BTreeKeyPage root = getRoot();
		if( root.getNumKeys() == 0) {
			if( DEBUG || DEBUGDELETE ) System.out.println("Root Keypage has no keys when attempting DELETE FOR "+newKey);
			return NOTFOUND;
		}
	    root.remove(newKey);
	    // If the root node now has 0 keys, make its first child as the new root
	    // if it has a child
	    if (root.getNumKeys() == 0 && !root.getmIsLeafNode()) {
			if( DEBUG || DEBUGDELETE ) System.out.println("Root non leaf Keypage has no keys when attempting DELETE FOR "+newKey);
			BTreeKeyPage newRoot = root.getPage(0);  // get child of root keypage object
			root.replacePage(newRoot); // read the childs data into old root, replacing it
	        if( DEBUG || DEBUGDELETE ) System.out.println("Root RESET with first child "+getRoot());
	    }
		if( DEBUG || DEBUGDELETE ) System.out.println("BTreeMain.delete Just deleted "+newKey);
		return 0;
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
		left.putDataToArray(parent.getData(index), left.getNumKeys());

		// Append the contents of the right
		// page onto the left key page
		j = left.getNumKeys() + 1;
		for (i = 0; i < right.getNumKeys(); i++) {
			left.putPageToArray(right.getPage(i), j);
			left.putDataToArray(right.getData(i), j);
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
	public synchronized boolean rewind() throws IOException {
		currentPage = getRoot();
		currentIndex = 0;
		currentChild = 0;
		clearStack();
		return seekLeftTree();
		//if( DEBUG )
		//	System.out.println("BTreeMain.rewind positioned at "+currentPage+" "+currentIndex+" "+currentChild);
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
                	// we are at leaf, we pop or return having not found
                	if( DEBUG || DEBUGSEARCH)
                		System.out.println("BTreeMain.repositionStack set to return index :"+i+" for leaf "+sourcePage);
                	// If our key has run off the end of page or will do so, pop to subtree right in parent, we are at leaf still
                	if( i >= sourcePage.getNumKeys() || sourcePage.getKey(i).compareTo(key) < 0 ) {
                		currentPage = sourcePage;
                		int v = popUntilValid(true);
                		//return new TreeSearchResult(currentPage, currentIndex, true);
                		return new TreeSearchResult(currentPage, currentIndex, (v == 0));
                	}
                	// didnt run off end or key on page all > key and we are at leaf, key must not exist
                	//return new TreeSearchResult(sourcePage, i, true);
             		return new TreeSearchResult(sourcePage, i, false);
                } else {
                	// non leaf
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
			setCurrent();
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
	* Set the current object and key based on value of currentPage.
	* and currentIndex
	*/
	public synchronized void setCurrent() throws IOException {
		setCurrentKey(currentPage.getKey(currentIndex));
		setCurrentObject(currentPage.getData(currentIndex));
		if( DEBUG || DEBUGCURRENT)
			System.out.println("BTreeMain.setCurrent page:["+ currentIndex +"] "+getCurrentKey()+" "+getCurrentObject());
	}
	/**
	* Set the current object and key based on value of currentPage.
	* and currentIndex
	*/
	public synchronized Comparable setCurrentKey() throws IOException {
		setCurrentKey(currentPage.getKey(currentIndex));
		if( DEBUG || DEBUGCURRENT)
			//System.out.println("BTreeMain.setCurrentKey page:"+currentPage+" index:"+currentIndex);
			System.out.println("BTreeMain.setCurrentKey page:["+ currentIndex +"] "+getCurrentKey()+" "+getCurrentObject());
		return getCurrentKey();
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
	private synchronized boolean seekLeftTree() throws IOException {
		boolean foundPage = false;
		if( DEBUG || DEBUGSEARCH ) {
			System.out.println("BTreeMain.seekLeftTree page:"+currentPage+" index:"+currentIndex+" child:"+currentChild);
		}
		BTreeKeyPage tPage = currentPage.getPage(currentChild);
		while (tPage != null) {
			foundPage = true;
			// Push the 'old' value of currentPage, currentIndex etc as a TraversalStackElement since we are
			// intent on descending. After push and verification that tPage is not null, set page to tPage.
			push();
			if( DEBUG || DEBUGSEARCH )
				System.out.println("BTreeMain.seekLeftTree PUSH using "+currentPage+" index:"+currentIndex+" child:"+currentChild+" new entry:"+tPage);
			currentIndex = 0;
			currentChild = 0;
			currentPage = tPage;
			setCurrent();
			tPage = currentPage.getPage(currentChild);
		}
		return foundPage;
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
			setCurrent();
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
