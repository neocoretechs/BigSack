package com.neocoretechs.bigsack.btree;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Stack;
import java.util.stream.Stream;

import com.neocoretechs.bigsack.btree.BTreeNavigator.StackInfo;
import com.neocoretechs.bigsack.io.Optr;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.iterator.Entry;
import com.neocoretechs.bigsack.iterator.EntrySetIterator;
import com.neocoretechs.bigsack.iterator.KeySetIterator;
import com.neocoretechs.bigsack.keyvaluepages.KeyPageInterface;
import com.neocoretechs.bigsack.keyvaluepages.KeySearchResult;
import com.neocoretechs.bigsack.keyvaluepages.KeyValue;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
import com.neocoretechs.bigsack.keyvaluepages.NodeInterface;
import com.neocoretechs.bigsack.keyvaluepages.RootKeyPageInterface;
import com.neocoretechs.bigsack.keyvaluepages.TraversalStackElement;
import com.neocoretechs.bigsack.session.BigSackAdapter;
import com.neocoretechs.bigsack.session.BufferedTreeSet;

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
 * <dd>Merge 4 with parent if parent is not full.
 *
* @author Groff Copyright (C) NeoCoreTechs 2015,2017,2021
*/
public final class BTreeMain implements KeyValueMainInterface {
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

	private KeyPageInterface root;
	BTreeNavigator bTreeNavigator;
	long numKeys = 0;
	
	GlobalDBIO sdbio;

	public BTreeMain(GlobalDBIO globalDBIO) throws IOException {
		this.sdbio = globalDBIO;
		this.bTreeNavigator = new BTreeNavigator<Comparable, Object>(this);
		if(DEBUG)
			System.out.printf("%s ctor %s%n",this.getClass().getName(), this.bTreeNavigator);
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
		if(this.root.getNumKeys() == 0)
			((BTNode)(((BTreeKeyPage)this.root).bTNode)).setmIsLeaf(true);
		if( DEBUG )
			System.out.printf("%s Root KeyPageInterface: %s%n",this.getClass().getName(),root);	
		return this.root;
	}
	
	public void test() throws IOException {
		if( TEST ) {
			System.out.printf("MAXKEYS=%d%n", BTreeKeyPage.MAXKEYS);
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
		numKeys = 0;
		long tim = System.currentTimeMillis();
		countBTree((BTreeKeyPage) root);
		if( DEBUG || DEBUGCOUNT )
			System.out.println("Count for "+sdbio.getDBName()+" returned "+numKeys+" keys in "+(System.currentTimeMillis()-tim)+" ms.");
		// deallocate outstanding blocks in all tablespaces
		sdbio.deallocOutstanding();
		return numKeys;
	}
	
	private void countBTree(BTreeKeyPage node) throws IOException {
        if(node != null) {
            if (((BTreeKeyPage) node).getmIsLeafNode()) {
            	numKeys += node.getNumKeys();
            } else {
            	//System.out.print("NonLeaf node:"+node.getNumKeys());
                    int i;
                    for (i = 0; i < node.getNumKeys(); i++) {
                    	BTreeKeyPage btk = (BTreeKeyPage) node.getPage(i);
                        countBTree(btk);
                        //System.out.print(" Page:"+GlobalDBIO.valueOf(node.getPageId())+" INDEX:"+i+" node:"+ node.getKey(i) + ", ");
                    }
                    numKeys += node.getNumKeys();
                    countBTree((BTreeKeyPage) node.getPage(i));
            }                       
        }
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
	* @return The BigSack iterator {@link Entry} if found. null otherwise.
	* @exception IOException if read failure
	*/
	@Override
	@SuppressWarnings("rawtypes")
	public synchronized Object seekObject(Object targetObject) throws IOException {	
		Iterator it = new EntrySetIterator(this);
		Entry o = null;
		while(it.hasNext()) {
			o = (Entry) it.next();
			if(o.getValue().equals(targetObject))
				return o;
		}
		// deallocate outstanding blocks in all tablespaces
		sdbio.deallocOutstanding();
		return null;
	}
	/**
	* Seek the key, if we dont find it, leave the tree at it position closest greater than element.
	* If we do find it return true in atKey of result and leave at found key.
	* Calls locate, which calls clearStack, repositionStack and setCurrent.
	* @param targetKey The Comparable key to seek
	* @return search result with key data
	* @exception IOException if read failure
	*/
	@Override
	@SuppressWarnings("rawtypes")
	public synchronized KeySearchResult seekKey(Comparable targetKey, Stack stack) throws IOException {
		KeySearchResult tsr = locate(targetKey, stack);
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
		if(DEBUG)
			System.out.printf("%s insert exit key=%s value=%s result=%d%n", this.getClass().getName(), key, value,result);
		return result;
	}
    
    /**
     * Perform a search using {@link BTreeNavigator}, populating the stack as we traverse the tree levels.
     * The stack is needed for iterators and other operations. If we need a straight search, use 'search'
     * with does away with minor overhead of stack population, and preserves the stack.
     * @param key
     * @return populated {@link KeySearchResult}
     * @throws IOException
     */
    @Override
	public synchronized KeySearchResult locate(Comparable key, Stack stack) throws IOException {
        KeySearchResult ksr = bTreeNavigator.search(key, true); // uses stack.add
        for(Object o: bTreeNavigator.getStack().toArray()) {
        	StackInfo si = (StackInfo)o;
        	stack.push( new TraversalStackElement(si.mParent.getPage(), si.mNodeIdx, 0) ); 
        }
        // if it didnt find it exactly, remove duplicate element from stack
        if(!ksr.atKey && !stack.empty())
        	stack.pop();
        return ksr;
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
	* doesnt get too small during deletion.<p/> 
	* Node deletion is handled by consideration of several cases designed to
	* never leave an empty leaf, and by inclusion, never leave a non-leaf with an invalid child pointer.<p/>
	* <dd>Case 1: On delete, If we delete from a child, shift the remaining elements left if not empty<p/>
	* If a child empties, we never leave null links, so we split a node off from the end or
	* beginning if that position in the parent is the predecessor link, otherwise we split
	* the parent at the link to the now empty leaf, thus creating 2 valid links to 2 new leaves.<p/>
	* Again, if that parent can be merged with grandparent, do so.<p/>
	* <dd>Case 2: For a non-leaf that deletes from a leaf and does NOT empty it, we can rotate the right link far
	* left child or left link far right child to the former position in the parent, using the next inorder key.<p/>
	* Special case here is when a parent has 2 leaves with one key, and itself has one key, in that case bring them both up into parent
	* and remove 2 leaves and designate parent a leaf. We use our checkDegenerateSingletons method.
	* <dd>Case 3: Finally for 2 internal nodes, a non-leaf parent deleting from a non-leaf child, we have to take the right node
	* and follow it to the left most leaf, take the first key, and rotate it into the slot. That is,
	* the least valued node immediately to the right. that is, the next inorder traversal key.<p/>
	* If case 3 empties a leaf, handle it with case using the parent of that leftmost leaf. For any operation that
	* descends into a subtree to extract the next inorder key, recursively perform the checks until we come to the
	* original root of our operation.<p/>
	* Since most of the keys in a B-tree are in the leaves, deletion operations most often delete keys from leaves. 
	* The recursive delete procedure then acts in one downward pass through the tree, without having to back up. 
	* When deleting a key in an internal node, however, the procedure makes a downward pass through the tree but may have to 
	* return to the node from which the key was deleted to replace the key with its predecessor or successor.
	* The {@link BTreeNavigator} contains most of the functionality.
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
	 * @param rewound TraversalStackElement to be populated with bottom element
	 * @param stack Stack to be populated with traversal
	 * @return The KeyValue at beginning of tree
	 * @exception IOException If read fails
	 */
	@Override
	public synchronized KeyValue rewind(TraversalStackElement rewound, Stack stack) throws IOException {
		stack.clear();
		TraversalStackElement trewound = seekLeftTree(new TraversalStackElement(root, 0, 0), stack);
		if(trewound == null)
			return null;
		rewound.child = trewound.child;
		rewound.index = trewound.index;
		rewound.keyPage = trewound.keyPage;
		return ((BTreeKeyPage)rewound.keyPage).getKeyValueArray(0);
		//if( DEBUG )
		//	System.out.println("BTreeMain.rewind positioned at "+currentPage+" "+currentIndex+" "+currentChild);
	}


	/**
	 * Set current position to end of tree.Sets up stack with pages and indexes
	 * such that traversal can take place. Remember to clear stack after these operations. 
	 * @param rewound TraversalStackElement to be populated with bottom element
	 * @param stack Stack to be populated with traversal
	 * @return The KeyValue at end of tree
	 * @exception IOException If read fails
	 */
	@Override
	public synchronized KeyValue toEnd(TraversalStackElement rewound, Stack stack) throws IOException {
		stack.clear();
		TraversalStackElement trewound = seekRightTree(new TraversalStackElement(root, root.getNumKeys(), root.getNumKeys()), stack);
		if(rewound == null)
			return null;
		rewound.child = trewound.child;
		rewound.index = trewound.index;
		rewound.keyPage = trewound.keyPage;
		return ((KeyPageInterface)rewound.keyPage).getKeyValueArray(rewound.index);
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
	* @param tse TraversalStackElement that carries the pointers to advance
	* @param stack Stack to be extracted with traversal
	* @return Element with indexes advanced
	* @exception IOException If read fails
	*/
	@Override
	public synchronized TraversalStackElement gotoNextKey(TraversalStackElement tse, Stack stack) throws IOException {
		if( DEBUG || DEBUGSEARCH ) {
			System.out.println("BTreeMain.gotoNextKey "/*page:"+currentPage+*/+" index "+tse);
			printStack(stack);
		}
		int currentIndex = tse.index+1;
		// If we are at a key, then advance the index
		if(((BTNode)((BTreeKeyPage)tse.keyPage).bTNode).getIsLeaf() ) {
			if( currentIndex < tse.keyPage.getNumKeys()) {
				tse.index = currentIndex; // use advanced index
				tse.child = currentIndex; // left
				return tse;
			}
			if(stack.isEmpty())
				return null; // root was leaf, and we are done
			TraversalStackElement tsex = (TraversalStackElement) stack.pop(); // go to zero of parent
			if(tsex.index == tsex.keyPage.getNumKeys()) // dont pop to nonexistent key for final right page pointer
				return gotoNextKey(tsex, stack);
			return tsex;
		}
		// not leaf, and we got element 0 of leaf with above pop, work the leaf until end then go right, left
		if(currentIndex <= tse.keyPage.getNumKeys()) { // increment, go left or right at end
			tse.index = currentIndex; // use advanced index
			tse.child = currentIndex; // left
			return seekLeftTree(tse,stack);
		}
		// if its root increment and go left
		if(stack.isEmpty()) {
				return null; // done at far right of root
		}
		return (TraversalStackElement) stack.pop();
	}

	/**
	* Go to location of previous key in tree
	* @param tse TraversalStackElement that carries the pointers to advance
	* @param stack Stack to be extracted with traversal
	* @return Element with indexes decremented
	* @exception IOException If read fails
	*/
	@Override
	public synchronized TraversalStackElement gotoPrevKey(TraversalStackElement tse, Stack stack) throws IOException {
		if( DEBUG || DEBUGSEARCH ) {
			System.out.println("BTreeMain.gotoPrevKey "/*page:"+currentPage+*/+" index "+tse);
		}
		int currentIndex = tse.index-1;
		// If we are at a key, then advance the index
		if(((BTNode)((BTreeKeyPage)tse.keyPage).bTNode).getIsLeaf() ) {
			if( currentIndex >= 0) {
				tse.index = currentIndex; // use advanced index
				tse.child = currentIndex; // right
				return tse;
			}
			if(stack.isEmpty())
				return null; // root was leaf, and we are done
			TraversalStackElement tsex = (TraversalStackElement) stack.pop(); // go to numkeys of parent
			if(tsex.index == tsex.keyPage.getNumKeys()) // dont pop to nonexistent key for final right page pointer
				return gotoPrevKey(tsex, stack);
			return tsex;
		}
		// not leaf, and we got element 0 of leaf with above pop, work the leaf until end then go right, left
		if(currentIndex >= 0) { // decrement, go right
			tse.index = currentIndex; // use advanced index
			tse.child = currentIndex; // left
			return seekRightTree(tse, stack);
		}
		// if its root decrement and go right
		if(stack.isEmpty()) {
				return null; // done at far left of root
		}
		return (TraversalStackElement) stack.pop();
	}

	/**
	 * Auxiliary method to advance page by page, vs key by key
	 * @param tse
	 * @param stack
	 * @return
	 * @throws IOException
	 */
	public synchronized TraversalStackElement gotoNextPage(TraversalStackElement tse, Stack stack) throws IOException {
		if( DEBUG || DEBUGSEARCH ) {
			System.out.println("BTreeMain.gotoNextPage "/*page:"+currentPage+*/+" index "+tse);
			printStack(stack);
		}
		int currentIndex = tse.index+1;
		// If we are at a key, then advance the index
		if(((BTNode)((BTreeKeyPage)tse.keyPage).bTNode).getIsLeaf() ) {
			if( currentIndex < tse.keyPage.getNumKeys()) {
				tse.index = tse.keyPage.getNumKeys();
				tse.child = tse.keyPage.getNumKeys();
				return tse;
			}
			if(stack.isEmpty())
				return null; // root was leaf, and we are done
			TraversalStackElement tsex = (TraversalStackElement) stack.pop(); // go to zero of parent
			if(tsex.index == tsex.keyPage.getNumKeys()) // dont pop to nonexistent key for final right page pointer
				return gotoNextKey(tsex, stack);
			return tsex;
		}
		// not leaf, and we got element 0 of leaf with above pop, work the leaf until end then go right, left
		if(currentIndex <= tse.keyPage.getNumKeys()) { // increment, go left or right at end
			tse.index = currentIndex; // use advanced index
			tse.child = currentIndex; // left
			return seekLeftTree(tse, stack);
		}
		// if its root increment and go left
		if(stack.isEmpty()) {
				return null; // done at far right of root
		}
		return (TraversalStackElement) stack.pop();
	}
	
	/**
	 * Do a search without populating the stack.
	 * @param targetKey The key to search for in BTree
	 * @return TreeSearchResult containing page, insertion index, atKey = true for key found
	 * @throws IOException
	 */
	@Override
	public synchronized KeySearchResult search(Comparable targetKey) throws IOException {
		KeySearchResult tsr = bTreeNavigator.search(targetKey, false);      
    	if( DEBUG || DEBUGSEARCH) {
    		System.out.println("BTreeMain.search returning with currentPage:"+tsr);
    	}
        return tsr;
	}

	/**
	* Seeks to leftmost key in current subtree. Takes the currentChild and currentIndex from currentPage and uses the
	* child at currentChild to descend the subtree and gravitate left.
	* @param tse TraversalStackElement that carries the pointers to advance
	* @param stack Stack to be populated with traversal
	* @return bottom leaf node, not pushed to stack
	*/
	private synchronized TraversalStackElement seekLeftTree(TraversalStackElement tse, Stack stack) throws IOException {
		KeyPageInterface node = (KeyPageInterface) tse.keyPage;
        if (((BTreeKeyPage) node).getmIsLeafNode()) {
        	if(DEBUGSEARCH)
            	System.out.printf("%s Leaf node numkeys:%d%n",this.getClass().getName(),node.getNumKeys());
                    //for (int i = 0; i < node.getNumKeys(); i++) {
                    //        System.out.print(" Page:"+GlobalDBIO.valueOf(node.getPageId())+" INDEX:"+i+" node:"+node.getKey(i) + ", ");
                    //}
                    //System.out.println("\n");
            tse.index = 0;
        } else {
            if(DEBUGSEARCH)
            	System.out.printf("%s NonLeaf node numkeys:%d%n",this.getClass().getName(),node.getNumKeys());
            KeyPageInterface btk = (KeyPageInterface) node.getPage(tse.index);
            TraversalStackElement tsex = new TraversalStackElement(node, tse.index, tse.index);
            stack.push(tsex);
            return seekLeftTree(new TraversalStackElement(btk, 0, 0), stack);
        }                       
        return tse;
	}

	/**
	* Seeks to rightmost key in current subtree
	* @param tse TraversalStackElement that carries the pointers to advance
	* @param stack Stack to be populated with traversal
	* @return the bottom leaf node, not pushed to stack
	*/
	private synchronized TraversalStackElement seekRightTree(TraversalStackElement tse, Stack stack) throws IOException {
		KeyPageInterface node = (KeyPageInterface) tse.keyPage;
        if (((BTreeKeyPage) node).getmIsLeafNode()) {
        	if(DEBUG) {
            	System.out.printf("%s Leaf node numkeys:%d %s %s%n",this.getClass().getName(),node.getNumKeys(),node,tse);
            	printStack(stack);
            }
            tse.index = node.getNumKeys()-1;
        } else {
            if(DEBUG) {
            	System.out.printf("%s NonLeaf node numkeys:%d %s %s%n",this.getClass().getName(),node.getNumKeys(),node,tse);
            	printStack(stack);
            }
            KeyPageInterface btk = (KeyPageInterface) node.getPage(tse.index);
            TraversalStackElement tsex = new TraversalStackElement(node, tse.index, tse.index);
            stack.push(tsex);
            return seekRightTree(new TraversalStackElement(btk, btk.getNumKeys(), btk.getNumKeys()), stack);
        }                       
        return tse;
	}

	private synchronized void printStack(Stack stack) {
		System.out.println("Stack Depth:"+stack.size());
		for(int i = 0; i < stack.size(); i++) {
			TraversalStackElement tse = (TraversalStackElement) stack.get(i);
			System.out.println("index:"+i+" "+GlobalDBIO.valueOf(tse.keyPage.getPageId())+" "+tse.index+" "+tse.child);
		}
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
                    	System.out.print("Leaf node numkeys:"+node.getNumKeys());
                            for (int i = 0; i < node.getNumKeys(); i++) {
                                    System.out.print(" Page:"+GlobalDBIO.valueOf(node.getPageId())+" INDEX:"+i+" node:"+node.getKey(i) + ", ");
                            }
                            System.out.println("\n");
                    } else {
                    	System.out.print("NonLeaf node:"+node.getNumKeys());
                            int i;
                            for (i = 0; i < node.getNumKeys(); i++) {
                            	KeyPageInterface btk = (KeyPageInterface) node.getPage(i);
                                printBTree(btk);
                                System.out.print(" Page:"+GlobalDBIO.valueOf(node.getPageId())+" INDEX:"+i+" node:"+ node.getKey(i) + ", ");
                            }
                            // get last far right node
                            printBTree((KeyPageInterface) node.getPage(i));
                            System.out.println("\n");
                    }                       
            }
    }
     
    synchronized void printBTreeDescending(KeyPageInterface node) throws IOException {
        if (node != null) {
                if (((BTreeKeyPage) node).getmIsLeafNode()) {
                	System.out.print("Leaf node numkeys:"+node.getNumKeys());
                        for (int i = node.getNumKeys()-1; i >= 0; i--) {
                                System.out.print(" Page:"+GlobalDBIO.valueOf(node.getPageId())+" INDEX:"+i+" node:"+node.getKey(i) + ", ");
                        }
                        System.out.println("\n");
                } else {
                	System.out.print("NonLeaf node:"+node.getNumKeys());
                        int i;
                        for (i = node.getNumKeys(); i > 0; i--) {
                        	KeyPageInterface btk = (KeyPageInterface) node.getPage(i);
                            printBTreeDescending(btk);
                            System.out.print(" Page:"+GlobalDBIO.valueOf(node.getPageId())+" INDEX:"+i+" node:"+ node.getKey(i-1) + ", ");
                        }
                        // get last left node
                        printBTreeDescending((KeyPageInterface) node.getPage(0));
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
   	 
   	public static void main(String[] args) throws Exception {
    	BigSackAdapter.setTableSpaceDir(args[0]);
		BufferedTreeSet bts = BigSackAdapter.getBigSackTreeSet(Class.forName(args[1]));
		KeyValueMainInterface bTree = bts.getKVStore();
		((BTreeMain)bTree).printBTreeDescending((BTreeKeyPage) bTree.getRoot()[0]);
   	}

}

