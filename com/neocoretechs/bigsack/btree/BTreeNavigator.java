package com.neocoretechs.bigsack.btree;

import java.io.IOException;
import java.util.Stack;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.io.ThreadPoolManager;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.keyvaluepages.KVIteratorIF;
import com.neocoretechs.bigsack.keyvaluepages.KeyPageInterface;
import com.neocoretechs.bigsack.keyvaluepages.KeySearchResult;
import com.neocoretechs.bigsack.keyvaluepages.KeyValue;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
import com.neocoretechs.bigsack.keyvaluepages.NodeInterface;

/**
 * Auxiliary methods to aid BTree navigation. Insert, split, merge, retrieve.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 */
public class BTreeNavigator<K extends Comparable, V> {
	private static final boolean DEBUG = false;
	private static final boolean DEBUGINSERT = false;
	private static final boolean DEBUGSPLIT = false;
	private static final boolean DEBUGMERGE = false;
	private static final boolean DEBUGTREE = false;
    public final static int     REBALANCE_FOR_LEAF_NODE         =   1;
    public final static int     REBALANCE_FOR_INTERNAL_NODE     =   2;

    private BTNode<K, V> mIntermediateInternalNode = null;
    private int mNodeIdx = 0;
    private final Stack<StackInfo> mStack = new Stack<StackInfo>();
    private KeyValueMainInterface bTreeMain;
    // search results
    private KeySearchResult tsr;
    // node split thread infrastructure
	private CyclicBarrier nodeSplitSynch = new CyclicBarrier(3);
	private LeftNodeSplitThread leftNodeSplitThread;
	private RightNodeSplitThread rightNodeSplitThread;

    public BTreeNavigator(KeyValueMainInterface bMain) {
    	this.bTreeMain = bMain;
		// Append the worker name to thread pool identifiers, if there, dont overwrite existing thread group
		ThreadPoolManager.init(new String[]{String.format("%s%s", "LEFTNODESPLITWORKER",bMain.getIO().getDBName()),
											String.format("%s%s", "RIGHTNODESPLITWORKER",bMain.getIO().getDBName())}, false);
		leftNodeSplitThread = new LeftNodeSplitThread(nodeSplitSynch, this);
		rightNodeSplitThread = new RightNodeSplitThread(nodeSplitSynch, this);
		ThreadPoolManager.getInstance().spin(leftNodeSplitThread,String.format("%s%s", "LEFTNODESPLITWORKER",bMain.getIO().getDBName()));
		ThreadPoolManager.getInstance().spin(rightNodeSplitThread,String.format("%s%s", "RIGHTNODESPLITWORKER",bMain.getIO().getDBName()));
    }
    /**
     * Gets the root node from KeyValueMainInterface. We have only one root for a btree.
     * @return
     */
    public NodeInterface<K, V> getRootNode() {
    	return (NodeInterface<K, V>) ((BTreeRootKeyPage)bTreeMain.getRoot()[0]).bTNode;
    }

    public KeyValueMainInterface getKeyValueMain() {
    	return bTreeMain;
    }

    public Stack<StackInfo> getStack() {
    	return mStack;
    }
    
    /**
     * private method to create node. We communicate back to our KeyPageInterface, which talks to 
     * our BlockAccessIndex, and eventually to deep store. Does not preclude creating a new root at some point.
     * @return
     * @throws IOException 
     */
    protected NodeInterface<K, V> createNode(boolean isLeaf) throws IOException {
        KeyPageInterface kpi = ((BTreeMain)bTreeMain).sdbio.getBTreePageFromPool(-1L);
        NodeInterface ni = (NodeInterface<K, V>) ((BTreeKeyPage)kpi).bTNode;
        ((BTNode)ni).setmIsLeaf(isLeaf);
        return ni;
    }

    public KeySearchResult getTreeSearchResult() {
    	return tsr;
    }
    
    /**
     * Search for the given key in the BTree
     * @param key
     * @param stack true to populate stack
     * @return
     * @throws IOException
     */
	public V search(K key, boolean stack) throws IOException {
        BTNode<K, V> currentNode = (BTNode<K, V>) getRootNode();
        BTNode<K, V> parentNode = null;
        KeyValue<K, V> currentKey;
        int i=0, numberOfKeys;
        if(stack)
        	mStack.clear();
        
        while (currentNode != null) {
            numberOfKeys = currentNode.getNumKeys();
            i = 0;
            currentKey = currentNode.getKeyValueArray(i);
            while ((i < numberOfKeys) && (key.compareTo(currentKey.getmKey()) > 0)) {
                ++i;
                if (i < numberOfKeys) {
                    currentKey = currentNode.getKeyValueArray(i);
                }
                else {
                    --i;
                    break;
                }
            }

            if ((i < numberOfKeys) && (key.compareTo(currentKey.getmKey()) == 0)) {
            	tsr = new KeySearchResult(currentNode.getPageId(), i, true);
                return currentKey.getmValue();
            }
            if(stack)
            	parentNode = currentNode;
            
            if (key.compareTo(currentKey.getmKey()) > 0) {
                currentNode = (BTNode<K, V>) BTNode.getRightChildAtIndex(currentNode, i);
            } else {
                currentNode = (BTNode<K, V>) BTNode.getLeftChildAtIndex(currentNode, i);
            }
            
            if(stack)
            	mStack.add(new StackInfo(parentNode, currentNode, i));
        }
       	tsr = new KeySearchResult(-1L, i, false);
        return null;
    }

    /**
     * Insert key and its value into the tree
     * @param key The key to insert. Must be non-null.
     * @param value the value to insert.
     * @return 0 for absent, 1 if key exists
     * @throws IOException
     */
    public int insert(K key, V value) throws IOException {
        if (getRootNode().getNumKeys() == BTNode.UPPER_BOUND_KEYNUM) {
         	if(DEBUGINSERT)
        		System.out.printf("%s.insert root is full, splitting key=%s value=%s%n", this.getClass().getName(), key, value);
            // The root is full, split it
            splitNode((BTNode) getRootNode());     
        }
        mStack.clear();
        return insertKeyAtNode((BTNode) getRootNode(), key, value) ? 1 : 0;
    }

    /**
     * Insert key and its value to the specified root
     * @param rootNode The root of this tree.
     * @param key the key to insert.
     * @param value The value for the inserted, or overwritten key;
     * @return true if key existed, false if it is new
     * @throws IOException
     */
    private boolean insertKeyAtNode(BTNode rootNode, K key, V value) throws IOException {
        int numberOfKeys = rootNode.getNumKeys();
      	// was it an empty leaf?
        if(rootNode.getIsLeaf()) {
            if(numberOfKeys == 0) {
             	if(DEBUGINSERT)
            		System.out.printf("%s.insertKeyAtNode root %s is EMPTY leaf, current keys=0 key=%s value=%s%n", this.getClass().getName(),GlobalDBIO.valueOf(rootNode.getPageId()), key, value);
                // Empty root
                rootNode.setKeyValueArray(0, new KeyValue<K, V>(key, value, rootNode));
                rootNode.getKeyValueArray(0).keyState = KeyValue.synchStates.mustWrite;
                rootNode.getKeyValueArray(0).valueState = KeyValue.synchStates.mustWrite;
                rootNode.getPage().setNumKeys(rootNode.getNumKeys()); // set the page with new number of keys
                rootNode.setUpdated(true);
                rootNode.getPage().putPage();
                return false;
            }
            // is it a full leaf?
            if(numberOfKeys == BTNode.UPPER_BOUND_KEYNUM) {
                // If the child node is a full node then handle it by splitting out
                // then insert key starting at the root node after splitting node
               	if(DEBUGINSERT)
            		System.out.printf("%s.insertKeyAtNode moving to split node %s current keys=%d key=%s value=%s%n", this.getClass().getName(), GlobalDBIO.valueOf(rootNode.getPageId()), rootNode.getNumKeys(), key, value);
                splitNode(rootNode);
              	return insertKeyAtNode(rootNode, key, value);
            }
        }
        // start the search
        int i = 0;
        int cmpRes = 0;
        KeyValue<K, V> currentKey = null;
        boolean foundSlot = false;
     	if(DEBUGINSERT)
    		System.out.printf("%s.insertKeyAtNode search root %s current keys=%d key=%s value=%s%n", this.getClass().getName(), GlobalDBIO.valueOf(rootNode.getPageId()), rootNode.getNumKeys(), key, value);
     	for(; i < numberOfKeys;i++) {
     	    currentKey = rootNode.getKeyValueArray(i);
     	    cmpRes = key.compareTo(currentKey.getmKey());
     	    if(cmpRes == 0 ) {
     	    	// The key already existed so replace its value and done with it
     	    	if(DEBUGINSERT)
     	    		System.out.printf("%s.insertKeyAtNode root %s, key EXISTS, replace found key current keys=%d key=%s value=%s insert position=%d%n", this.getClass().getName(), GlobalDBIO.valueOf(rootNode.getPageId()), rootNode.getNumKeys(), key, value,i);
     	    	currentKey.setmValue(value);
     	    	currentKey.valueState = KeyValue.synchStates.mustReplace;
     	    	rootNode.setUpdated(true);
     	    	rootNode.getPage().setUpdated(true);
     	    	rootNode.getPage().putPage();
     	    	return true;
     	    } else {
     	    	if(cmpRes > 0) {
     	    		// key > node key, continue search
     	    		continue;
     	    	} else {
     	    		// key < node key, stop and perform operation
     	    		foundSlot = true;
     	    		break;
     	    	}
     	    }
     	}
      	int newInsertPosition = i;
     	// if its a leaf, assume we have traversed to the proper point for insertion and proceed, else continue the traversal
        if(rootNode.getIsLeaf()) {
          	if(foundSlot) { // we found insert position in sequence otherwise we skip all the right movement and place new node at end
        		// move the keys to the right to make room for new key at proper position
        		for(i = numberOfKeys-1; i >= newInsertPosition; i--) {
        			rootNode.setKeyValueArray(i + 1, rootNode.getKeyValueArray(i));
        			rootNode.getKeyValueArray(i + 1).keyState = KeyValue.synchStates.mustUpdate;
        			rootNode.getKeyValueArray(i + 1).valueState = KeyValue.synchStates.mustUpdate;
        		}
        	} else
        		newInsertPosition = numberOfKeys;
        	// now insert new node at proper position, which is at new end if we came from right node, otherwise its in the new shifted sequence
            rootNode.setKeyValueArray(newInsertPosition, new KeyValue<K, V>(key, value, rootNode));
            rootNode.getKeyValueArray(newInsertPosition).keyState = KeyValue.synchStates.mustWrite;
            rootNode.getKeyValueArray(newInsertPosition).valueState = KeyValue.synchStates.mustWrite;
            rootNode.setUpdated(true);
            rootNode.getPage().setNumKeys(rootNode.getNumKeys());
            rootNode.getPage().putPage();
           	if(DEBUGINSERT)
        		System.out.printf("%s.insertKeyAtNode root %s is leaf, key inserted at position %d, current keys=%d key=%s value=%s%n", this.getClass().getName(), GlobalDBIO.valueOf(rootNode.getPageId()), newInsertPosition, rootNode.getNumKeys(), key, value);
            // number of keys is automatically increased by placement via setKeyValueArray
            return false;
        }
        // root is non-leaf here
        // This is an internal node (i.e: not a leaf node)
        // So let find the child node where the key is supposed to belong
     	// result: special case if key is greater than all others, go right, otherwise always go left
     	// this is because index represents left child and index+1 indicates right child and our node is stored in ascending order
        BTNode<K, V> btNode;
        if(foundSlot) {
         	if(DEBUGINSERT)
            	System.out.printf("%s.insertKeyAtNode root %s is non-leaf, slot found getting left child, current keys=%d key=%s value=%s child position=%d currentKey=%s%n", this.getClass().getName(),GlobalDBIO.valueOf(rootNode.getPageId()), rootNode.getNumKeys(), key, value,i, currentKey);
            btNode = (BTNode<K, V>) BTNode.getLeftChildAtIndex(rootNode, newInsertPosition);      
        } else {
          	if(DEBUGINSERT)
        		System.out.printf("%s.insertKeyAtNode root %s is non-leaf, slot not found key getting right child, current keys=%d key=%s value=%s child position=%d currentKey=%s%n", this.getClass().getName(), GlobalDBIO.valueOf(rootNode.getPageId()), rootNode.getNumKeys(), key, value,i, currentKey);
            btNode = (BTNode<K, V>) BTNode.getRightChildAtIndex(rootNode, numberOfKeys-1); // this shifts the index by 1
            newInsertPosition = numberOfKeys; // from right node we are just adding the new one to the end
        }
        mStack.push(new StackInfo(rootNode, btNode, newInsertPosition));
        // see if we can merge the node we are going to descend into
        // it cant be leaf or we wind up with null child pointers
     	if(!btNode.getIsLeaf() && btNode.getNumKeys() == 1 && rootNode.getNumKeys() < BTNode.UPPER_BOUND_KEYNUM-1) {
     		mergeParent(foundSlot);
     		btNode = rootNode; // we now re-scan with newly added child node
     	}
        if(DEBUGINSERT)
    		System.out.printf("%s.insertKeyAtNode moving to recursively insert in node %s current keys=%d key=%s value=%s depth=%d%n", this.getClass().getName(), GlobalDBIO.valueOf(btNode.getPageId()), btNode.getNumKeys(), key, value, mStack.size());
        return insertKeyAtNode(btNode, key, value);
    }

    /**
     * Split a child node with a presumed parent. Perform a merge on single value node with parent if not root later.
     * @param parentNode The new parent of the previously full key
     * @param nodeIdx Position into which to insert LOWER_BOUND_KEYNUM from btNode into parentNode
     * @param btNode The previous, full key
     * @throws IOException
     */
    private void splitNode(BTNode parentNode) throws IOException {
        // create 2 new node with the same leaf status as the previous full node
    	leftNodeSplitThread.startSplit(parentNode);
    	rightNodeSplitThread.startSplit(parentNode);
    	try {
			nodeSplitSynch.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
			return;
		}
        BTNode<K, V> leftNode = leftNodeSplitThread.getResult();//(BTNode<K, V>) createNode(parentNode.getIsLeaf());
        BTNode<K, V> rightNode = rightNodeSplitThread.getResult();//(BTNode<K, V>) createNode(parentNode.getIsLeaf());
       	if(DEBUGSPLIT)
    		System.out.printf("%s.splitNode parentNode %s%n", this.getClass().getName(), GlobalDBIO.valueOf(parentNode.getPageId()));
        // Since the node is full,
        // new nodes must share LOWER_BOUND_KEYNUM (aka t - 1) keys from the node
       	/* single thread code
       	int i;
        leftNode.setNumKeys(BTNode.LOWER_BOUND_KEYNUM);
        rightNode.setNumKeys(BTNode.LOWER_BOUND_KEYNUM);
        // Copy right half of the keys from the node to the new nodes
      	//if(DEBUGSPLIT)
    	//	System.out.printf("%s.splitNode copy keys. parentNode %s%n", this.getClass().getName(), parentNode);
        for (i = 0; i < BTNode.LOWER_BOUND_KEYNUM; ++i) {
        	leftNode.setKeyValueArray(i, parentNode.getKeyValueArray(i));
        	leftNode.childPages[i] = parentNode.childPages[i];
        	leftNode.setChild(i, parentNode.getChildNoread(i));
        	leftNode.getKeyValueArray(i).keyState = KeyValue.synchStates.mustUpdate; // transfer Optr
        	leftNode.getKeyValueArray(i).valueState = KeyValue.synchStates.mustUpdate; // transfer Optr
            parentNode.setKeyValueArray(i, null);
            parentNode.setChild(i, null);
        }
        for(i = BTNode.MIN_DEGREE; i < BTNode.UPPER_BOUND_KEYNUM; i++) {
            rightNode.setKeyValueArray(i-BTNode.MIN_DEGREE, parentNode.getKeyValueArray(i));
            rightNode.childPages[i-BTNode.MIN_DEGREE] = parentNode.childPages[i];
            rightNode.setChild(i-BTNode.MIN_DEGREE, parentNode.getChildNoread(i));
        	rightNode.getKeyValueArray(i-BTNode.MIN_DEGREE).keyState = KeyValue.synchStates.mustUpdate; // transfer Optr
        	rightNode.getKeyValueArray(i-BTNode.MIN_DEGREE).valueState = KeyValue.synchStates.mustUpdate; // transfer Optr
            parentNode.setKeyValueArray(i, null);
            parentNode.setChild(i, null);
        }
        rightNode.childPages[BTNode.UPPER_BOUND_KEYNUM-BTNode.MIN_DEGREE] = parentNode.childPages[BTNode.UPPER_BOUND_KEYNUM];
        rightNode.setChild(BTNode.UPPER_BOUND_KEYNUM-BTNode.MIN_DEGREE, parentNode.getChildNoread(BTNode.UPPER_BOUND_KEYNUM));
        parentNode.setChild(BTNode.UPPER_BOUND_KEYNUM, null);
        */
      	//if(DEBUGSPLIT)
    	//	System.out.printf("%s.splitNode setup parent. parentNodeNode %s, leftNode %s rightNode=%s%n", this.getClass().getName(), parentNode, leftNode, rightNode);
        // The node should have 1 key at this point.
        // move its middle key to position 0 and set left and right child pointers to new node.
        parentNode.setKeyValueArray(0, parentNode.getKeyValueArray(BTNode.LOWER_BOUND_KEYNUM));
        parentNode.setKeyValueArray(BTNode.LOWER_BOUND_KEYNUM, null);
        parentNode.getKeyValueArray(0).keyState = KeyValue.synchStates.mustUpdate;
        parentNode.getKeyValueArray(0).valueState = KeyValue.synchStates.mustUpdate;
        parentNode.setChild(0, leftNode); // make sure to set child pages after setChild
        parentNode.childPages[0] = leftNode.getPageId();
        parentNode.setChild(1, rightNode);
        parentNode.childPages[1] = rightNode.getPageId();
        parentNode.setNumKeys(1);
        parentNode.getPage().setNumKeys(1);
        parentNode.setmIsLeaf(false);
        leftNode.setUpdated(true);
        rightNode.setUpdated(true);
        parentNode.setUpdated(true);
        leftNode.getPage().setNumKeys(leftNode.getNumKeys());
        leftNode.getPage().putPage();
        rightNode.getPage().setNumKeys(rightNode.getNumKeys());
        rightNode.getPage().putPage();
        parentNode.getPage().putPage(); // parent might be flushed from buffer pool, so do a put
     	if(DEBUGSPLIT)
    		System.out.printf("%s.splitNode exit. parentNodeNode %s%n", this.getClass().getName(), GlobalDBIO.valueOf(parentNode.getPageId()));
    }
    /**
     * Merge the split node with the parent to maintain balance, if and only if parent is not full
     * @param foundSlot If we came from left node from parent, then we have to shift nodes to insert new sibling, otherwise add to end
     * @throws IOException
     */
    private void mergeParent(boolean foundSlot) throws IOException {
    	StackInfo si = mStack.pop();
    	BTNode rootNode = si.mParent;
        int numberOfKeys = rootNode.getNumKeys();
        int i = numberOfKeys - 1;
        int newInsertPosition = si.mNodeIdx;
    	if(DEBUGMERGE)
    		System.out.printf("%s.mergeParent merging node %s current keys=%d%n", this.getClass().getName(), GlobalDBIO.valueOf(rootNode.getPageId()), numberOfKeys);
    	if(foundSlot) { // we came here from left node , otherwise we skip all the right movement and place new node at end
    		//
    		// move the keys to the right to make room for new key at proper position
    		rootNode.childPages[numberOfKeys + 1] = rootNode.childPages[numberOfKeys];
    		rootNode.setChild(numberOfKeys + 1, rootNode.getChildNoread(numberOfKeys));
    		for(; i >= newInsertPosition; i--) {
    			rootNode.setKeyValueArray(i + 1, rootNode.getKeyValueArray(i));
    			rootNode.setChild(i + 1, rootNode.getChildNoread(i));
      			rootNode.childPages[i + 1] = rootNode.childPages[i]; // make sure to setChild then set the childPages as setChild may compensate for unretrieved node
    			rootNode.getKeyValueArray(i + 1).keyState = KeyValue.synchStates.mustUpdate;
    			rootNode.getKeyValueArray(i + 1).valueState = KeyValue.synchStates.mustUpdate;
    		}
    	}
    	// now insert new node at proper position, which is at new end if we came from right node, otherwise its in the new shifted sequence
        rootNode.setKeyValueArray(newInsertPosition, si.mNode.getKeyValueArray(0));
        rootNode.setChild(newInsertPosition, si.mNode.getChildNoread(0));
        rootNode.setChild(newInsertPosition+1, si.mNode.getChildNoread(1));
        rootNode.childPages[newInsertPosition] = si.mNode.childPages[0];
        rootNode.childPages[newInsertPosition+1] = si.mNode.childPages[1];
        rootNode.getKeyValueArray(newInsertPosition).keyState = KeyValue.synchStates.mustUpdate;
        rootNode.getKeyValueArray(newInsertPosition).valueState = KeyValue.synchStates.mustUpdate;
        rootNode.setUpdated(true);
        rootNode.getPage().setNumKeys(rootNode.getNumKeys());
        rootNode.getPage().putPage();
      	if(DEBUGMERGE)
    		System.out.printf("%s.mergeParent merged node %s current keys=%d new insert=%d%n", this.getClass().getName(), GlobalDBIO.valueOf(rootNode.getPageId()), rootNode.getNumKeys(), newInsertPosition);
        // set the old node to free, previous reference to it should be gone
        si.mNode.getPage().getBlockAccessIndex().resetBlock(true);
        si.mNode.getPage().setNumKeys(0);
        // number of keys is automatically increased by placement via setKeyValueArray
    }
    
    //
    // Find the predecessor node for a specified node
    //
    private NodeInterface<K, V> findPredecessor(BTNode<K, V> btNode, int nodeIdx) throws IOException {
        if (btNode.getIsLeaf()) {
            return btNode;
        }

        BTNode<K, V> predecessorNode;
        if (nodeIdx > -1) {
            predecessorNode = (BTNode<K, V>) BTNode.getLeftChildAtIndex(btNode, nodeIdx);
            if (predecessorNode != null) {
                mIntermediateInternalNode = btNode;
                mNodeIdx = nodeIdx;
                btNode = (BTNode<K, V>) findPredecessor(predecessorNode, -1);
            }

            return btNode;
        }

        predecessorNode = (BTNode<K, V>) BTNode.getRightChildAtIndex(btNode, btNode.getNumKeys() - 1);
        if (predecessorNode != null) {
            mIntermediateInternalNode = btNode;
            mNodeIdx = btNode.getNumKeys();
            btNode = (BTNode<K, V>) findPredecessorForNode(predecessorNode, -1);
        }

        return btNode;
    }


    //
    // Find predecessor node of a specified node
    //
    private NodeInterface<K, V> findPredecessorForNode(BTNode<K, V> btNode, int keyIdx) throws IOException {
        BTNode<K, V> predecessorNode;
        BTNode<K, V> originalNode = btNode;
        if (keyIdx > -1) {
            predecessorNode = (BTNode<K, V>) BTNode.getLeftChildAtIndex(btNode, keyIdx);
            if (predecessorNode != null) {
                btNode = (BTNode<K, V>) findPredecessorForNode(predecessorNode, -1);
                rebalanceTreeAtNode(originalNode, predecessorNode, keyIdx, REBALANCE_FOR_LEAF_NODE);
            }

            return btNode;
        }

        predecessorNode = (BTNode<K, V>) BTNode.getRightChildAtIndex(btNode, btNode.getNumKeys() - 1);
        if (predecessorNode != null) {
            btNode = (BTNode<K, V>) findPredecessorForNode(predecessorNode, -1);
            rebalanceTreeAtNode(originalNode, predecessorNode, keyIdx, REBALANCE_FOR_LEAF_NODE);
        }

        return btNode;
    }


    //
    // Do the left rotation
    //
    private void performLeftRotation(BTNode<K, V> btNode, int nodeIdx, NodeInterface<K, V> parentNode, BTNode<K, V> rightSiblingNode) {
        int parentKeyIdx = nodeIdx;

        /*
        if (nodeIdx >= parentNode.mCurrentKeyNum) {
            // This shouldn't happen
            parentKeyIdx = nodeIdx - 1;
        }
        */

        // Move the parent key and relevant child to the deficient node
        btNode.setKeyValueArray(btNode.getNumKeys(), parentNode.getKeyValueArray(parentKeyIdx));
        btNode.setChild(btNode.getNumKeys() + 1,  (BTNode<K, V>) rightSiblingNode.getChild(0));
        btNode.setNumKeys(btNode.getNumKeys() + 1);

        // Move the leftmost key of the right sibling and relevant child pointer to the parent node
        parentNode.setKeyValueArray(parentKeyIdx, rightSiblingNode.getKeyValueArray(0));
        rightSiblingNode.setNumKeys(rightSiblingNode.getNumKeys() - 1);
        // Shift all keys and children of the right sibling to its left
        for (int i = 0; i < rightSiblingNode.getNumKeys(); ++i) {
            rightSiblingNode.setKeyValueArray(i, rightSiblingNode.getKeyValueArray(i + 1));
            rightSiblingNode.setChild(i,  (BTNode<K, V>) rightSiblingNode.getChild(i + 1));
        }
        rightSiblingNode.setChild(rightSiblingNode.getNumKeys(), (BTNode<K, V>) rightSiblingNode.getChild(rightSiblingNode.getNumKeys() + 1));
        rightSiblingNode.setChild(rightSiblingNode.getNumKeys() + 1,  null);
    }


    //
    // Do the right rotation
    //
    private void performRightRotation(BTNode<K, V> btNode, int nodeIdx, BTNode<K, V> parentNode, BTNode<K, V> leftSiblingNode) {
        int parentKeyIdx = nodeIdx;
        if (nodeIdx >= parentNode.getNumKeys()) {
            // This shouldn't happen
            parentKeyIdx = nodeIdx - 1;
        }

        // Shift all keys and children of the deficient node to the right
        // So that there will be available left slot for insertion
        btNode.setChild(btNode.getNumKeys() + 1,  (BTNode<K, V>) btNode.getChild(btNode.getNumKeys()));
        for (int i = btNode.getNumKeys() - 1; i >= 0; --i) {
            btNode.setKeyValueArray(i + 1, btNode.getKeyValueArray(i));
            btNode.setChild(i + 1, (BTNode<K, V>) btNode.getChild(i));
        }

        // Move the parent key and relevant child to the deficient node
        btNode.setKeyValueArray(0, parentNode.getKeyValueArray(parentKeyIdx));
        btNode.setChild(0, (BTNode<K, V>) leftSiblingNode.getChild(leftSiblingNode.getNumKeys()));
        btNode.setNumKeys(btNode.getNumKeys() + 1);

        // Move the leftmost key of the right sibling and relevant child pointer to the parent node
        parentNode.setKeyValueArray(parentKeyIdx, leftSiblingNode.getKeyValueArray(leftSiblingNode.getNumKeys() - 1));
        leftSiblingNode.setChild(leftSiblingNode.getNumKeys(), null);
        leftSiblingNode.setNumKeys(leftSiblingNode.getNumKeys() - 1);
    }


    //
    // Do a left sibling merge
    // Return true if it should continue further
    // Return false if it is done
    //
    private boolean performMergeWithLeftSibling(BTNode<K, V> btNode, int nodeIdx, BTNode<K, V> parentNode, BTNode<K, V> leftSiblingNode) throws IOException {
        if (nodeIdx == parentNode.getNumKeys()) {
            // For the case that the node index can be the right most
            nodeIdx = nodeIdx - 1;
        }

        // Here we need to determine the parent node's index based on child node's index (nodeIdx)
        if (nodeIdx > 0) {
            if (leftSiblingNode.getKeyValueArray(leftSiblingNode.getNumKeys() - 1).getmKey().compareTo(parentNode.getKeyValueArray(nodeIdx - 1).getmKey()) < 0) {
                nodeIdx = nodeIdx - 1;
            }
        }

        // Copy the parent key to the node (on the left)
        leftSiblingNode.setKeyValueArray(leftSiblingNode.getNumKeys(), parentNode.getKeyValueArray(nodeIdx));
        leftSiblingNode.setNumKeys(leftSiblingNode.getNumKeys() + 1);

        // Copy keys and children of the node to the left sibling node
        for (int i = 0; i < btNode.getNumKeys(); ++i) {
            leftSiblingNode.setKeyValueArray(leftSiblingNode.getNumKeys() + i, btNode.getKeyValueArray(i));
            leftSiblingNode.setChild(leftSiblingNode.getNumKeys() + i, (BTNode<K, V>) btNode.getChild(i));
            btNode.setKeyValueArray(i, null);
        }
        leftSiblingNode.setNumKeys(leftSiblingNode.getNumKeys() + btNode.getNumKeys());
        leftSiblingNode.setChild(leftSiblingNode.getNumKeys(),  (BTNode<K, V>) btNode.getChild(btNode.getNumKeys()));
        btNode.setNumKeys(0);  // Abandon the node

        // Shift all relevant keys and children of the parent node to the left
        // since it lost one of its keys and children (by moving it to the child node)
        int i;
        for (i = nodeIdx; i < parentNode.getNumKeys() - 1; ++i) {
            parentNode.setKeyValueArray(i, parentNode.getKeyValueArray(i + 1));
            parentNode.setChild(i + 1, (BTNode<K, V>) parentNode.getChild(i + 2));
        }
        parentNode.setKeyValueArray(i, null);
        parentNode.setChild(parentNode.getNumKeys(), null);
        parentNode.setNumKeys(parentNode.getNumKeys() - 1);

        // Make sure the parent point to the correct child after the merge
        parentNode.setChild(nodeIdx,leftSiblingNode);

        if ((parentNode == getRootNode()) && (parentNode.getNumKeys() == 0)) {
            // Root node is updated.  It should be done
            leftSiblingNode.setAsNewRoot();
            return false;
        }

        return true;
    }

    //
    // Do the right sibling merge
    // Return true if it should continue further
    // Return false if it is done
    //
    private boolean performMergeWithRightSibling(BTNode<K, V> btNode, int nodeIdx, BTNode<K, V> parentNode, BTNode<K, V> rightSiblingNode) throws IOException {
        // Copy the parent key to right-most slot of the node
        btNode.setKeyValueArray(btNode.getNumKeys(),  parentNode.getKeyValueArray(nodeIdx));
        btNode.setNumKeys(btNode.getNumKeys() + 1);

        // Copy keys and children of the right sibling to the node
        for (int i = 0; i < rightSiblingNode.getNumKeys(); ++i) {
            btNode.setKeyValueArray(btNode.getNumKeys() + i,  rightSiblingNode.getKeyValueArray(i));
            btNode.setChild(btNode.getNumKeys() + i, (BTNode<K, V>) rightSiblingNode.getChild(i));
        }
        btNode.setNumKeys(btNode.getNumKeys() + rightSiblingNode.getNumKeys());
        btNode.setChild(btNode.getNumKeys(),  (BTNode<K, V>) rightSiblingNode.getChild(rightSiblingNode.getNumKeys()));
        rightSiblingNode.setNumKeys(0);  // Abandon the sibling node

        // Shift all relevant keys and children of the parent node to the left
        // since it lost one of its keys and children (by moving it to the child node)
        int i;
        for (i = nodeIdx; i < parentNode.getNumKeys() - 1; ++i) {
            parentNode.setKeyValueArray(i,  parentNode.getKeyValueArray(i + 1));
            parentNode.setChild(i + 1, (BTNode<K, V>) parentNode.getChild(i + 2));
        }
        parentNode.setKeyValueArray(i, null);
        parentNode.setChild(parentNode.getNumKeys(), null);
        parentNode.setNumKeys(parentNode.getNumKeys() - 1);

        // Make sure the parent point to the correct child after the merge
        parentNode.setChild(nodeIdx, btNode);

        if ((parentNode == getRootNode()) && (parentNode.getNumKeys() == 0)) {
            // Root node is updated.  It should be done
            btNode.setAsNewRoot();
            return false;
        }

        return true;
    }


    //
    // Search the specified key within a node
    // Return index of the keys if it finds
    // Return -1 otherwise
    //
    private int searchKey(BTNode<K, V> btNode, K key) throws IOException {
        for (int i = 0; i < btNode.getNumKeys(); ++i) {
            if (key.compareTo(btNode.getKeyValueArray(i).getmKey()) == 0) {
                return i;
            }
            else if (key.compareTo(btNode.getKeyValueArray(i).getmKey()) < 0) {
                return -1;
            }
        }

        return -1;
    }

    private BTNode<K, V> seekRightTree(BTNode<K, V> treeNode, int index) {
    	if(treeNode.getNumKeys() == 0)
    		return null;
		BTNode<K, V> parentNode = treeNode;
		do {
    		BTNode<K, V> childNode = (BTNode<K, V>) BTNode.getRightChildAtIndex(parentNode, index);
    		mStack.add(new StackInfo(parentNode,childNode,index));
    		if(childNode != null)
    			index = childNode.getNumKeys();
    		else
    			index = parentNode.getNumKeys();
    		parentNode = childNode;
    	} while(parentNode != null && !parentNode.getIsLeaf());
    	return treeNode;
    }
    
    private BTNode<K, V> seekLeftTree(BTNode<K, V> treeNode, int index) {
		if(DEBUGTREE)
			System.out.printf("%s.seekLeftTree(%s, %d)%n",this.getClass().getName(), treeNode, index);
		BTNode<K, V> parentNode = treeNode;
		do {
    		BTNode<K, V> childNode = (BTNode<K, V>) BTNode.getLeftChildAtIndex(parentNode, index);
    		mStack.add(new StackInfo(parentNode,childNode,index));
    		if(DEBUGTREE)
    			System.out.printf("%s.seekLeftTree pushed parent=%s child=%s index=%d%n",this.getClass().getName(), parentNode, childNode, index);
    		index = 0;
    		parentNode = childNode;
    	} while(parentNode != null && !parentNode.getIsLeaf());
    	return treeNode;
    }
    
    public KeyValue get(Object object) throws IOException {
    	KVIteratorIF iterImpl = new KVIteratorIF() {
			@Override
			public boolean item(Comparable key, Object value) {
				if(DEBUGTREE)
					System.out.printf("%s.item(%s, %s) target=%s%n",this.getClass().getName(), key, value, object);
				if(value.equals(object))
					return true;
				return false;
			}		
    	};
       	mStack.clear();
    	return retrieveEntriesInOrder((BTNode<K, V>) getRootNode(), iterImpl, 0);
    }

    public KeyValue<K, V> retrieveEntriesInOrder(BTNode<K, V> treeNode, KVIteratorIF<K, V> iterImpl, int index) throws IOException {
    	treeNode = seekLeftTree(treeNode, index);
        boolean bStatus;
        KeyValue<K, V> keyVal = null;
        while(!mStack.isEmpty()) {
        	StackInfo stack = mStack.pop();
			if(DEBUGTREE)
				System.out.printf("%s.retrieveEntriesInOrder(%s, %d) popped=%s%n",this.getClass().getName(), treeNode, index, stack);
        	BTNode tNode = stack.mNode;
        	if(tNode == null)// then parent is leaf if mNode null
        		tNode = stack.mParent;
        	// process this node regardless, we have taken care of subtree and then descend left on next node or right at end
        	int numKeys = tNode.getNumKeys();
        	for (int i = 0; i < numKeys; ++i) {
        		keyVal = tNode.getKeyValueArray(i);
        		bStatus = iterImpl.item(keyVal.getmKey(), keyVal.getmValue());
        		if (bStatus) {
        			return keyVal;
        		}
        		if(!tNode.getIsLeaf()) { 
        			int newIdx = stack.mNodeIdx + 1;
        			if(newIdx == numKeys) {
        				if(BTNode.getRightChildAtIndex(tNode, newIdx) == null)
        					continue;
        				retrieveEntriesInOrder((BTNode<K, V>) BTNode.getRightChildAtIndex(tNode, newIdx), iterImpl, newIdx);
        			} else {
        				if(BTNode.getLeftChildAtIndex(tNode, newIdx) == null)
        					continue;
        				retrieveEntriesInOrder((BTNode<K, V>) BTNode.getLeftChildAtIndex(tNode, newIdx), iterImpl, newIdx);
        			}
        		}
        	}
        }
        return keyVal;
    }

    /**
     * 
     * @param key
     * @return The deleted value or null if not found
     * @throws IOException
     */
    public V delete(K key) throws IOException {
        mIntermediateInternalNode = null;
        KeyValue<K, V> keyVal = deleteKey(null, (BTNode)getRootNode(), key, 0);
        if (keyVal == null) {
            return null;
        }
        bTreeMain.delete(keyVal.getValueOptr(), keyVal.getmValue());
        bTreeMain.delete(keyVal.getKeyOptr(), key);
        return keyVal.getmValue();
    }

    /**
     * Delete the given key.
     * @param parentNode
     * @param btNode
     * @param key
     * @param nodeIdx
     * @return The Kev/Value entry of deleted key
     * @throws IOException
     */
    private KeyValue<K, V> deleteKey(BTNode<K, V> parentNode, BTNode<K, V> btNode, K key, int nodeIdx) throws IOException {
        int i;
        int nIdx;
        KeyValue<K, V> retVal;

        if (btNode == null) {
            // The tree is empty
            return null;
        }

        if (btNode.getIsLeaf()) {
            nIdx = searchKey(btNode, key);
            if (nIdx < 0) {
                // Can't find the specified key
                return null;
            }

            retVal = btNode.getKeyValueArray(nIdx);

            if ((btNode.getNumKeys() > BTNode.LOWER_BOUND_KEYNUM) || (parentNode == null)) {
                // Remove it from the node
                for (i = nIdx; i < btNode.getNumKeys() - 1; ++i) {
                    btNode.setKeyValueArray(i, btNode.getKeyValueArray(i + 1));
                    btNode.getKeyValueArray(i).keyState = KeyValue.synchStates.mustUpdate;
                }
                btNode.setKeyValueArray(i, null);
                btNode.getKeyValueArray(i).keyState = KeyValue.synchStates.mustUpdate;
                btNode.setNumKeys(btNode.getNumKeys() - 1);
                btNode.getPage().setUpdated(true);
                if (btNode.getNumKeys() == 0) {
                    // btNode is actually the root node
                    btNode.setAsNewRoot();
                }

                return retVal;
            }

            // Find the left sibling
            BTNode<K, V> rightSibling;
            BTNode<K, V> leftSibling = (BTNode<K, V>) BTNode.getLeftSiblingAtIndex(parentNode, nodeIdx);
            if ((leftSibling != null) && (leftSibling.getNumKeys() > BTNode.LOWER_BOUND_KEYNUM)) {
                // Remove the key and borrow a key from the left sibling
                moveLeftLeafSiblingKeyWithKeyRemoval(btNode, nodeIdx, nIdx, parentNode, leftSibling);
            }
            else {
                rightSibling = (BTNode<K, V>) BTNode.getRightSiblingAtIndex(parentNode, nodeIdx);
                if ((rightSibling != null) && (rightSibling.getNumKeys() > BTNode.LOWER_BOUND_KEYNUM)) {
                    // Remove a key and borrow a key the right sibling
                    moveRightLeafSiblingKeyWithKeyRemoval(btNode, nodeIdx, nIdx, parentNode, rightSibling);
                }
                else {
                    // Merge to its sibling
                    boolean isRebalanceNeeded = false;
                    boolean bStatus;
                    if (leftSibling != null) {
                        // Merge with the left sibling
                        bStatus = doLeafSiblingMergeWithKeyRemoval(btNode, nodeIdx, nIdx, parentNode, leftSibling, false);
                        if (!bStatus) {
                            isRebalanceNeeded = false;
                        }
                        else if (parentNode.getNumKeys() < BTNode.LOWER_BOUND_KEYNUM) {
                            // Need to rebalance the tree
                            isRebalanceNeeded = true;
                        }
                    }
                    else {
                        // Merge with the right sibling
                        bStatus = doLeafSiblingMergeWithKeyRemoval(btNode, nodeIdx, nIdx, parentNode, rightSibling, true);
                        if (!bStatus) {
                            isRebalanceNeeded = false;
                        }
                        else if (parentNode.getNumKeys() < BTNode.LOWER_BOUND_KEYNUM) {
                            // Need to rebalance the tree
                            isRebalanceNeeded = true;
                        }
                    }

                    if (isRebalanceNeeded && (getRootNode() != null)) {
                        rebalanceTree((BTNode<K, V>) getRootNode(), parentNode, (K) parentNode.getKeyValueArray(0).getmKey());
                    }
                }
            }

            return retVal;  // Done with handling for the leaf node
        }

        //
        // At this point the node is an internal node
        //

        nIdx = searchKey(btNode, key);
        if (nIdx >= 0) {
            // We found the key in the internal node

            // Find its predecessor
            mIntermediateInternalNode = btNode;
            mNodeIdx = nIdx;
            BTNode<K, V> predecessorNode =  (BTNode<K, V>) findPredecessor(btNode, nIdx);
            KeyValue<K, V> predecessorKey = predecessorNode.getKeyValueArray(predecessorNode.getNumKeys() - 1);

            // Swap the data of the deleted key and its predecessor (in the leaf node)
            KeyValue<K, V> deletedKey = btNode.getKeyValueArray(nIdx);
            btNode.setKeyValueArray(nIdx, predecessorKey);
            predecessorNode.setKeyValueArray(predecessorNode.getNumKeys() - 1, deletedKey);

            // mIntermediateNode is done in findPrecessor
            return deleteKey(mIntermediateInternalNode, predecessorNode, deletedKey.getmKey(), mNodeIdx);
        }

        //
        // Find the child subtree (node) that contains the key
        //
        i = 0;
        KeyValue<K, V> currentKey = btNode.getKeyValueArray(0);
        while ((i < btNode.getNumKeys()) && (key.compareTo(currentKey.getmKey()) > 0)) {
            ++i;
            if (i < btNode.getNumKeys()) {
                currentKey = btNode.getKeyValueArray(i);
            }
            else {
                --i;
                break;
            }
        }

        BTNode<K, V> childNode;
        if (key.compareTo(currentKey.getmKey()) > 0) {
            childNode = (BTNode<K, V>) BTNode.getRightChildAtIndex(btNode, i);
            if (childNode.getKeyValueArray(0).getmKey().compareTo(btNode.getKeyValueArray(btNode.getNumKeys() - 1).getmKey()) > 0) {
                // The right-most side of the node
                i = i + 1;
            }
        }
        else {
            childNode = (BTNode<K, V>) BTNode.getLeftChildAtIndex(btNode, i);
        }

        return deleteKey(btNode, childNode, key, i);
    }


    //
    // Remove the specified key and move a key from the right leaf sibling to the node
    // Note: The node and its sibling must be leaves
    //
    private void moveRightLeafSiblingKeyWithKeyRemoval(BTNode<K, V> btNode,
                                                       int nodeIdx,
                                                       int keyIdx,
                                                       NodeInterface<K, V> parentNode,
                                                       BTNode<K, V> rightSiblingNode) {
        // Shift to the right where the key is deleted
        for (int i = keyIdx; i < btNode.getNumKeys() - 1; ++i) {
            btNode.setKeyValueArray(i, btNode.getKeyValueArray(i + 1));
        }

        btNode.setKeyValueArray(btNode.getNumKeys() - 1, parentNode.getKeyValueArray(nodeIdx));
        parentNode.setKeyValueArray(nodeIdx, rightSiblingNode.getKeyValueArray(0));

        for (int i = 0; i < rightSiblingNode.getNumKeys() - 1; ++i) {
            rightSiblingNode.setKeyValueArray(i, rightSiblingNode.getKeyValueArray(i + 1));
        }

        rightSiblingNode.setNumKeys(rightSiblingNode.getNumKeys() - 1);
    }


    //
    // Remove the specified key and move a key from the left leaf sibling to the node
    // Note: The node and its sibling must be leaves
    //
    private void moveLeftLeafSiblingKeyWithKeyRemoval(NodeInterface<K, V> btNode,
                                                      int nodeIdx,
                                                      int keyIdx,
                                                      NodeInterface<K, V> parentNode,
                                                      BTNode<K, V> leftSiblingNode) {
        // Use the parent key on the left side of the node
        nodeIdx = nodeIdx - 1;

        // Shift to the right to where the key will be deleted 
        for (int i = keyIdx; i > 0; --i) {
            btNode.setKeyValueArray(i, btNode.getKeyValueArray(i - 1));
        }

        btNode.setKeyValueArray(0, parentNode.getKeyValueArray(nodeIdx));
        parentNode.setKeyValueArray(nodeIdx, leftSiblingNode.getKeyValueArray(leftSiblingNode.getNumKeys() - 1));
        leftSiblingNode.setNumKeys(leftSiblingNode.getNumKeys() - 1);
    }


    //
    // Do the leaf sibling merge
    // Return true if we need to perform futher re-balancing action
    // Return false if we reach and update the root hence we don't need to go futher for re-balancing the tree
    //
    private boolean doLeafSiblingMergeWithKeyRemoval(BTNode<K, V> btNode,
                                                     int nodeIdx,
                                                     int keyIdx,
                                                     BTNode<K, V> parentNode,
                                                     BTNode<K, V> siblingNode,
                                                     boolean isRightSibling) throws IOException {
        int i;

        if (nodeIdx == parentNode.getNumKeys()) {
            // Case node index can be the right most
            nodeIdx = nodeIdx - 1;
        }

        if (isRightSibling) {
            // Shift the remaining keys of the node to the left to remove the key
            for (i = keyIdx; i < btNode.getNumKeys() - 1; ++i) {
                btNode.setKeyValueArray(i, btNode.getKeyValueArray(i + 1));
            }
            btNode.setKeyValueArray(i, parentNode.getKeyValueArray(nodeIdx));
        }
        else {
            // Here we need to determine the parent node id based on child node id (nodeIdx)
            if (nodeIdx > 0) {
                if (siblingNode.getKeyValueArray(siblingNode.getNumKeys() - 1).getmKey().compareTo(parentNode.getKeyValueArray(nodeIdx - 1).getmKey()) < 0) {
                    nodeIdx = nodeIdx - 1;
                }
            }

            siblingNode.setKeyValueArray(siblingNode.getNumKeys(), parentNode.getKeyValueArray(nodeIdx));
            // siblingNode.mKeys[siblingNode.mCurrentKeyNum] = parentNode.mKeys[0];
            siblingNode.setNumKeys(siblingNode.getNumKeys() + 1);

            // Shift the remaining keys of the node to the left to remove the key
            for (i = keyIdx; i < btNode.getNumKeys() - 1; ++i) {
                btNode.setKeyValueArray(i, btNode.getKeyValueArray(i + 1));
            }
            btNode.setKeyValueArray(i,  null);
            btNode.setNumKeys(btNode.getNumKeys() - 1);
        }

        if (isRightSibling) {
            for (i = 0; i < siblingNode.getNumKeys(); ++i) {
                btNode.setKeyValueArray(btNode.getNumKeys() + i, siblingNode.getKeyValueArray(i));
                siblingNode.setKeyValueArray(i, null);
            }
            btNode.setNumKeys(btNode.getNumKeys() + siblingNode.getNumKeys());
        }
        else {
            for (i = 0; i < btNode.getNumKeys(); ++i) {
                siblingNode.setKeyValueArray(siblingNode.getNumKeys() + i, btNode.getKeyValueArray(i));
                btNode.setKeyValueArray(i, null);
            }
            siblingNode.setNumKeys(siblingNode.getNumKeys() + btNode.getNumKeys());
            btNode.setKeyValueArray(btNode.getNumKeys(), null);
        }

        // Shift the parent keys accordingly after the merge of child nodes
        for (i = nodeIdx; i < parentNode.getNumKeys() - 1; ++i) {
            parentNode.setKeyValueArray(i, parentNode.getKeyValueArray(i + 1));
            parentNode.setChild(i + 1, (BTNode<K, V>) parentNode.getChild(i + 2));
        }
        parentNode.setKeyValueArray(i, null);
        parentNode.setChild(parentNode.getNumKeys(), null);
        parentNode.setNumKeys(parentNode.getNumKeys() - 1);

        if (isRightSibling) {
            parentNode.setChild(nodeIdx, btNode);
        }
        else {
            parentNode.setChild(nodeIdx, siblingNode);
        }

        if ((getRootNode() == parentNode) && (getRootNode().getNumKeys() == 0)) {
            // Only root left
            BTNode mRoot = (BTNode<K, V>) parentNode.getChild(nodeIdx);
            mRoot.setAsNewRoot();
            //mRoot.mIsLeaf = true;
            return false;  // Root has been changed, we don't need to go further
        }

        return true;
    }


    /**
     * Re-balance the tree at a specified node
     * @param parentNode the parent node of the node needs to be re-balanced
     * @param btNode the node needs to be re-balanced
     * @param nodeIdx the index of the parent node's child array where the node belongs
     * @param balanceType either REBALANCE_FOR_LEAF_NODE or REBALANCE_FOR_INTERNAL_NODE. REBALANCE_FOR_LEAF_NODE: the node is a leaf. REBALANCE_FOR_INTERNAL_NODE: the node is an internal node
     * @return true if it needs to continue rebalancing further. false if further rebalancing is no longer needed
     * @throws IOException 
     */
    private boolean rebalanceTreeAtNode(BTNode<K, V> parentNode, BTNode<K, V> btNode, int nodeIdx, int balanceType) throws IOException {
        if (balanceType == REBALANCE_FOR_LEAF_NODE) {
            if ((btNode == null) || (btNode == getRootNode())) {
                return false;
            }
        }
        else if (balanceType == REBALANCE_FOR_INTERNAL_NODE) {
            if (parentNode == null) {
                // Root node
                return false;
            }
        }

        if (btNode.getNumKeys() >= BTNode.LOWER_BOUND_KEYNUM) {
            // The node doesn't need to rebalance
            return false;
        }

        BTNode<K, V> rightSiblingNode;
        BTNode<K, V> leftSiblingNode = (BTNode<K, V>) BTNode.getLeftSiblingAtIndex(parentNode, nodeIdx);
        if ((leftSiblingNode != null) && (leftSiblingNode.getNumKeys() > BTNode.LOWER_BOUND_KEYNUM)) {
            // Do right rotate
            performRightRotation(btNode, nodeIdx, parentNode, leftSiblingNode);
        }
        else {
            rightSiblingNode = (BTNode<K, V>) BTNode.getRightSiblingAtIndex(parentNode, nodeIdx);
            if ((rightSiblingNode != null) && (rightSiblingNode.getNumKeys() > BTNode.LOWER_BOUND_KEYNUM)) {
                // Do left rotate
                performLeftRotation(btNode, nodeIdx, parentNode, rightSiblingNode);
            }
            else {
                // Merge the node with one of the siblings
                boolean bStatus;
                if (leftSiblingNode != null) {
                    bStatus = performMergeWithLeftSibling(btNode, nodeIdx, parentNode, leftSiblingNode);
                }
                else {
                    bStatus = performMergeWithRightSibling(btNode, nodeIdx, parentNode, rightSiblingNode);
                }

                if (!bStatus) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Re-balance the tree upward from the lower node to the upper node
     * @param upperNode
     * @param lowerNode
     * @param key
     * @throws IOException 
     */
    private void rebalanceTree(BTNode<K, V> upperNode, NodeInterface<K, V> lowerNode, K key) throws IOException {
        mStack.clear();
        mStack.add(new StackInfo(null, upperNode, 0));

        //
        // Find the child subtree (node) that contains the key
        //
        BTNode<K, V> parentNode, childNode;
        KeyValue<K, V> currentKey;
        int i;
        parentNode = upperNode;
        while ((parentNode != lowerNode) && !parentNode.getIsLeaf()) {
            currentKey = parentNode.getKeyValueArray(0);
            i = 0;
            while ((i < parentNode.getNumKeys()) && (key.compareTo(currentKey.getmKey()) > 0)) {
                ++i;
                if (i < parentNode.getNumKeys()) {
                    currentKey = parentNode.getKeyValueArray(i);
                }
                else {
                    --i;
                    break;
                }
            }

            if (key.compareTo(currentKey.getmKey()) > 0) {
                childNode = (BTNode<K, V>) BTNode.getRightChildAtIndex(parentNode, i);
                if (childNode.getKeyValueArray(0).getmKey().compareTo(parentNode.getKeyValueArray(parentNode.getNumKeys() - 1).getmKey()) > 0) {
                    // The right-most side of the node
                    i = i + 1;
                }
            }
            else {
                childNode = (BTNode<K, V>) BTNode.getLeftChildAtIndex(parentNode, i);
            }

            if (childNode == null) {
                break;
            }

            if (key.compareTo(currentKey.getmKey()) == 0) {
                break;
            }

            mStack.add(new StackInfo(parentNode, childNode, i));
            parentNode = childNode;
        }

        boolean bStatus;
        StackInfo stackInfo;
        while (!mStack.isEmpty()) {
            stackInfo = mStack.pop();
            if ((stackInfo != null) && !stackInfo.mNode.getIsLeaf()) {
                bStatus = rebalanceTreeAtNode(stackInfo.mParent,
                                              stackInfo.mNode,
                                              stackInfo.mNodeIdx,
                                              REBALANCE_FOR_INTERNAL_NODE);
                if (!bStatus) {
                    break;
                }
            }
        }
    }

    /**
     * Inner class StackInfo for back tracing
     * Structure contains parent node and node index
     */
    public class StackInfo {
        public BTNode<K, V> mParent = null;
        public BTNode<K, V> mNode = null;
        public int mNodeIdx = -1;
        /**
         * Construct an entry for stack trace
         * @param parent
         * @param node
         * @param nodeIdx
         */
        public StackInfo(BTNode<K, V> parent, BTNode<K, V> node, int nodeIdx) {
            mParent = parent;
            mNode = node;
            mNodeIdx = nodeIdx;
        }
        @Override
        public String toString() {
        	return String.format("StackInfo parent=%s node=%s index=%d%n", mParent, mNode, mNodeIdx);
        }
    }
}
