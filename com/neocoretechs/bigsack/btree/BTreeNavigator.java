package com.neocoretechs.bigsack.btree;

import java.io.IOException;
import java.util.Stack;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.btree.BTreeNavigator.StackInfo;
import com.neocoretechs.bigsack.io.ThreadPoolManager;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.keyvaluepages.KVIteratorIF;
import com.neocoretechs.bigsack.keyvaluepages.KeyPageInterface;
import com.neocoretechs.bigsack.keyvaluepages.KeySearchResult;
import com.neocoretechs.bigsack.keyvaluepages.KeyValue;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
import com.neocoretechs.bigsack.keyvaluepages.NodeInterface;
import com.neocoretechs.bigsack.keyvaluepages.TraversalStackElement;

/**
 * Auxiliary methods to aid BTree navigation. Insert, split, merge, retrieve.
 * Our acumen is when leaf fills, then it is split to a middle parent and two leaves. 
 * If the grandparent is not full, we can merge the lone parent, 
 * else we wait for further splits and merges to fill that non-leaf.<p/>
 * <dd>Case 1: On delete, If we delete from a child, shift the remaining elements left if not empty<p/>
 * If a child empties, we never leave null links, so we split a node off from the end or
 * beginning if that position in the parent is the predecessor link, otherwise we split
 * the parent at the link to the now empty leaf, thus creating 2 valid links to 2 new leaves.<p/>
 * Again, if that parent can be merged with grandparent, do so.<p/>
 * <dd>Case 2: For a non-leaf that deletes from a leaf and does NOT empty it, we can rotate the far right
 * left child to the former position in the parent.<p/>
 * <dd>Case 3: Finally for 2 internal nodes, a non-leaf parent deleting from a non-leaf child, we have to take the right node
 * and follow it to the left most leaf, take the first child, and rotate it into the slot. That is,
 * the least valued node immediately to the right<p/>
 * If case 3 empties a leaf, handle it with case using the parent of that leftmost leaf.
 * 
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
	public KeySearchResult search(K key, boolean stack) throws IOException {
		if(key == null)
			throw new IOException("Key cannot be null");
        BTNode<K, V> currentNode = (BTNode<K, V>) getRootNode();
        BTNode<K, V> parentNode = null;
        KeyValue<K, V> currentKey;
        int i=0, numberOfKeys;
        if(stack)
        	mStack.clear();
        
        while (currentNode != null) {
            numberOfKeys = currentNode.getNumKeys();
            if(numberOfKeys == 0) {
            	tsr = new KeySearchResult(currentNode.getPage(), 0, false);
            	return tsr;
            }
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
            	tsr = new KeySearchResult(currentNode.getPage(), i, true);
                return tsr;
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
       	tsr = new KeySearchResult(parentNode.getPage(), i, false);
        return tsr;
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
    
    /**
     * Split a child node with a presumed parent. Perform a merge on single value node with parent if not root later.
     * @param parentNode The new parent of the previously full key
     * @param nodeIdx Position into which to insert LOWER_BOUND_KEYNUM from btNode into parentNode
     * @param btNode The previous, full key
     * @throws IOException
     */
    private void splitNode(BTNode parentNode, int leftUpperBound, int rightLowerBound, int rightUpperBound, int newRightKeys) throws IOException {
        // create 2 new node with the same leaf status as the previous full node
    	leftNodeSplitThread.startSplit(parentNode, leftUpperBound);
    	rightNodeSplitThread.startSplit(parentNode, newRightKeys, rightLowerBound, rightUpperBound);
    	try {
			nodeSplitSynch.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
			return;
		}
        BTNode<K, V> leftNode = leftNodeSplitThread.getResult();
        BTNode<K, V> rightNode = rightNodeSplitThread.getResult();
       	if(DEBUGSPLIT)
    		System.out.printf("%s.splitNode parentNode %s%n", this.getClass().getName(), GlobalDBIO.valueOf(parentNode.getPageId()));
      	//if(DEBUGSPLIT)
    	//	System.out.printf("%s.splitNode setup parent. parentNodeNode %s, leftNode %s rightNode=%s%n", this.getClass().getName(), parentNode, leftNode, rightNode);
        // The node should have 1 key at this point.
        // move its middle key to position 0 and set left and right child pointers to new node.
        parentNode.setKeyValueArray(0, parentNode.getKeyValueArray(leftUpperBound));
        parentNode.setKeyValueArray(leftUpperBound, null);
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
     * Starting at rootNode and i, get the right child, then seek left tree until leaf
     * and return the stack with that node at the top.
     * @param rootNode
     * @param i
     * @return Stack populated with leftmost right child at the top giving us the next inorder greater node
     * @throws IOException
     */
    private Stack<StackInfo> getRightChildLeastLeaf(BTNode<K, V> rootNode, int i) throws IOException {
    	BTNode<K, V> rightNode = (BTNode<K, V>) BTNode.getRightChildAtIndex(rootNode, i);
    	if(rightNode == null)
    		return null;
    	StackInfo tse = new StackInfo(rootNode,rightNode,i);
    	Stack<StackInfo> stack = new Stack<StackInfo>();
    	StackInfo tsex = seekLeftTree(tse, stack);
    	stack.push(tsex);
    	return stack;
    }
    /**
     * Starting at rootNode and i, get the left child, then seek right tree until leaf
     * and return the stack with that node at top in preparation to retrieve the far right node therein
     * @param rootNode
     * @param i
     * @return The node that gives us the next inorder lesser node at top of stack
     * @throws IOException
     */
    private Stack<StackInfo> getLeftChildGreatestLeaf(BTNode<K, V> rootNode, int i) throws IOException {
      	BTNode<K, V> leftNode = (BTNode<K, V>) rootNode.getLeftChildAtIndex(rootNode, i);
    	if(leftNode == null)
    		return null;
     	StackInfo tse = new StackInfo(rootNode,leftNode,i);
    	Stack<StackInfo> stack = new Stack<StackInfo>();
    	StackInfo tsex = seekRightTree(tse, stack);
    	stack.push(tsex);
    	return stack;
    }

    /**
     * Search strictly within a given node
     * @param btNode The given node
     * @param key the key for which to search
     * @return The index where found or -1 if not
     * @throws IOException
     */
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

    /**
     * 
     * @param key
     * @return The deleted value or null if not found
     * @throws IOException
     */
    public V delete(K key) throws IOException {
        mIntermediateInternalNode = null;
        KeySearchResult ksr = search(key, true); // populate stack
        if(!ksr.atKey)
        	return null; // didnt specifically find it;
        StackInfo si = mStack.peek(); // get parent and target
        KeyValue<K, V> keyVal = deleteKey(si.mParent, si.mNodeIdx, si.mNode, key, ksr.insertPoint);
        if (keyVal == null) {
            return null;
        }
        return keyVal.getmValue();
    }

    /**
     * Delete the given key. See preamble above. In general we favor a non-conversion of non-leaf to leaf nodes
     * which prevents having to check all the links when we remove nodes. We favor the addition of new leaves from emptied ones.
     * When a leaf fills we convert it by splitting.
     * @param parentNode
     * @param btNode
     * @param key
     * @param nodeIdx
     * @return The Kev/Value entry of deleted key
     * @throws IOException
     */
    private KeyValue<K, V> deleteKey(BTNode<K, V> parentNode, int parentIndex, BTNode<K, V> btNode, K key, int nodeIdx) throws IOException {
        int i;
        int nIdx;
        KeyValue<K, V> retVal;

        if (btNode == null) {
            // The tree is empty
            return null;
        }
        // if its a leaf node, just shift left unless empty at which point we have split at parent node
        // position, then attempt a merge
        if (btNode.getIsLeaf()) {
        	retVal = shiftNodeLeft(btNode, nodeIdx); // deletes key and data from page and node
        	// start our initial stack to prime recursive rotation of empty leaf nodes if necessary
        	Stack<StackInfo> subStack = new Stack<StackInfo>();
        	subStack.push(new StackInfo(parentNode, btNode, parentIndex));
        	recursiveRotate(subStack); // much now happens
            return retVal;  // Done with handling for the leaf node
        }
        //
        // At this point the target node is an internal, non-leaf node, so case 3 in the preamble applies
        //
		Stack<StackInfo> s2 = getRightChildLeastLeaf(btNode,nodeIdx);
		retVal = shiftNodeLeft(s2.peek().mNode,0); // shiftNodeLeft handles housekeeping of page and indexes etc.
		recursiveRotate(s2);
        return retVal;
    }
    /**
     * Call with stack populated with at least the initial parent and empty target and the index of link from parent.
     * We will recursively process to rotate the nodes leaving no empty leaf.<p/>
     * 
     * <dd>1.) is our leaf now empty?
     * <dd>2.) was it root of stack? yes, return
     * <dd>3.) split parent at point where we came to leaf child, if parent position is 0 just split off left node
     * <dd>4.) if parent is last node just extract right and move it to one we just zapped to zero
     * <dd>5.) if parent not first or last, we have to split both nodes from parent, then potentially join parent to grandparent
     * <dd>6.) corner case: only 1 node in parent, if left node was deleted, pick least valued right leaf to rotate in
	 * if right node was deleted, pick greatest valued left leaf node to rotate in.
	 * 
     * @param stack
     * @throws IOException 
     */
    @SuppressWarnings("unchecked")
	private void recursiveRotate(Stack<StackInfo> stack) throws IOException {
    	StackInfo si = stack.pop();
    	BTNode<K, V>  btNode = si.mNode;
    	BTNode<K, V> parentNode = si.mParent;
    	int parentIndex = si.mNodeIdx;
    	KeyValue<K, V> retVal;
     	if(btNode.getNumKeys() == 0) { // is our leaf now empty?
    		if(stack.isEmpty()) // it was root
    			return;
    		// split parent at point where we came to leaf child
    		// if parent position is 0 just split off left node
  			if(parentNode.getNumKeys() > 1) { // can we split off the node?
  				if(parentIndex == 0) {
    				leftNodeSplitThread.startSplit(parentNode, btNode, 1); // btNode is target we populate
    				shiftNodeLeft(parentNode, 0); // handles putPage
    				btNode.getPage().setNumKeys(btNode.getNumKeys());
    				btNode.getPage().putPage();
  				} else {
  					// if parent is last node just extract right and move it to one we just zapped to zero
  					if(parentIndex >= parentNode.getNumKeys()-1) {
  						rightNodeSplitThread.startSplit(parentNode, btNode, 1, parentNode.getNumKeys()-1, parentNode.getNumKeys());
  						// set num keys to current-1, then set the key at that index to mustUpdate
  						parentNode.setNumKeys(parentNode.getNumKeys()-1);
  						parentNode.getKeyValueArray(btNode.getNumKeys()-1).keyState = KeyValue.synchStates.mustUpdate;
  						btNode.getPage().setNumKeys(btNode.getNumKeys());
  						btNode.getPage().putPage();
  						parentNode.getPage().setNumKeys(parentNode.getNumKeys());
  						parentNode.getPage().putPage();
  					} else {
  						// we have to split both nodes from parent, then potentially join parent to grandparent
  						splitNode(parentNode, parentIndex, parentIndex+1, parentNode.getNumKeys(),parentNode.getNumKeys() - parentIndex);
  						// anything left in our stack is our target, barring that the main stack is the target
  						// if all that is empty, we split the root presumably
  						if(stack.isEmpty() && mStack.isEmpty())
  							return; //we split root
  						if(stack.isEmpty())
  							mStack.push(stack.pop()); // put our target on main to prime the merge
  						mergeParent(false);
  					}
  				}
  				return;
   			} else {
				// only 1 node in parent, if left node was deleted, pick least valued right leaf to rotate in
				// if right node was deleted, pick greatest valued left leaf node to rotate in
   				BTNode<K, V> reNode = null;
   				if(BTNode.getLeftChildAtIndex(parentNode, 0) == btNode) {
   					Stack<StackInfo> s2 = getRightChildLeastLeaf(parentNode,0); // start from parent element 0, go right, then leftmost
   					reNode = s2.peek().mNode; // our target, presumably
   					retVal = shiftNodeLeft(reNode,0); // shiftNodeLeft handles housekeeping of page and indexes etc.
   				} else {
   					if(BTNode.getRightChildAtIndex(parentNode, 0) == btNode) {
   						// peel off right node and put, no routine for right shift as its trivial
   						Stack<StackInfo> s2 = getLeftChildGreatestLeaf(parentNode,0);
   						reNode = s2.peek().mNode;
   						retVal = reNode.getKeyValueArray(reNode.getNumKeys()-1);
   						reNode.setNumKeys(reNode.getNumKeys()-1);
   						reNode.getPage().setNumKeys(reNode.getNumKeys());
   						reNode.getPage().putPage(); // peeled off last node
   					} else {
   						// no links out left? could mean we have to demote our parent node to a leaf...
   						if(parentNode.getChild(0) != null || parentNode.getChild(1) != null) {
   							throw new IOException("Could not find corresponding link to child node from parent during single node parent delete:"+parentNode);
   						} else {
   							parentNode.setmIsLeaf(true);
   							parentNode.setUpdated(true);
   							parentNode.getPage().putPage();
   							return;
   						}
   					}
   				}
   				// we processed our reNode, we have our retVal, so set previously empty leaf to it
   				btNode.setKeyValueArray(0, retVal);
   				btNode.getPage().setNumKeys(1);
   				btNode.getKeyValueArray(0).keyState = KeyValue.synchStates.mustUpdate;
   				btNode.getPage().putPage();
			}
    	}
     	// either recursively process our parent from above or return if we are done
     	if(stack.isEmpty())
     		return;
     	recursiveRotate(stack);
    }
    /**
     * Move the indicated node out and overwrite with rightmost nodes, then put page to deep store.
     * @param rootNode Node to shift
     * @param i position to overwrite
     * @return The overwritten node
     * @throws IOException
     */
    private KeyValue<K, V> shiftNodeLeft(BTNode<K, V> rootNode, int i) throws IOException {
   		//
		// move the keys to the left and overwrite indicated node
    	int numberOfKeys = rootNode.getNumKeys();
    	KeyValue<K,V> kv = rootNode.getKeyValueArray(i);
		for(; i < numberOfKeys; i++) {
			if(i+1 != numberOfKeys) {
				rootNode.setKeyValueArray(i, rootNode.getKeyValueArray(i+1));
			}
			rootNode.setChild(i, rootNode.getChildNoread(i+1));
  			rootNode.childPages[i] = rootNode.childPages[i+1]; // make sure to setChild then set the childPages as setChild may compensate for unretrieved node
			rootNode.getKeyValueArray(i).keyState = KeyValue.synchStates.mustUpdate;
			rootNode.getKeyValueArray(i).valueState = KeyValue.synchStates.mustUpdate;
		}
		--numberOfKeys;
		rootNode.setNumKeys(numberOfKeys);
		((BTreeKeyPage)rootNode.getPage()).delete(i);
		rootNode.getPage().setNumKeys(numberOfKeys);
		rootNode.getPage().putPage();
		return kv;
    }

	/**
	* Seeks to leftmost key in current subtree. Takes the currentChild and currentIndex from currentPage and uses the
	* child at currentChild to descend the subtree and gravitate left.
	* @return bottom leaf node, not pushed to stack
	*/
	private synchronized StackInfo seekLeftTree(StackInfo tse, Stack<StackInfo> stack) throws IOException {
		BTNode<K, V> node = tse.mNode;
        if (node.getIsLeaf()) {
        	if(DEBUG)
            	System.out.printf("%s Leaf node numkeys:%d%n",this.getClass().getName(),node.getNumKeys());
                    //for (int i = 0; i < node.getNumKeys(); i++) {
                    //        System.out.print(" Page:"+GlobalDBIO.valueOf(node.getPageId())+" INDEX:"+i+" node:"+node.getKey(i) + ", ");
                    //}
                    //System.out.println("\n");
            tse.mNodeIdx = 0;
        } else {
            if(DEBUG)
            	System.out.printf("%s NonLeaf node numkeys:%d%n",this.getClass().getName(),node.getNumKeys());
            BTNode<K, V> btk = (BTNode<K, V>) node.getChild(tse.mNodeIdx);
            StackInfo tsex = new StackInfo(node, btk, tse.mNodeIdx);
            stack.push(tsex);
            return seekLeftTree(new StackInfo(node, btk, 0), stack);
        }                       
        return tse;
	}

	/**
	* Seeks to rightmost key in current subtree
	* @return the bottom leaf node, not pushed to stack
	*/
	private synchronized StackInfo seekRightTree(StackInfo tse, Stack<StackInfo> stack) throws IOException {
		BTNode<K, V> node = tse.mNode;
        if (node.getIsLeaf()) {
        	if(DEBUG)
            	System.out.printf("%s Leaf node numkeys:%d%n",this.getClass().getName(),node.getNumKeys());
                    //for (int i = 0; i < node.getNumKeys(); i++) {
                    //        System.out.print(" Page:"+GlobalDBIO.valueOf(node.getPageId())+" INDEX:"+i+" node:"+node.getKey(i) + ", ");
                    //}
                    //System.out.println("\n");
        	tse.mNodeIdx = node.getNumKeys()-1;
        } else {
            if(DEBUG)
            	System.out.printf("%s NonLeaf node numkeys:%d%n",this.getClass().getName(),node.getNumKeys());
            BTNode<K, V> btk = (BTNode<K, V>) node.getChild(tse.mNodeIdx);
            StackInfo tsex = new StackInfo(node, btk, tse.mNodeIdx);
            stack.push(tsex);
            return seekLeftTree(new StackInfo(node, btk, btk.getNumKeys()), stack);
        }                       
        return tse;
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
