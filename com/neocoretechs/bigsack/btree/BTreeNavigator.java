package com.neocoretechs.bigsack.btree;

import java.io.IOException;
import java.util.Stack;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.btree.BTreeNavigator.StackInfo;
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
 * <dd>Case 2: For a non-leaf that deletes from a leaf and does NOT empty it, we can rotate the right far
 * left child or left far right child to the former position in the parent.<p/>
 * Special case here is when a parent has 2 leaves with one node, in that case bring them both up into parent
 * and remove 2 leaves and designate parent a leaf.
 * <dd>Case 3: Finally for 2 internal nodes, a non-leaf parent deleting from a non-leaf child, we have to take the right node
 * and follow it to the left most leaf, take the first child, and rotate it into the slot. That is,
 * the least valued node immediately to the right<p/>
 * If case 3 empties a leaf, handle it with case using the parent of that leftmost leaf.
 * In the context of the methods, 'rootNode' refers to the starting node in the process, and not explicitly to
 * the root node of the entire tree.
 * 
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 */
public class BTreeNavigator<K extends Comparable, V> {
	private static final boolean DEBUG = false;
	private static final boolean DEBUGINSERT = false;
	private static final boolean DEBUGSPLIT = false;
	private static final boolean DEBUGMERGE = false;
	private static final boolean DEBUGTREE = false;
 
    private int mNodeIdx = 0;
    private final Stack<StackInfo> mStack = new Stack<StackInfo>();
    private KeyValueMainInterface bTreeMain;
    // search results
    private KeySearchResult tsr;
    // node split thread infrastructure
	private CyclicBarrier nodeSplitSynch = new CyclicBarrier(3);
	private CyclicBarrier nodeSplitSynchDelete = new CyclicBarrier(2); // when left or right split runs independently
	private LeftNodeSplitThread leftNodeSplitThread;
	private RightNodeSplitThread rightNodeSplitThread;
	private LeftNodeSplitThread leftNodeSplitThreadDelete;
	private RightNodeSplitThread rightNodeSplitThreadDelete;

    public BTreeNavigator(KeyValueMainInterface bMain) {
    	this.bTreeMain = bMain;
		// Append the worker name to thread pool identifiers, if there, dont overwrite existing thread group
		ThreadPoolManager.init(new String[]{String.format("%s%s", "LEFTNODESPLITWORKER",bMain.getIO().getDBName()),
											String.format("%s%s", "RIGHTNODESPLITWORKER",bMain.getIO().getDBName()),
											String.format("%s%s", "DELETELEFTNODESPLITWORKER",bMain.getIO().getDBName()),
											String.format("%s%s", "DELETERIGHTNODESPLITWORKER",bMain.getIO().getDBName())
											}, false);
		leftNodeSplitThread = new LeftNodeSplitThread(nodeSplitSynch, this);
		rightNodeSplitThread = new RightNodeSplitThread(nodeSplitSynch, this);
		leftNodeSplitThreadDelete = new LeftNodeSplitThread(nodeSplitSynchDelete, this);
		rightNodeSplitThreadDelete = new RightNodeSplitThread(nodeSplitSynchDelete, this);
		ThreadPoolManager.getInstance().spin(leftNodeSplitThread,String.format("%s%s", "LEFTNODESPLITWORKER",bMain.getIO().getDBName()));
		ThreadPoolManager.getInstance().spin(rightNodeSplitThread,String.format("%s%s", "RIGHTNODESPLITWORKER",bMain.getIO().getDBName()));
		ThreadPoolManager.getInstance().spin(leftNodeSplitThreadDelete,String.format("%s%s", "DELETELEFTNODESPLITWORKER",bMain.getIO().getDBName()));
		ThreadPoolManager.getInstance().spin(rightNodeSplitThreadDelete,String.format("%s%s", "DELETERIGHTNODESPLITWORKER",bMain.getIO().getDBName()));
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
    	seekLeftTree(tse, stack);
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
    	seekRightTree(tse, stack);
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
        KeySearchResult ksr = search(key, true); // populate stack
        if(!ksr.atKey || ksr.page == null)
        	return null; // didnt specifically find it
        KeyValue<K, V> keyVal = null;
        if(!mStack.isEmpty()) {
        	StackInfo si = mStack.peek(); // get parent and target
        	keyVal = deleteKey(si.mParent, si.mNodeIdx, si.mNode, key, ksr.insertPoint);
        } else {
        	keyVal = deleteKey(null, 0, (BTNode<K, V>)((BTreeKeyPage)ksr.page).bTNode, key, ksr.insertPoint);
        }
        if (keyVal == null) {
            return null;
        }
        V value = keyVal.getmValue();
        // delete our returned item from deep store
        deleteFromDeepStore(keyVal);
        return value;
    }
    /**
     * Remove from deep store, we assume it has been unlinked and dealt with beforehand and is no longer
     * present in the key collection for a node, this cleans up the remainder
     * @param kv
     * @throws IOException
     */
    private void deleteFromDeepStore(KeyValue<K, V> kv) throws IOException {
		if( !kv.getKeyOptr().equals(Optr.emptyPointer)) {
			bTreeMain.getIO().delete_object(kv.getKeyOptr(), GlobalDBIO.getObjectAsBytes(kv.getmKey()).length);
		}
		if( kv.getValueOptr() != null && !kv.getValueOptr().equals(Optr.emptyPointer)) {
			bTreeMain.getIO().delete_object(kv.getValueOptr(), GlobalDBIO.getObjectAsBytes(kv.getmValue()).length);
		}
    }
    /**
     * Delete the given key. See preamble above. In general we favor a non-conversion of non-leaf to leaf nodes
     * which prevents having to check all the links when we remove nodes. We favor the addition of new leaves from emptied ones.
     * When a leaf fills we convert it by splitting.
     * @param parentNode
     * @param btNode
     * @param key
     * @param nodeIdx
     * @return The Kev/Value entry of deleted key. The data in deep store is preserved until explicit delete
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
        // check case of parent one node and 2 one node leaves, in this case consolidate the survivors and demote the parent
        retVal = checkDegenerateSingletons(parentNode, btNode);
        if(retVal != null)
        	return retVal;
        // if its a leaf node, just shift left unless empty at which point we have split at parent node
        // position, then attempt a merge
        if (btNode.getIsLeaf()) {
        	System.out.println("Case 2:");
        	retVal = shiftNodeLeft(btNode, nodeIdx); // removes key from page and node, entry still in deep store
        	// start our initial stack to prime recursive rotation of empty leaf nodes if necessary
        	Stack<StackInfo> subStack = new Stack<StackInfo>();
        	subStack.push(new StackInfo(parentNode, btNode, parentIndex));
        	recursiveRotate(subStack); // much now happens
            return retVal;  // Done with handling for the leaf node
        }
        //
        // At this point the target node is an internal, non-leaf node, so case 3 in the preamble applies
        //
    	System.out.println("Case 3:");
		Stack<StackInfo> s2 = getRightChildLeastLeaf(btNode,nodeIdx);
		retVal = btNode.getKeyValueArray(nodeIdx);
		KeyValue<K, V> replVal = shiftNodeLeft(s2.peek().mNode,0); // shiftNodeLeft handles housekeeping of page and indexes etc.
		btNode.setKeyValueArray(nodeIdx, replVal); // overwrite target with right child least leaf
		recursiveRotate(s2);
        return retVal;
    }
    /**
     * Check the case of a one key node and 2 one key leaves, we delete one and consolidate the other
     * to root, then demote root to leaf
     * @param parentNode
     * @param btNode
     * @return
     * @throws IOException 
     */
    private KeyValue<K, V> checkDegenerateSingletons(BTNode<K, V> parentNode, BTNode<K, V> btNode) throws IOException {
    	KeyValue<K, V> retVal = null;
    	// If target node is root, and it has 2 leaves each with 1 key, and 1 key itself, then consolidate
    	if(parentNode == null) { // node is root as parent, so process btNode as root of this process
    		if(btNode.getNumKeys() > 1) // make sure node has 1 key
    			return null;
			if(btNode.getChild(0) == null || !((BTNode<K, V>) btNode.getChild(0)).getIsLeaf() || btNode.getChild(0).getNumKeys() > 1)
				return null;
			if(btNode.getChild(1) == null || !((BTNode<K, V>) btNode.getChild(1)).getIsLeaf() || btNode.getChild(1).getNumKeys() > 1)
				return null;
		   	System.out.println("Case degenerate 0+1:");
			retVal = btNode.getKeyValueArray(0); // 
			// move the 2 leaves to root, set root as leaf
			NodeInterface leftNode = btNode.getChild(0);
			NodeInterface rightNode = btNode.getChild(1);
			btNode.setKeyValueArray(1, rightNode.getKeyValueArray(0));
			btNode.setKeyValueArray(0, leftNode.getKeyValueArray(0));
			// left
			leftNode.setKeyValueArray(0, null);
			leftNode.setNumKeys(0);
			((BTNode)leftNode).getPage().setNumKeys(0);
			((BTNode)leftNode).getPage().getBlockAccessIndex().getBlk().resetBlock();
			((BTNode)leftNode).getPage().putPage();
			// right
			rightNode.setKeyValueArray(0, null);
			rightNode.setNumKeys(0);
			((BTNode)rightNode).getPage().setNumKeys(0);
			((BTNode)rightNode).getPage().getBlockAccessIndex().getBlk().resetBlock();
			((BTNode)rightNode).getPage().putPage();
			// parent
			btNode.setChild(0, null); // set child pages after setChild
			btNode.childPages[0] = -1L;
			btNode.setChild(1, null);
			btNode.childPages[1] = -1L;
			btNode.getKeyValueArray(0).keyState = KeyValue.synchStates.mustUpdate;
			btNode.getKeyValueArray(1).keyState = KeyValue.synchStates.mustUpdate;
		   	btNode.getPage().setNumKeys(parentNode.getNumKeys());
	    	btNode.setmIsLeaf(true);
	    	btNode.getPage().putPage();
	    	return retVal;
    	} else {
    		// parent not null, see if its our 1 key parent 1 key each node scenario
    		if(parentNode.getNumKeys() == 1 && btNode.getIsLeaf() && btNode.getNumKeys() == 1) {
    			if(parentNode.getChild(0) == btNode) {
    				// parent has 1 key, and child 0 has 1 key then check child 1
    				if(!((BTNode<K, V>) parentNode.getChild(1)).getIsLeaf() || parentNode.getChild(1).getNumKeys() > 1)
    					return null;
    		  	   	System.out.println("Case degenerate 0+2:");
    				// delete target is left leaf, move right node up and set parent to leaf
    				parentNode.setKeyValueArray(1, parentNode.getChild(1).getKeyValueArray(0));
    			} else {
    				// parent has 1 key, and child 1 has 1 key, so check child 0
    				if(parentNode.getChild(1) == btNode) {
    					if(!((BTNode<K, V>) parentNode.getChild(0)).getIsLeaf() || parentNode.getChild(0).getNumKeys() > 1)
    						return null;
    			  	   	System.out.println("Case degenerate 0+3:");
    					// delete target is right leaf, move left node up after moving parent right 1
    					parentNode.setKeyValueArray(1, parentNode.getKeyValueArray(0));
    					parentNode.setKeyValueArray(0, parentNode.getChild(0).getKeyValueArray(0));
    				} else {
    					throw new IOException("Node inconsistency in singleton parent "+parentNode);
    				}
    			}
    		} else {
    			// its not our case
    			return null;
    		}
    	}
		// target of delete, above case of one key parent one key each node was confirmed and preprocessed
    	// set up parent and get rid of other 2 nodes
    	retVal = btNode.getKeyValueArray(0);
		NodeInterface leftNode = btNode.getChild(0);
		NodeInterface rightNode = btNode.getChild(1);
		// left
		leftNode.setKeyValueArray(0, null);
		leftNode.setNumKeys(0);
		((BTNode)leftNode).getPage().setNumKeys(0);
		((BTNode)leftNode).getPage().getBlockAccessIndex().getBlk().resetBlock();
		((BTNode)leftNode).getPage().putPage();
		// right
		rightNode.setKeyValueArray(0, null);
		rightNode.setNumKeys(0);
		((BTNode)rightNode).getPage().setNumKeys(0);
		((BTNode)rightNode).getPage().getBlockAccessIndex().getBlk().resetBlock();
		((BTNode)rightNode).getPage().putPage();
		// parent
		parentNode.setChild(0, null); // set child pages after setChild
		parentNode.childPages[0] = -1L;
		parentNode.setChild(1, null);
		parentNode.childPages[1] = -1L;
		parentNode.setChild(2, null);
		parentNode.childPages[2] = -1L;
    	parentNode.getPage().setNumKeys(parentNode.getNumKeys());
		parentNode.getKeyValueArray(0).keyState = KeyValue.synchStates.mustUpdate;
		parentNode.getKeyValueArray(1).keyState = KeyValue.synchStates.mustUpdate;
    	parentNode.setmIsLeaf(true);
    	parentNode.getPage().putPage();
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
    		// split parent at point where we came to leaf child
    		// if parent position is 0 just split off left node
  			if(parentNode.getNumKeys() > 1) { // can we split off the node?
  				if(parentIndex == 0) {
  			  	   	System.out.println("Case 1+0:");
    				leftNodeSplitThreadDelete.startSplit(parentNode, btNode, 1); // btNode is target we populate, splits it off, sets parent pos to null
    			 	try {
    					nodeSplitSynchDelete.await();
    				} catch (InterruptedException | BrokenBarrierException e) {
    					e.printStackTrace();
    					return;
    				}
    				parentNode.setChild(1, btNode);
    				shiftNodeLeft(parentNode, 0); // handles putPage
    				btNode.getPage().setNumKeys(btNode.getNumKeys());
    				btNode.getPage().putPage();
  				} else {
  					// if parent is last node just extract right and move it to one we just zapped to zero
  					if(parentIndex >= parentNode.getNumKeys()-1) {
  				  	   	System.out.println("Case 1+1:");
  						rightNodeSplitThreadDelete.startSplit(parentNode, btNode, 1, parentNode.getNumKeys()-1, parentNode.getNumKeys());
  		 			 	try {
  	    					nodeSplitSynchDelete.await();
  	    				} catch (InterruptedException | BrokenBarrierException e) {
  	    					e.printStackTrace();
  	    					return;
  	    				}
  						// set num keys to current-1, then set the key at that index to mustUpdate
  						parentNode.setNumKeys(parentNode.getNumKeys()-1);
  						parentNode.getKeyValueArray(btNode.getNumKeys()-1).keyState = KeyValue.synchStates.mustUpdate;
  						btNode.getPage().setNumKeys(btNode.getNumKeys());
  						btNode.getPage().putPage();
  						parentNode.getPage().setNumKeys(parentNode.getNumKeys());
  						parentNode.getPage().putPage();
  					} else {
  				  	   	System.out.println("Case 1+2:");
  						// we have to split both nodes from parent, then potentially join parent to grandparent
  						splitNode(parentNode, parentIndex, parentIndex+1, parentNode.getNumKeys(),parentNode.getNumKeys() - parentIndex);
  						// wait for thread completion is handled in splitNode
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
   				// one node in parent, check our degenerate case
   				if(checkDegenerateSingletons(parentNode, btNode) != null) {
   					System.out.println("Degenerate singleton detected from recursiveRotate");
   					return;
   				}
				// only 1 node in parent, if left node was deleted, pick least valued right leaf to rotate in to parent
				// if right node was deleted, pick greatest valued left leaf node to rotate in to parent
   				// then place old parent link 
   				BTNode<K, V> reNode = null;
   				if(BTNode.getLeftChildAtIndex(parentNode, 0) == btNode) {
   			  	   	System.out.println("Case 1+3:");
   					Stack<StackInfo> s2 = getRightChildLeastLeaf(parentNode,0); // start from parent element 0, go right, then leftmost
   					if(s2.empty()) {
   						System.out.println("Stack empty at Case 1+3");
   						return;
   					}
					printStack(s2);
   					reNode = s2.peek().mNode; // our target, presumably
   					retVal = shiftNodeLeft(reNode,0); // get out left node out, shiftNodeLeft handles housekeeping of page and indexes etc.
   					KeyValue<K, V> saveKey = parentNode.getKeyValueArray(0);
   					parentNode.setKeyValueArray(0, retVal);
   					btNode.setKeyValueArray(0, saveKey); // exchange
   					btNode.getPage().setNumKeys(1); //because btNode was originally empty
   					parentNode.getKeyValueArray(0).keyState = KeyValue.synchStates.mustUpdate;
   					btNode.getKeyValueArray(0).keyState = KeyValue.synchStates.mustUpdate;
   					parentNode.getPage().putPage();
   					btNode.getPage().putPage();
   					recursiveRotate(s2);
   				} else {
   					if(BTNode.getRightChildAtIndex(parentNode, 0) == btNode) {
   				  	   	System.out.println("Case 1+4:");
   						// peel off right node and put, no routine for right shift as its trivial
   						Stack<StackInfo> s2 = getLeftChildGreatestLeaf(parentNode,0);
   						if(s2.empty()) {
   	   						System.out.println("Stack empty at Case 1+4");
   	   						return;
   	   					}
   						reNode = s2.peek().mNode;
   						retVal = reNode.getKeyValueArray(reNode.getNumKeys()-1); // far right node
   						reNode.setNumKeys(reNode.getNumKeys()-1); // shift it out
   						reNode.getPage().setNumKeys(reNode.getNumKeys());
   						reNode.getPage().putPage(); // peeled off last node
   						KeyValue<K, V> saveKey = parentNode.getKeyValueArray(0);
   	   					parentNode.setKeyValueArray(0, retVal);
   	   					btNode.setKeyValueArray(0, saveKey); // exchange
   	   					btNode.getPage().setNumKeys(1); //because btNode was originally empty
   	   					parentNode.getKeyValueArray(0).keyState = KeyValue.synchStates.mustUpdate;
   	   					btNode.getKeyValueArray(0).keyState = KeyValue.synchStates.mustUpdate;
   	   					parentNode.getPage().putPage();
   	   					btNode.getPage().putPage();
   	   					recursiveRotate(s2);
   					} else {
   						// no links out remaining? could mean we have to demote our parent node to a leaf...
   						if(parentNode.getChild(0) != null || parentNode.getChild(1) != null) {
   							throw new IOException("Could not find corresponding link to child node from parent during single node parent delete:"+parentNode);
   						} else {
   					  	   	System.out.println("Case ****9+9***:");
   							parentNode.setmIsLeaf(true);
   							parentNode.setUpdated(true);
   							parentNode.getPage().putPage();
   							return;
   						}
   					}
   				}
			}
    	}
     	// either recursively process our parent from above or return if we are done
     	if(stack.isEmpty())
     		return;
     	recursiveRotate(stack);
    }
    /**
     * Move the indicated node out and overwrite with rightmost nodes, then put page to deep store.
     * We return the overwritten node, so the value is not deleted from deep store. Number of keys is
     * decreased by 1. child pages are updated, only passed in rootNode is affected.
     * @param rootNode Node to shift
     * @param i position to overwrite
     * @return The overwritten node
     * @throws IOException
     */
    private KeyValue<K, V> shiftNodeLeft(BTNode<K, V> rootNode, int i) throws IOException {
    	int target = i;
   		//
		// move the keys to the left and overwrite indicated node
    	int numberOfKeys = rootNode.getNumKeys();
    	KeyValue<K,V> kv = rootNode.getKeyValueArray(i);
		for(; i < numberOfKeys; i++) {
			if(i+1 < numberOfKeys) {
				rootNode.setKeyValueArray(i, rootNode.getKeyValueArray(i+1));
			}
			rootNode.setChild(i, rootNode.getChildNoread(i+1));
  			rootNode.childPages[i] = rootNode.childPages[i+1]; // make sure to setChild then set the childPages as setChild may compensate for unretrieved node
			rootNode.getKeyValueArray(i).keyState = KeyValue.synchStates.mustUpdate;
			rootNode.getKeyValueArray(i).valueState = KeyValue.synchStates.mustUpdate;
		}
		--numberOfKeys;
		rootNode.setNumKeys(numberOfKeys);
		rootNode.getPage().setNumKeys(numberOfKeys);
		//System.out.println("rootNode:"+rootNode);
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
	
	private synchronized void printStack(Stack stack) {
		System.out.println("Stack Depth:"+stack.size());
		for(int i = 0; i < stack.size(); i++) {
			StackInfo tse = (StackInfo) stack.get(i);
			System.out.println(i+"= P:"+GlobalDBIO.valueOf(tse.mParent.getPageId())+" numKeys="+tse.mParent.getNumKeys()+" Leaf:"+tse.mParent.getIsLeaf()+" C:"+GlobalDBIO.valueOf(tse.mNode.getPageId())+" numKeys="+tse.mNode.getNumKeys()+" Leaf:"+tse.mNode.getIsLeaf()+" node index="+tse.mNodeIdx);
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
