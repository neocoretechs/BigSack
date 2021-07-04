package com.neocoretechs.bigsack.btree;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Stack;

import com.neocoretechs.bigsack.io.Optr;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.BlockStream;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.keyvaluepages.KVIteratorIF;
import com.neocoretechs.bigsack.keyvaluepages.KeyPageInterface;
import com.neocoretechs.bigsack.keyvaluepages.KeySearchResult;
import com.neocoretechs.bigsack.keyvaluepages.KeyValue;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
import com.neocoretechs.bigsack.keyvaluepages.NodeInterface;


/**
 * Class BTree
 * Description: BTree implementation
 */
public class BTree<K extends Comparable, V> {
	private static final boolean DEBUGINSERT = true;
    public final static int     REBALANCE_FOR_LEAF_NODE         =   1;
    public final static int     REBALANCE_FOR_INTERNAL_NODE     =   2;

    private BTNode<K, V> mRoot = null;
    private long  mSize = 0L;
    private BTNode<K, V> mIntermediateInternalNode = null;
    private int mNodeIdx = 0;
    private final Stack<StackInfo> mStackTracer = new Stack<StackInfo>();
    private BTreeMain bMain;
    // search results
    private KeySearchResult tsr;
	private boolean DEBUG;

    public BTree(BTreeMain bMain) {
    	this.bMain = bMain;
    	//mRoot = getRootNode(); may need buckets first
    }
    /**
     * Gets the root node stored in mRoot instance, if its null call {@link createRootNode} to
     * Explicitly create root node at db creation time. Create a {@link BTNode}, set pageId to 0.
     * then calls {@link HMapMain.createRootNode} with the newly created BTNode
     * @return
     */
    public NodeInterface<K, V> getRootNode() {
    	if(mRoot == null)
			try {
				mRoot = (BTNode<K, V>) createRootNode();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
        return mRoot;
    }


    //
    // The total number of nodes in the tree
    //
    public long size() {
        return mSize;
    }


    /**
     * Clear all tree entries
     */
    public void clear() {
        mSize = 0L;
        mRoot = null;
    }

    public KeyValueMainInterface getKeyValueMain() {
    	return bMain;
    }

    /**
     * private method to create node. We communicate back to our KeyPageInterface, which talks to 
     * our BlockAccessIndex, and eventually to deep store. Does not preclude creating a new root at some point.
     * Calls {@link HMapMain.createNode} with the new {@link BTNode}
     * @return
     * @throws IOException 
     */
    private NodeInterface<K, V> createNode(boolean isLeaf) throws IOException {
        BTNode<K, V> btNode;
        btNode = new BTNode(this, isLeaf);
        btNode.setNumKeys(0);
        KeyPageInterface newNode = bMain.createNode(btNode);
        btNode.pageId = newNode.getBlockAccessIndex().getBlockNum();
        return btNode;
    }
    /**
     * Explicitly create root node at db creation time. Create a {@link BTNode}, set pageId to 0.
     * then calls {@link HMapMain.createRootNode} with the newly created BTNode
     * @return
     * @throws IOException
     */
    private NodeInterface<K, V> createRootNode() throws IOException {
        BTNode<K, V> btNode;
        btNode = new BTNode(this, true); // leaf true
        btNode.pageId = 0L;
        btNode.setNumKeys(0);
        bMain.createRootNode(btNode);
        return btNode;
    }

    public KeySearchResult getTreeSearchResult() {
    	return tsr;
    }
    //
    // Search value for a specified key of the tree
    //
    @SuppressWarnings("unchecked")
	public V search(K key) throws IOException {
        BTNode<K, V> currentNode = mRoot;
        KeyValue<K, V> currentKey;
        int i=0, numberOfKeys;

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
            	tsr = new KeySearchResult(currentNode.pageId, i, true);
                return currentKey.getmValue();
            }

            // We don't need it
            /*
            if (currentNode.mIsLeaf) {
                return null;
            }
            */

            if (key.compareTo(currentKey.getmKey()) > 0) {
                currentNode = (BTNode<K, V>) BTNode.getRightChildAtIndex(currentNode, i);
            } else {
                currentNode = (BTNode<K, V>) BTNode.getLeftChildAtIndex(currentNode, i);
            }
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
        if (mRoot == null) {
        	if(DEBUGINSERT)
        		System.out.printf("%s.insert root is null key=%s value=%s%n", this.getClass().getName(), key, value);
            mRoot = (BTNode<K, V>) createNode(true);
        }

        ++mSize;
        if (mRoot.getNumKeys() == BTNode.UPPER_BOUND_KEYNUM) {
         	if(DEBUGINSERT)
        		System.out.printf("%s.insert root is full, splitting key=%s value=%s%n", this.getClass().getName(), key, value);
            // The root is full, split it
            BTNode<K, V> btNode = (BTNode<K, V>) createNode(false);
            //btNode.mIsLeaf = false;
            btNode.setChild(0, mRoot);
            mRoot = btNode;
            splitNode(mRoot, 0, (BTNode) btNode.getChild(0));
        }

        return insertKeyAtNode(mRoot, key, value) ? 1 : 0;
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
        int i;
        int currentKeyNum = rootNode.getNumKeys();

        if (rootNode.getIsLeaf()) {
            if (rootNode.getNumKeys() == 0) {
             	if(DEBUGINSERT)
            		System.out.printf("%s.insertKeyAtNode root is leaf, current keys=0 key=%s value=%s%n", this.getClass().getName(), key, value);
                // Empty root
                rootNode.setKeyValueArray(0, new KeyValue<K, V>(key, value, rootNode));
                rootNode.setNumKeys(rootNode.getNumKeys() + 1);
                return false;
            }

            // Verify if the specified key doesn't exist in the node
            for (i = 0; i < rootNode.getNumKeys(); ++i) {
                if (key.compareTo(rootNode.getKeyValueArray(i).getmKey()) == 0) {
                    // Find existing key, overwrite its value only
                    rootNode.getKeyValueArray(i).setmValue(value);
                    --mSize;
                    return true;
                }
            }

            i = currentKeyNum - 1;
            KeyValue<K, V> existingKeyVal = rootNode.getKeyValueArray(i);
            while ((i > -1) && (key.compareTo(existingKeyVal.getmKey()) < 0)) {
                rootNode.setKeyValueArray(i + 1, existingKeyVal);
                --i;
                if (i > -1) {
                    existingKeyVal = rootNode.getKeyValueArray(i);
                }
            }

            i = i + 1;
            rootNode.setKeyValueArray(i, new KeyValue<K, V>(key, value, rootNode));

            rootNode.setNumKeys(rootNode.getNumKeys() + 1);
            return false;
        }

        // This is an internal node (i.e: not a leaf node)
        // So let find the child node where the key is supposed to belong
        i = 0;
        int numberOfKeys = rootNode.getNumKeys();
        KeyValue<K, V> currentKey = rootNode.getKeyValueArray(i);
        while ((i < numberOfKeys) && (key.compareTo(currentKey.getmKey()) > 0)) {
            ++i;
            if (i < numberOfKeys) {
                currentKey = rootNode.getKeyValueArray(i);
            }
            else {
                --i;
                break;
            }
        }

        if ((i < numberOfKeys) && (key.compareTo(currentKey.getmKey()) == 0)) {
            // The key already existed so replace its value and done with it
            currentKey.setmValue(value);
            --mSize;
            return true;
        }

        BTNode<K, V> btNode;
        if (key.compareTo(currentKey.getmKey()) > 0) {
            btNode = (BTNode<K, V>) BTNode.getRightChildAtIndex(rootNode, i);
            i = i + 1;
        }
        else {
            if ((i - 1 >= 0) && (key.compareTo(rootNode.getKeyValueArray(i - 1).getmKey()) > 0)) {
                btNode = (BTNode<K, V>) BTNode.getRightChildAtIndex(rootNode, i - 1);
            }
            else {
                btNode = (BTNode<K, V>) BTNode.getLeftChildAtIndex(rootNode, i);
            }
        }

        if (btNode.getNumKeys() == BTNode.UPPER_BOUND_KEYNUM) {
            // If the child node is a full node then handle it by splitting out
            // then insert key starting at the root node after splitting node
            splitNode(rootNode, i, btNode);
            insertKeyAtNode(rootNode, key, value);
            return false;
        }

        return insertKeyAtNode(btNode, key, value);
    }


    //
    // Split a child with respect to its parent at a specified node
    //
    private void splitNode(BTNode parentNode, int nodeIdx, BTNode btNode) throws IOException {
        int i;

        BTNode<K, V> newNode = (BTNode<K, V>) createNode(btNode.getIsLeaf());

        // newNode.mIsLeaf = btNode.mIsLeaf;

        // Since the node is full,
        // new node must share LOWER_BOUND_KEYNUM (aka t - 1) keys from the node
        newNode.setNumKeys(BTNode.LOWER_BOUND_KEYNUM);

        // Copy right half of the keys from the node to the new node
        for (i = 0; i < BTNode.LOWER_BOUND_KEYNUM; ++i) {
            newNode.setKeyValueArray(i, btNode.getKeyValueArray(i + BTNode.MIN_DEGREE));
            btNode.setKeyValueArray(i + BTNode.MIN_DEGREE, null);
        }

        // If the node is an internal node (not a leaf),
        // copy the its child pointers at the half right as well
        if (!btNode.getIsLeaf()) {
            for (i = 0; i < BTNode.MIN_DEGREE; ++i) {
                newNode.setChild(i, (BTNode<K, V>) btNode.getChild(i + BTNode.MIN_DEGREE));
                btNode.setChild(i + BTNode.MIN_DEGREE, null);
            }
        }

        // The node at this point should have LOWER_BOUND_KEYNUM (aka min degree - 1) keys at this point.
        // We will move its right-most key to its parent node later.
        btNode.setNumKeys(BTNode.LOWER_BOUND_KEYNUM);

        // Do the right shift for relevant child pointers of the parent node
        // so that we can put the new node as its new child pointer
        for (i = parentNode.getNumKeys(); i > nodeIdx; --i) {
            parentNode.setChild(i + 1, (BTNode) parentNode.getChild(i));
            parentNode.setChild(i, null);
        }
        parentNode.setChild(nodeIdx + 1, newNode);

        // Do the right shift all the keys of the parent node the right side of the node index as well
        // so that we will have a slot for move a median key from the split node
        for (i = parentNode.getNumKeys() - 1; i >= nodeIdx; --i) {
            parentNode.setKeyValueArray(i + 1, parentNode.getKeyValueArray(i));
            parentNode.setKeyValueArray(i, null);
        }
        parentNode.setKeyValueArray(nodeIdx, btNode.getKeyValueArray(BTNode.LOWER_BOUND_KEYNUM));
        btNode.setKeyValueArray(BTNode.LOWER_BOUND_KEYNUM, null);
        parentNode.setNumKeys(parentNode.getNumKeys() + 1);
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

        if ((parentNode == mRoot) && (parentNode.getNumKeys() == 0)) {
            // Root node is updated.  It should be done
            mRoot = leftSiblingNode;
            return false;
        }

        return true;
    }


    //
    // Do the right sibling merge
    // Return true if it should continue further
    // Return false if it is done
    //
    private boolean performMergeWithRightSibling(BTNode<K, V> btNode, int nodeIdx, BTNode<K, V> parentNode, BTNode<K, V> rightSiblingNode) {
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

        if ((parentNode == mRoot) && (parentNode.getNumKeys() == 0)) {
            // Root node is updated.  It should be done
            mRoot = btNode;
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


    //
    // List all the items in the tree
    //
    public void list(KVIteratorIF<K, V> iterImpl) throws IOException {
        if (mSize < 1) {
            return;
        }

        if (iterImpl == null) {
            return;
        }

        listEntriesInOrder(mRoot, iterImpl);
    }

    public KeyValue get(Object object) throws IOException {
    	KVIteratorIF iterImpl = new KVIteratorIF() {
			@Override
			public boolean item(Comparable key, Object value) {
				if(value.equals(object))
					return true;
				return false;
			}		
    	};
    	return retrieveEntriesInOrder(mRoot, iterImpl);
    }
    //
    // Recursively loop to list out the keys and their values
    // Return true if it should continues listing out futher
    // Return false if it is done
    //
    private boolean listEntriesInOrder(BTNode<K, V> treeNode, KVIteratorIF<K, V> iterImpl) throws IOException {
        if ((treeNode == null) ||
            (treeNode.getNumKeys() == 0)) {
            return false;
        }

        boolean bStatus;
        KeyValue<K, V> keyVal;
        int currentKeyNum = treeNode.getNumKeys();
        for (int i = 0; i < currentKeyNum; ++i) {
            listEntriesInOrder((BTNode<K, V>) BTNode.getLeftChildAtIndex(treeNode, i), iterImpl);

            keyVal = treeNode.getKeyValueArray(i);
            bStatus = iterImpl.item(keyVal.getmKey(), keyVal.getmValue());
            if (!bStatus) {
                return false;
            }

            if (i == currentKeyNum - 1) {
                listEntriesInOrder((BTNode<K, V>) BTNode.getRightChildAtIndex(treeNode, i), iterImpl);
            }
        }

        return true;
    }

    private KeyValue<K, V> retrieveEntriesInOrder(BTNode<K, V> treeNode, KVIteratorIF<K, V> iterImpl) throws IOException {
        if ((treeNode == null) ||
            (treeNode.getNumKeys() == 0)) {
            return null;
        }
        boolean bStatus;
        KeyValue<K, V> keyVal;
        int currentKeyNum = treeNode.getNumKeys();
        for (int i = 0; i < currentKeyNum; ++i) {
            retrieveEntriesInOrder((BTNode<K, V>) BTNode.getLeftChildAtIndex(treeNode, i), iterImpl);
            keyVal = treeNode.getKeyValueArray(i);
            bStatus = iterImpl.item(keyVal.getmKey(), keyVal.getmValue());
            if (bStatus) {
                return keyVal;
            }

            if (i == currentKeyNum - 1) {
                retrieveEntriesInOrder((BTNode<K, V>) BTNode.getRightChildAtIndex(treeNode, i), iterImpl);
            }
        }

        return null;
    }

    //
    // Delete a key from the tree
    // Return value if it finds the key and delete it
    // Return null if it cannot find the key
    //
    public V delete(K key) throws IOException {
        mIntermediateInternalNode = null;
        KeyValue<K, V> keyVal = deleteKey(null, mRoot, key, 0);
        if (keyVal == null) {
            return null;
        }
        --mSize;
        bMain.delete(keyVal.getValueOptr(), keyVal.getmValue());
        bMain.delete(keyVal.getKeyOptr(), key);
        return keyVal.getmValue();
    }


    //
    // Delete a key from a tree node
    //
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
                }
                btNode.setKeyValueArray(i, null);
                btNode.setNumKeys(btNode.getNumKeys() - 1);

                if (btNode.getNumKeys() == 0) {
                    // btNode is actually the root node
                    mRoot = null;
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

                    if (isRebalanceNeeded && (mRoot != null)) {
                        rebalanceTree(mRoot, parentNode, parentNode.getKeyValueArray(0).getmKey());
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

        if ((mRoot == parentNode) && (mRoot.getNumKeys() == 0)) {
            // Only root left
            mRoot = (BTNode<K, V>) parentNode.getChild(nodeIdx);
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
            if ((btNode == null) || (btNode == mRoot)) {
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
        mStackTracer.clear();
        mStackTracer.add(new StackInfo(null, upperNode, 0));

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

            mStackTracer.add(new StackInfo(parentNode, childNode, i));
            parentNode = childNode;
        }

        boolean bStatus;
        StackInfo stackInfo;
        while (!mStackTracer.isEmpty()) {
            stackInfo = mStackTracer.pop();
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
	 * Serialize this page to deep store on a page boundary.
	 * Key pages are always on page boundaries. The data is written
	 * to the page buffers from the updated node values via the {@link BTreeKeyPage} facade.
	 * The push to deep store takes place at commit time or
	 * when the buffer fills and it becomes necessary to open a spot.
	 * @exception IOException If write fails
	 */
	public synchronized void putNodeToPage(BTNode<K,V> bTNode, KeyPageInterface page) throws IOException {
		if(bTNode == null) {
			throw new IOException(String.format("%s.putPage BTNode is null:%s%n",this.getClass().getName(),this));
		}
		if(!bTNode.updated) {
			if(DEBUG)
				System.out.printf("%s.putPage page NOT updated:%s%n",this.getClass().getName(),this);
			return;
			//throw new IOException("KeyPageInterface.putPage page NOT updated:"+this);
		}
		if( DEBUG ) 
			System.out.printf("%s.putPage:%s%n",this.getClass().getName(),this);
		// hold accumulated insert pages
		ArrayList<Optr> keys = new ArrayList<Optr>();
		ArrayList<Optr> values = new ArrayList<Optr>();
		int i = 0;
		for(; i < bTNode.getNumKeys(); i++) {
			if(bTNode.getKeyValueArray(i) != null && bTNode.getKeyValueArray(i) != null ) {
				if(!bTNode.getKeyValueArray(i).getKeyOptr().equals(Optr.emptyPointer) ) {
					keys.add(bTNode.getKeyValueArray(i).getKeyOptr());
				}
			}
			if(bTNode.getKeyValueArray(i) != null && bTNode.getKeyValueArray(i) != null ) {
				if(!bTNode.getKeyValueArray(i).getValueOptr().equals(Optr.emptyPointer) ) {
					values.add(bTNode.getKeyValueArray(i).getValueOptr());
				}
			}
		}
		//.map(Map::values)                  // -> Stream<List<List<String>>>
		//.flatMap(Collection::stream)       // -> Stream<List<String>>
		//.flatMap(Collection::stream)       // -> Stream<String>
		//.collect(Collectors.toSet())       // -> Set<String>
		// Persist each key that is updated to fill the keyIds in the current page
		// Once this is complete we write the page contiguously
		// Write the object serialized keys out to deep store, we want to do this out of band of writing key page
		for(i = 0; i < BTNode.UPPER_BOUND_KEYNUM; i++) {
			if( bTNode.getKeyValueArray(i) != null ) {
				if(bTNode.getKeyValueArray(i).getKeyUpdated() ) {
					// put the key to a block via serialization and assign KeyIdArray the position of stored key
					//page.putKey(i, keys);	
				}
				if( bTNode.getKeyValueArray(i).getValueUpdated()) {
					//putData(i, values);
				}
			}
		}
		//
		assert (page.getBlockAccessIndex().getBlockNum() != -1L) : " KeyPageInterface unlinked from page pool:"+this;
		// write the page to the current block
		page.getBlockAccessIndex().setByteindex((short) 0);
		// Write to the block output stream
		DataOutputStream bs = GlobalDBIO.getBlockOutputStream(page.getBlockAccessIndex());
		if( DEBUG )
			System.out.println("KeyPageInterface.putPage BlockStream:"+page);
		bs.writeByte(bTNode.getIsLeaf() ? 1 : 0);
		bs.writeInt(bTNode.getNumKeys());
		for(i = 0; i < BTNode.UPPER_BOUND_KEYNUM; i++) {
			if(bTNode.getKeyValueArray(i) != null && bTNode.getKeyValueArray(i).getKeyUpdated() ) { // if set, key was processed by putKey[i]
				bs.writeLong(bTNode.getKeyValueArray(i).getKeyOptr().getBlock());
				bs.writeShort(bTNode.getKeyValueArray(i).getKeyOptr().getOffset());
				bTNode.getKeyValueArray(i).setKeyUpdated(false);
			} else { // skip
				page.getBlockAccessIndex().setByteindex((short) (page.getBlockAccessIndex().getByteindex()+10));
			}
			// data array
			if(bTNode.getKeyValueArray(i) != null &&  bTNode.getKeyValueArray(i).getValueUpdated() ) {
				bs.writeLong(bTNode.getKeyValueArray(i).getValueOptr().getBlock());
				bs.writeShort(bTNode.getKeyValueArray(i).getValueOptr().getOffset());
				bTNode.getKeyValueArray(i).setValueUpdated(false);
			} else {
				// skip the data Id for this index as it was not updated, so no need to write anything
				page.getBlockAccessIndex().setByteindex((short) (page.getBlockAccessIndex().getByteindex()+10));
			}
		}
		// persist btree key page indexes
		for(i = 0; i <= BTNode.UPPER_BOUND_KEYNUM; i++) {
			BTreeKeyPage btk = (BTreeKeyPage) page.getPage(i);
			if(btk != null)
				bs.writeLong(btk.getBlockAccessIndex().getBlockNum());
			else // skip 8 bytes
				//bks.getBlockAccessIndex().setByteindex((short) (bks.getBlockAccessIndex().getByteindex()+8));
				bs.writeLong(-1L); // empty page
		}
		bs.flush();
		bs.close();
		if( DEBUG ) {
			System.out.println("KeyPageInterface.putPage Added Keypage @:"+this);
		}
		bTNode.updated = false;
	}

    /**
     * Inner class StackInfo for back tracing
     * Structure contains parent node and node index
     */
    public class StackInfo {
        public BTNode<K, V> mParent = null;
        public BTNode<K, V> mNode = null;
        public int mNodeIdx = -1;

        public StackInfo(BTNode<K, V> parent, BTNode<K, V> node, int nodeIdx) {
            mParent = parent;
            mNode = node;
            mNodeIdx = nodeIdx;
        }
    }
}
