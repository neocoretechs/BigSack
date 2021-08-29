package com.neocoretechs.bigsack.btree;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.keyvaluepages.KeyValue;


/**
 * The function of each of the two node split threads
 * is to process each half of the payload KeyPageInterface. This means acquiring a block, picking off the
 * left or right half of the keys in the main node, and forming the new left or right node. The logic to 
 * perform either the left or right processing is in the request payload invoked by process().
 * The cyclic barrier is initialized with 3 waiters and is recycled for each completion of the 2 threads.
 * The additional cycle is the main thread waiting.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 *
 */
public class RightNodeSplitThread<K extends Comparable, V> implements Runnable {
	private static boolean DEBUG = false;
	private volatile boolean shouldRun = true;
	private CyclicBarrier synch;
	private CountDownLatch trigger = new CountDownLatch(1);
    BTNode<K, V> rightNode = null;
    BTNode<K, V> parentNode;
    BTreeNavigator<K, V> bTree;
    int NEWKEYS;
    int LOWERRIGHTLIMIT;
    int UPPERRIGHTLIMIT;

	public RightNodeSplitThread(CyclicBarrier synch, BTreeNavigator<K, V> bTree) {
		this.synch = synch;
		this.bTree = bTree;
	}
	
	public CyclicBarrier getBarrier() { return synch; }
	/**
	 * Split a full node, bounds are taken from constants in {@link BTNode}
	 * @param parentNode
	 */
	public void startSplit(BTNode<K, V> parentNode) {
		this.parentNode = parentNode;
		this.rightNode = null;
		NEWKEYS = BTNode.LOWER_BOUND_KEYNUM;
		LOWERRIGHTLIMIT = BTNode.MIN_DEGREE;
		UPPERRIGHTLIMIT = BTNode.UPPER_BOUND_KEYNUM;
		trigger.countDown();
	}
	/**
	 * Split a potentially partially full node
	 * @param parentNode
	 * @param newKeys total keys in new node, typically upperRightLimit = loweRightLimit
	 * @param lowerRightLimit index of parent target
	 * @param upperRightLimit numKeys of parent in almost all cases
	 */
	public void startSplit(BTNode<K, V> parentNode, int newKeys, int lowerRightLimit, int upperRightLimit) {
		this.parentNode = parentNode;
		this.rightNode = null;
		NEWKEYS = newKeys;
		LOWERRIGHTLIMIT = lowerRightLimit;
		UPPERRIGHTLIMIT = upperRightLimit;
		trigger.countDown();
	}
	
	/**
	 * Split a potentially partially full node
	 * @param parentNode
	 * @param rightNode re-usable right
	 * @param newKeys total keys in new node, typically upperRightLimit = loweRightLimit
	 * @param lowerRightLimit index of parent target
	 * @param upperRightLimit numKeys of parent in almost all cases
	 */
	public void startSplit(BTNode<K, V> parentNode, BTNode<K, V> rightNode, int newKeys, int lowerRightLimit, int upperRightLimit) {
		this.parentNode = parentNode;
		this.rightNode = rightNode;
		NEWKEYS = newKeys;
		LOWERRIGHTLIMIT = lowerRightLimit;
		UPPERRIGHTLIMIT = upperRightLimit;
		trigger.countDown();
	}
	
	public BTNode<K, V> getResult() {
		return rightNode;
	}

	@Override
	public void run() {
		while(shouldRun) {
			try {
				trigger.await();
					if( DEBUG ) {
						System.out.printf("%s processing:",this.getClass().getName());
					}
				       // create node with the same leaf status as the previous full node
					if(rightNode == null)
						rightNode = (BTNode<K, V>) bTree.createNode(parentNode.getIsLeaf());
			        int i;
			       	if(DEBUG)
			    		System.out.printf("%s.splitNode parentNode %s%n", this.getClass().getName(), GlobalDBIO.valueOf(parentNode.getPageId()));
			        // Since the node is full,
			        // new nodes must share LOWER_BOUND_KEYNUM (aka t - 1) keys from the node
			        rightNode.setNumKeys(NEWKEYS);
			        // Copy right half of the keys from the node to the new nodes
			      	//if(DEBUGSPLIT)
			    	//	System.out.printf("%s.splitNode copy keys. parentNode %s%n", this.getClass().getName(), parentNode);
			        for(i = LOWERRIGHTLIMIT; i < UPPERRIGHTLIMIT; i++) {
			        	int j = i-LOWERRIGHTLIMIT;
			            rightNode.setKeyValueArray(j, parentNode.getKeyValueArray(i));
			            rightNode.setChild(j, parentNode.getChildNoread(i));
			            rightNode.childPages[j] = parentNode.childPages[i]; // make sure to set child pages after setChild in case instance is null
			        	rightNode.getKeyValueArray(j).keyState = KeyValue.synchStates.mustUpdate; // transfer Optr
			        	rightNode.getKeyValueArray(j).valueState = KeyValue.synchStates.mustUpdate; // transfer Optr
			            parentNode.setKeyValueArray(i, null);
			            parentNode.setChild(i, null);
			        }
			        rightNode.setChild(UPPERRIGHTLIMIT-LOWERRIGHTLIMIT, parentNode.getChildNoread(UPPERRIGHTLIMIT));
			        rightNode.childPages[UPPERRIGHTLIMIT-LOWERRIGHTLIMIT] = parentNode.childPages[UPPERRIGHTLIMIT];
			        parentNode.setChild(UPPERRIGHTLIMIT, null);
				synch.await();
				trigger = new CountDownLatch(1);
			} catch (InterruptedException | BrokenBarrierException e) {
				return;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

	}

}
