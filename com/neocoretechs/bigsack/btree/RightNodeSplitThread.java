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
    BTNode<K, V> rightNode;
    BTNode<K, V> parentNode;
    BTreeNavigator<K, V> bTree;


	public RightNodeSplitThread(CyclicBarrier synch, BTreeNavigator<K, V> bTree) {
		this.synch = synch;
		this.bTree = bTree;
	}
	
	public CyclicBarrier getBarrier() { return synch; }
	
	public void startSplit(BTNode<K, V> parentNode) {
		this.parentNode = parentNode;
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
			        rightNode = (BTNode<K, V>) bTree.createNode(parentNode.getIsLeaf());
			        int i;
			       	if(DEBUG)
			    		System.out.printf("%s.splitNode parentNode %s%n", this.getClass().getName(), GlobalDBIO.valueOf(parentNode.getPageId()));
			        // Since the node is full,
			        // new nodes must share LOWER_BOUND_KEYNUM (aka t - 1) keys from the node
			        rightNode.setNumKeys(BTNode.LOWER_BOUND_KEYNUM);
			        // Copy right half of the keys from the node to the new nodes
			      	//if(DEBUGSPLIT)
			    	//	System.out.printf("%s.splitNode copy keys. parentNode %s%n", this.getClass().getName(), parentNode);
			        for(i = BTNode.MIN_DEGREE; i < BTNode.UPPER_BOUND_KEYNUM; i++) {
			        	int j = i-BTNode.MIN_DEGREE;
			            rightNode.setKeyValueArray(j, parentNode.getKeyValueArray(i));
			            rightNode.childPages[j] = parentNode.childPages[i];
			            rightNode.setChild(j, parentNode.getChildNoread(i));
			        	rightNode.getKeyValueArray(j).keyState = KeyValue.synchStates.mustUpdate; // transfer Optr
			        	rightNode.getKeyValueArray(j).valueState = KeyValue.synchStates.mustUpdate; // transfer Optr
			            parentNode.setKeyValueArray(i, null);
			            parentNode.setChild(i, null);
			        }
			        rightNode.childPages[BTNode.UPPER_BOUND_KEYNUM-BTNode.MIN_DEGREE] = parentNode.childPages[BTNode.UPPER_BOUND_KEYNUM];
			        rightNode.setChild(BTNode.UPPER_BOUND_KEYNUM-BTNode.MIN_DEGREE, parentNode.getChildNoread(BTNode.UPPER_BOUND_KEYNUM));
			        parentNode.setChild(BTNode.UPPER_BOUND_KEYNUM, null);
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