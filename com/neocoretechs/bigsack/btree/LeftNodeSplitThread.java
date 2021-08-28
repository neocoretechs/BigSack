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
public class LeftNodeSplitThread<K extends Comparable, V> implements Runnable {
	private static boolean DEBUG = false;
	private volatile boolean shouldRun = true;
	private CyclicBarrier synch;
	private CountDownLatch trigger = new CountDownLatch(1);
    BTNode<K, V> leftNode;
    BTNode<K, V> parentNode;
    BTreeNavigator<K, V> bTree;
    int LEFTUPPERLIMIT;

	public LeftNodeSplitThread(CyclicBarrier synch, BTreeNavigator<K, V> bTree) {
		this.synch = synch;
		this.bTree = bTree;
	}
	
	public CyclicBarrier getBarrier() { return synch; }
	/**
	 * Split a full node
	 * @param parentNode
	 */
	public void startSplit(BTNode<K, V> parentNode) {
		this.parentNode = parentNode;
		LEFTUPPERLIMIT = BTNode.LOWER_BOUND_KEYNUM;
		trigger.countDown();
	}
	/**
	 * Split a potentially partially full node
	 * @param parentNode
	 * @param leftUpperLimit
	 */
	public void startSplit(BTNode<K, V> parentNode, int leftUpperLimit) {
		this.parentNode = parentNode;
		LEFTUPPERLIMIT = leftUpperLimit;
		trigger.countDown();
	}
	
	public BTNode<K, V> getResult() {
		return leftNode;
	}
	
	@Override
	public void run() {
		while(shouldRun) {
			try {
				trigger.await();
				if( DEBUG ) {
					System.out.printf("%s processing:",this.getClass().getName());
				}
				       // create 2 new node with the same leaf status as the previous full node
			        leftNode = (BTNode<K, V>) bTree.createNode(parentNode.getIsLeaf());
			        int i;
			       	if(DEBUG)
			    		System.out.printf("%s.splitNode parentNode %s%n", this.getClass().getName(), GlobalDBIO.valueOf(parentNode.getPageId()));
			        // Since the node is full,
			        // new nodes must share LOWER_BOUND_KEYNUM (aka t - 1) keys from the node
			        leftNode.setNumKeys(LEFTUPPERLIMIT);
			        // Copy right half of the keys from the node to the new nodes
			      	//if(DEBUGSPLIT)
			    	//	System.out.printf("%s.splitNode copy keys. parentNode %s%n", this.getClass().getName(), parentNode);
			        for (i = 0; i < LEFTUPPERLIMIT; ++i) {
			        	leftNode.setKeyValueArray(i, parentNode.getKeyValueArray(i));
			        	leftNode.setChild(i, parentNode.getChildNoread(i));
			        	leftNode.childPages[i] = parentNode.childPages[i]; // make sure to set childPages after setChild in case child is null
			        	leftNode.getKeyValueArray(i).keyState = KeyValue.synchStates.mustUpdate; // transfer Optr
			        	leftNode.getKeyValueArray(i).valueState = KeyValue.synchStates.mustUpdate; // transfer Optr
			            parentNode.setKeyValueArray(i, null);
			            parentNode.setChild(i, null);
			        }
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
