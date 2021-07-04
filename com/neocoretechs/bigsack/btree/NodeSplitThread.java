package com.neocoretechs.bigsack.btree;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;


/**
 * The request packet contains the current node to split. The function of each of the two node split threads
 * is to process each half of the payload KeyPageInterface. This means acquiring a block, picking off the
 * left or right half of the keys in the main node, and forming the new left or right node. The logic to 
 * perform either the left or right processing is in the request payload invoked by process().
 * The cyclic barrier is initialized with 3 waiters and is recycled for each completion of the 2 threads.
 * The additional cycle is the main thread waiting.
 * @author jg
 *
 */
public class NodeSplitThread implements Runnable {
	private static boolean DEBUG = false;
	private volatile boolean shouldRun = true;
	private static int QUEUEMAX = 1024;
	private CyclicBarrier synch;

	public NodeSplitThread(CyclicBarrier synch) {
		this.synch = synch;
	}
	
	public CyclicBarrier getBarrier() { return synch; }
	

	
	@Override
	public void run() {
		while(shouldRun) {
			try {
					//IoRequestInterface iori = (IoRequestInterface) requestQueue.take();
					if( DEBUG ) {
						System.out.println("NodeSplitThread processing:");
					}
					//iori.process();
					synch.await();
				
			} catch (InterruptedException | BrokenBarrierException e) {
				return;
			}
		}

	}

}
