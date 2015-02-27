package com.neocoretechs.bigsack.io.request.cluster;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.IoManagerInterface;
import com.neocoretechs.bigsack.io.pooled.Datablock;
import com.neocoretechs.bigsack.io.pooled.MappedBlockBuffer;
import com.neocoretechs.bigsack.io.request.iomanager.CommitBufferFlushRequest;
/**
 * A special case of request the does not propagate outward to workers but instead is
 * used to serialize commit/rollback etc. on the request queue. In lieu of waiting for a synchronization
 * or waiting for the queue to empty, queue this type of special request to assure completion.
 * This is an intent parallel computation component of a tablespace wide request.
 * We are using a CyclicBarrier set up with the number of tablespaces and after each thread
 * does a commit it will await the barrier synch. 
 * Once released from barrier synch a countdown latch is decreased which activates the multi
 * threading IO manager countdown latch waiter when count reaches 0, thereby releasing the thread
 * to proceed.
 * Copyright (C) NeoCoreTechs 2014
 * @author jg
 *
 */
public final class CommitRequest extends AbstractClusterWork implements CompletionLatchInterface, Serializable  {
	private static final long serialVersionUID = 1L;
	private MappedBlockBuffer ioManager;
	private CyclicBarrier barrierSynch;
	private int tablespace;
	private CountDownLatch barrierCount;
	/**
	 * We re use the barriers, they are cyclic, so they are stored as fields
	 * in the ioManager and passed here
	 * @param blockBuffer
	 * @param barrierSynch
	 * @param barrierCount
	 */
	public CommitRequest(MappedBlockBuffer blockBuffer, CyclicBarrier barrierSynch, CountDownLatch barrierCount) {
		this.ioManager = blockBuffer;
		this.barrierSynch = barrierSynch;
		this.barrierCount = barrierCount;
	}
	@Override
	public synchronized void process() throws IOException {
		Commit();
		barrierCount.countDown();
	}
	/**
	 * Wair for the barrier synch to arrive then flip countdown
	 * @throws IOException
	 */
	private void Commit() throws IOException {
		CommitBufferFlushRequest cbfr = new CommitBufferFlushRequest(ioManager, barrierCount, barrierSynch);
		ioManager.queueRequest(cbfr);
		// wait at the barrier until all other tablespaces arrive at their result
		try {
			barrierSynch.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			// executor shutdown on wait
		}
	}
	@Override
	public synchronized long getLongReturn() {
		return -1L;
	}

	@Override
	public synchronized Object getObjectReturn() {
		return new Long(-1L);
	}
	/**
	 * This method is called by queueRequest to set the proper tablespace from IOManager 
	 * It is the default way to set the active IO unit
	 */
	@Override
	public synchronized void setIoInterface(IoInterface ioi) {
	}
	
	@Override
	public synchronized void setTablespace(int tablespace) {
		this.tablespace = tablespace;
	}
	public synchronized String toString() {
		return "Commit Request for tablespace "+tablespace;
	}
	@Override
	public CountDownLatch getCountDownLatch() {
		return barrierCount;
	}
	@Override
	public void setCountDownLatch(CountDownLatch cdl) {
		this.barrierCount = cdl;
	}
	@Override
	public void setLongReturn(long val) {
	}
	@Override
	public void setObjectReturn(Object o) {
	}
	@Override
	public CyclicBarrier getCyclicBarrier() {
		return barrierSynch;
	}
	@Override
	public void setCyclicBarrier(CyclicBarrier cb) {
		barrierSynch = cb;
		
	}

}
