package com.neocoretechs.bigsack.io.request.cluster;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.RecoveryLogManager;
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
 * to proceed. The commit request is a proxy to send the commit request to the block pools. Rather than
 * wait for a semaphore or something we are queuing a request to run when appropriate to achieve 
 * serial computation
 * Copyright (C) NeoCoreTechs 2014
 * @author jg
 *
 */
public final class CommitRequest extends AbstractClusterWork implements CompletionLatchInterface, Serializable  {
	private static final long serialVersionUID = 1L;
	private MappedBlockBuffer blockManager;
	private CyclicBarrier barrierSynch;
	private int tablespace;
	private CountDownLatch barrierCount;
	private RecoveryLogManager recoveryLog;
	private IoInterface ioManager;
	/**
	 * We re use the barriers, they are cyclic, so they are stored as fields
	 * in the blockManager and passed here
	 * @param blockBuffer
	 * @param barrierSynch
	 * @param barrierCount
	 */
	public CommitRequest(MappedBlockBuffer blockBuffer, RecoveryLogManager rlog, CyclicBarrier barrierSynch, CountDownLatch barrierCount) {
		this.blockManager = blockBuffer;
		this.recoveryLog = rlog;
		this.barrierSynch = barrierSynch;
		this.barrierCount = barrierCount;
	}
	@Override
	public void process() throws IOException {
		CommitBufferFlushRequest cbfr = new CommitBufferFlushRequest(blockManager, recoveryLog, barrierCount, barrierSynch);
		cbfr.setIoInterface(ioManager);
		blockManager.queueRequest(cbfr);
	}
	@Override
	public long getLongReturn() {
		return -1L;
	}

	@Override
	public Object getObjectReturn() {
		return new Long(-1L);
	}
	/**
	 * This method is called by queueRequest to set the proper tablespace from IOManager 
	 * It is the default way to set the active IO unit
	 */
	@Override
	public void setIoInterface(IoInterface ioi) {
		ioManager = ioi;
	}
	
	@Override
	public void setTablespace(int tablespace) {
		this.tablespace = tablespace;
	}
	public String toString() {
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
