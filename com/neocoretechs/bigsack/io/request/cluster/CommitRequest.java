package com.neocoretechs.bigsack.io.request.cluster;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.RecoveryLogManager;
import com.neocoretechs.bigsack.io.pooled.MappedBlockBuffer;

/**
 * A special case of request the does not propagate outward to workers but instead is
 * used to serialize commit/rollback etc. on the request queue. The overridden method
 * of AbstractClusterWork 'doPropagate' is set false to prevent this request from
 * traveling outward to nodes. The commit is two stage, with the first local 
 * operation happening which pushes out blocks from resident buffer pools to nodes.
 * In the second stage a request is sent to the nodes, causing the buffers to be persisted.
 * In lieu of waiting for a synchronization or waiting for the queue to empty, queue this 
 * type of special request to assure completion. We use countdown latches to control synchronization.
 * These latches are not serializable so we maintain them separately at the nodes.
 * This is an intent parallel computation component of a tablespace wide request.
 * We are using a CyclicBarrier set up with the number of tablespaces and after each thread
 * does a commit it will await the barrier synch. 
 * Once released from barrier synch a countdown latch is decreased which activates the
 * IO manager countdown latch waiter when count reaches 0, thereby releasing the thread
 * to proceed. The commit request is a proxy to send the commit request to the block pools. Rather than
 * wait for a semaphore or other synchronization, we are queuing a request to run when appropriate to achieve 
 * serial computation
 * Copyright (C) NeoCoreTechs 2014
 * @author jg
 *
 */
public final class CommitRequest extends AbstractClusterWork implements CompletionLatchInterface, Serializable  {
	private static final long serialVersionUID = 1L;
	private static final boolean DEBUG = false;
	private transient MappedBlockBuffer blockManager;
	private transient CyclicBarrier barrierSynch;
	private int tablespace;
	private transient CountDownLatch barrierCount;
	private transient RecoveryLogManager recoveryLog;
	private transient IoInterface ioManager;
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
		// dealloc outstanding block, call commit buffer flush in global IO, call recovery log manager commit
		if( DEBUG  )
			System.out.println("CommitRequest.process "+blockManager+" "+barrierSynch+" "+barrierCount);
		blockManager.commitBufferFlush(recoveryLog);
		try {
			if( DEBUG  )
				System.out.println("CommitRequest.process "+blockManager+" awaiting barrier "+barrierSynch);
			barrierSynch.await();
		} catch (InterruptedException |  BrokenBarrierException e) {
			// executor requests shutdown
		}
		// all buffers flushed, call commit
		recoveryLog.commit();
		// if we have local io manager that has file ops, call the close
		if( ioManager != null )
			ioManager.Fclose();
		barrierCount.countDown();
		if( DEBUG  )
			System.out.println("CommitRequest.process "+blockManager);
		//CommitBufferFlushRequest cbfr = new CommitBufferFlushRequest(blockManager, recoveryLog, barrierCount, barrierSynch);
		//cbfr.setIoInterface(ioManager);
		//blockManager.queueRequest(cbfr);
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
	@Override
	public boolean doPropagate() { return false; }
}
