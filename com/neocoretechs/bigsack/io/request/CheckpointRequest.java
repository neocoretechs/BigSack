package com.neocoretechs.bigsack.io.request;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.MappedBlockBuffer;
import com.neocoretechs.bigsack.io.RecoveryLogManager;

/**
 * A special case of request the does not propagate outward to workers but instead is
 * used to serialize checkpoint etc. on the request queue. In lieu of waiting for a synchronization
 * or waiting for the queue to empty, queue this type of special request to assure completion.
 * This is an intent parallel computation component of a tablespace wide request.
 * We are using a CyclicBarrier set up with the number of tablespaces and after each thread
 * does a commit it will await the barrier synch. 
 * Once released from barrier synch a countdown latch is decreased which activates the multi
 * threading IO manager countdown latch waiter when count reaches 0, thereby releasing the thread
 * to proceed. The commit request proxies a request to the buffer pools to flush. Instead of waiting and
 * queuing to an empty queue we just queue a request to the end to serialize it.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2014
 *
 */
public final class CheckpointRequest implements IoRequestInterface {
	private static boolean DEBUG = false;
	private MappedBlockBuffer blockManager;
	private CyclicBarrier barrierSynch;
	private int tablespace;
	private CountDownLatch barrierCount;
	private RecoveryLogManager recoveryLog;

	public CheckpointRequest(MappedBlockBuffer blockBuffer, RecoveryLogManager rlog, CyclicBarrier barrierSynch, CountDownLatch barrierCount) {
		this.blockManager = blockBuffer;
		this.recoveryLog = rlog;
		this.barrierSynch = barrierSynch;
		this.barrierCount = barrierCount;
	}
	@Override
	public void process() throws IOException {
		// dealloc outstanding block, call commit buffer flush in global IO, call recovery log manager commit
		if( DEBUG  )
			System.out.println("CheckpointRequest.process "+blockManager+" "+barrierSynch+" "+barrierCount);
		blockManager.commitBufferFlush(recoveryLog);
		try {
			if( DEBUG  )
				System.out.println("CheckpointRequest.process "+blockManager+" awaiting barrier "+barrierSynch);
			barrierSynch.await();
		} catch (InterruptedException |  BrokenBarrierException e) {
			// executor requests shutdown
			barrierCount.countDown();
			return;
		}
		// all buffers flushed, call checkpoint
		try {
			recoveryLog.checkpoint();
		} catch (IllegalAccessException e) { 
			throw new IOException(e);
		}
		barrierCount.countDown();
		if( DEBUG )
			System.out.println("CheckpointRequest queued flushed buffer, tablespace "+tablespace+ " latches "+barrierCount+
				" barrier waiters:"+barrierSynch.getNumberWaiting()+
				" barrier parties:"+barrierSynch.getParties()+
				" barier broken:"+barrierSynch.isBroken());
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
	}
	
	@Override
	public void setTablespace(int tablespace) {
		this.tablespace = tablespace;
	}
	public String toString() {
		return "Checkpoint Request for tablespace "+tablespace;
	}

}
