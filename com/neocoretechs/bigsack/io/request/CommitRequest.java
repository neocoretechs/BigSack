package com.neocoretechs.bigsack.io.request;

import java.io.IOException;
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
 * to proceed. The commit request proxies a request to the buffer pools to flush. Instead of waiting and
 * queuing to an empty queue we just queue a request to the end to serialize it.
 * Copyright (C) NeoCoreTechs 2014
 * @author jg
 *
 */
public final class CommitRequest implements IoRequestInterface {
	private static boolean DEBUG = false;
	private MappedBlockBuffer blockManager;
	private CyclicBarrier barrierSynch;
	private int tablespace;
	private CountDownLatch barrierCount;
	private RecoveryLogManager recoveryLog;
	private IoInterface ioManager;
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
		if( DEBUG )
			System.out.println("CommitRequest queued flushed buffer, tablespace "+tablespace+ " latches "+barrierCount+
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
		ioManager = ioi;
	}
	
	@Override
	public void setTablespace(int tablespace) {
		this.tablespace = tablespace;
	}
	public String toString() {
		return "Commit Request for tablespace "+tablespace;
	}

}
