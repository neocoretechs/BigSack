package com.neocoretechs.bigsack.io.request.iomanager;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.btree.BTreeMain;
import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.RecoveryLogManager;
import com.neocoretechs.bigsack.io.pooled.MappedBlockBuffer;
import com.neocoretechs.bigsack.io.pooled.ObjectDBIO;
import com.neocoretechs.bigsack.io.request.cluster.CompletionLatchInterface;
/**
 * ioManager request that synchs the buffers via cyclicbarrier after flush is performed
 * This is the main request to push blocks and commit
 * @author jg
 *
 */
public final class CommitBufferFlushRequest implements CompletionLatchInterface {
	private static boolean DEBUG = false;
	private int tablespace;
	private CyclicBarrier barrierSynch;
	private CountDownLatch barrierCount;
	private MappedBlockBuffer blockBuffer;
	private RecoveryLogManager recoveryManager;
	private IoInterface ioManager;
	public CommitBufferFlushRequest(MappedBlockBuffer blockBuffer, RecoveryLogManager rlog, CountDownLatch cdl, CyclicBarrier barrierSynch) {
		this.blockBuffer = blockBuffer;
		this.recoveryManager = rlog;
		this.barrierCount = cdl;
		this.barrierSynch = barrierSynch;
	}
	/**
	 * deallocOutstandingCommit on IoManager of blockBuffer calls:
	 * deallocOutstanding - remove latch
	 * blockBuffer.commitBufferFlush - which calls ioManager.commitBufferFlush, wherein the recovery log and deep store are written
	 * recoveryLogManager.commit - the only place recoveryLogManager.commit should be called.
	 * Then Fclose on IoManager
	 */
	@Override
	public synchronized void process() throws IOException {
		// dealloc outstanding block, call commit buffer flush in global IO, call recovery log manager commit
		if( DEBUG  )
			System.out.println("CommitBufferFlushRequest.process "+blockBuffer+" "+barrierSynch+" "+barrierCount);
		blockBuffer.commitBufferFlush();
		try {
			if( DEBUG  )
				System.out.println("CommitBufferFlushRequest.process "+blockBuffer+" awaiting barrier "+barrierSynch);
			barrierSynch.await();
		} catch (InterruptedException |  BrokenBarrierException e) {
			// executor requests shutdown
		}
		// all buffers flushed, call commit
		recoveryManager.commit(tablespace);
		ioManager.Fclose();
		barrierCount.countDown();
		if( DEBUG  )
			System.out.println("CommitBufferFlushRequest.process "+blockBuffer);
	}

	@Override
	public synchronized long getLongReturn() {
		return 0L;
	}

	@Override
	public synchronized Object getObjectReturn() {
		return null;
	}
	/**
	 * This interface implemented method is called by IoWorker before processing
	 */
	@Override
	public synchronized void setIoInterface(IoInterface ioi) {
		this.ioManager = ioi;
	}
	@Override
	public synchronized void setTablespace(int tablespace) {
		this.tablespace = tablespace;
	}
	
	public synchronized String toString() {
		return "CommitBufferFlushRequest for tablespace "+tablespace;
	}
	@Override
	public CountDownLatch getCountDownLatch() {
		return barrierCount;
	}
	@Override
	public void setCountDownLatch(CountDownLatch cdl) {
		barrierCount = cdl;
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
