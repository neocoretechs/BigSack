package com.neocoretechs.bigsack.io.request.iomanager;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.pooled.MappedBlockBuffer;
import com.neocoretechs.bigsack.io.request.cluster.CompletionLatchInterface;
/**
 * ioManager request that synchs the buffers via cyclicbarrier after flush is performed
 * @author jg
 *
 */
public final class CommitBufferFlushRequest implements CompletionLatchInterface {
	private int tablespace;
	private CyclicBarrier barrierSynch;
	private CountDownLatch barrierCount;
	private MappedBlockBuffer blockBuffer;
	public CommitBufferFlushRequest(MappedBlockBuffer blockBuffer, CountDownLatch cdl, CyclicBarrier barrierSynch) {
		this.blockBuffer = blockBuffer;
		this.barrierCount = cdl;
		this.barrierSynch = barrierSynch;
	}
	@Override
	public synchronized void process() throws IOException {
		blockBuffer.commitBufferFlush();
		try {
			barrierSynch.await();
		} catch (InterruptedException |  BrokenBarrierException e) {
			// executor requests shutdown
		}
		barrierCount.countDown();
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
	public synchronized void setIoInterface(IoInterface ioi) {}
	@Override
	public synchronized void setTablespace(int tablespace) {
		this.tablespace = tablespace;
	}
	
	public synchronized String toString() {
		return "CommitBufferFlushRequest for tablespace "+tablespace;
	}
	@Override
	public CountDownLatch getCountDownLatch() {
		return null;
	}
	@Override
	public void setCountDownLatch(CountDownLatch cdl) {
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
