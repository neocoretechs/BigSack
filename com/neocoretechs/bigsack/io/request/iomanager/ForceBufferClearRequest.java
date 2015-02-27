package com.neocoretechs.bigsack.io.request.iomanager;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.pooled.MappedBlockBuffer;
import com.neocoretechs.bigsack.io.request.cluster.CompletionLatchInterface;

public final class ForceBufferClearRequest implements CompletionLatchInterface {
	private int tablespace;
	private CountDownLatch barrierCount;
	private CyclicBarrier barrierSynch;
	private MappedBlockBuffer blockBuffer;
	public ForceBufferClearRequest(MappedBlockBuffer blockBuffer, CountDownLatch barrierCount, CyclicBarrier forceBarrierSynch) {
		this.blockBuffer = blockBuffer;
		this.barrierCount = barrierCount;
		this.barrierSynch = forceBarrierSynch;
	}
	@Override
	public synchronized void process() throws IOException {
		blockBuffer.forceBufferClear();
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
		return "ForceBufferClearRequest for tablespace "+tablespace;
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
		cb = barrierSynch;		
	}

}
