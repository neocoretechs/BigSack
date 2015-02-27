package com.neocoretechs.bigsack.io.request.iomanager;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.pooled.MappedBlockBuffer;
import com.neocoretechs.bigsack.io.request.cluster.CompletionLatchInterface;
/**
 * Issue a request to directly write the buffer. The raw store is written if block access is 0 and the log subsystem
 * is bypassed on write of the block. The incore flag is set as usual.
 * @author jg
 *
 */
public final class DirectBufferWriteRequest implements CompletionLatchInterface {
	private int tablespace;
	private CountDownLatch barrierCount;
	private CyclicBarrier barrierSynch; 
	private MappedBlockBuffer blockBuffer;
	/**
	 * Use of the cyclic barrier causes a rendezvous at the request level before latches are
	 * tripped at the same time, once all processing has completed. this affords a barrier synchronization
	 * that should ensure atomicity and consistency at the time of direct write or commit.
	 * @param blockBuffer
	 * @param barrierCount
	 * @param barrierSynch
	 */
	public DirectBufferWriteRequest(MappedBlockBuffer blockBuffer, CountDownLatch barrierCount, CyclicBarrier barrierSynch) {
		this.blockBuffer = blockBuffer;
		this.barrierCount = barrierCount;
		this.barrierSynch = barrierSynch;
	}
	@Override
	public synchronized void process() throws IOException {
		blockBuffer.directBufferWrite();
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
		return "DirectBufferWriteRequest for tablespace "+tablespace;
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
