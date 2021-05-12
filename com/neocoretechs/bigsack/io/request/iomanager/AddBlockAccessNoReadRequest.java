package com.neocoretechs.bigsack.io.request.iomanager;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.MappedBlockBuffer;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.request.cluster.CompletionLatchInterface;
/**
 * This is an IoManager based request which will call into the block buffer for this
 * tablespace and bring a block into the pool without connecting it to any data.
 * The scenario is present in block acquisition
 * @author jg
 *
 */
public final class AddBlockAccessNoReadRequest implements CompletionLatchInterface {
	private int tablespace;
	private CountDownLatch barrierCount;
	private MappedBlockBuffer blockBuffer;
	private long block;
	private BlockAccessIndex returnObject;
	public AddBlockAccessNoReadRequest(MappedBlockBuffer blockBuffer, CountDownLatch barrierCount, long block) {
		this.blockBuffer = blockBuffer;
		this.barrierCount = barrierCount;
		this.block = block;
	}
	/**
	 * addBlockAccessNoRead will latch through setBlockNumber of blockAccessIndex
	 */
	@Override
	public void process() throws IOException {
		returnObject = blockBuffer.addBlockAccessNoRead(block);
		// trip the countdown latch in waiting processor thread
		barrierCount.countDown();
	}

	@Override
	public long getLongReturn() {
		return block;
	}

	@Override
	public Object getObjectReturn() {
		return returnObject;
	}
	/**
	 * This interface implemented method is called by IoWorker before processing
	 */
	@Override
	public void setIoInterface(IoInterface ioi) {}
	@Override
	public void setTablespace(int tablespace) {
		this.tablespace = tablespace;
	}
	
	public String toString() {
		return "AddBlockAccessNoReadRequest for tablespace "+tablespace+" block "+GlobalDBIO.valueOf(block);
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
		block = val;
	}
	@Override
	public void setObjectReturn(Object o) {
		returnObject = (BlockAccessIndex) o;
	}
	@Override
	public CyclicBarrier getCyclicBarrier() {
		return null;
	}
	@Override
	public void setCyclicBarrier(CyclicBarrier cb) {
	}

}
