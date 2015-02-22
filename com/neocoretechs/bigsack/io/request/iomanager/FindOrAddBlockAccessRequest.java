package com.neocoretechs.bigsack.io.request.iomanager;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.pooled.MappedBlockBuffer;
import com.neocoretechs.bigsack.io.request.cluster.CompletionLatchInterface;

public final class FindOrAddBlockAccessRequest implements CompletionLatchInterface {
	private int tablespace;
	private CountDownLatch barrierCount;
	private MappedBlockBuffer blockBuffer;
	private long block;
	private BlockAccessIndex returnObject;
	public FindOrAddBlockAccessRequest(MappedBlockBuffer blockBuffer, CountDownLatch barrierCount, long block) {
		this.blockBuffer = blockBuffer;
		this.barrierCount = barrierCount;
		this.block = block;
	}
	@Override
	public synchronized void process() throws IOException {
		returnObject = blockBuffer.findOrAddBlockAccess(block);
		barrierCount.countDown();
	}

	@Override
	public synchronized long getLongReturn() {
		return block;
	}

	@Override
	public synchronized Object getObjectReturn() {
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
	
	public synchronized String toString() {
		return "FindOrAddBlockAccessRequest for tablespace "+tablespace+" block "+GlobalDBIO.valueOf(block);
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

}
