package com.neocoretechs.bigsack.io.request.cluster;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.cluster.NodeBlockBuffer;
import com.neocoretechs.bigsack.io.cluster.NodeBlockBufferInterface;
import com.neocoretechs.bigsack.io.pooled.Datablock;
/**
 * Request to seek a block within a tablespace and write the contents of a block buffer
 * @author jg
 *
 */
public final class FSeekAndWriteFullyRequest extends AbstractClusterWork implements CompletionLatchInterface, Serializable {
	private static final long serialVersionUID = 7162415350039169751L;
	private transient IoInterface ioUnit;
	private long offset;
	private Datablock dblk;
	private int tablespace;
	private transient CountDownLatch barrierCount;
	private transient NodeBlockBuffer blockBuffer; //gets set via IOUnit
	public FSeekAndWriteFullyRequest(){}
	public FSeekAndWriteFullyRequest(CountDownLatch barrierCount, long offset, Datablock dblk) {
		this.barrierCount = barrierCount;
		this.offset = offset;
		this.dblk = dblk;
	}
	@Override
	public synchronized void process() throws IOException {
		// see if its buffered, set incore to latch it for write
		dblk.setIncore(true);
		blockBuffer.put(offset, dblk);
		// flip the latch, incore stays until block written
		barrierCount.countDown();
		ioUnit.Fseek(offset);
		dblk.write(ioUnit);
		dblk.setIncore(false);
	}
	@Override
	public synchronized long getLongReturn() {
		return offset;
	}

	@Override
	public synchronized Object getObjectReturn() {
		return dblk;
	}
	@Override
	public synchronized void setIoInterface(IoInterface ioi) {
		this.ioUnit = ioi;
		blockBuffer = ((NodeBlockBufferInterface)ioUnit).getBlockBuffer();
	}
	@Override
	public synchronized void setTablespace(int tablespace) {
		this.tablespace = tablespace;
	}
	public synchronized String toString() {
		return getUUID()+",tablespace:"+tablespace+"FSeekAndWriteFullyRequest:"+offset;
	}
	/**
	 * The latch will be extracted by the Master and when a response comes back it will be tripped
	 */
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
		offset = val;
	}

	@Override
	public void setObjectReturn(Object o) {
		dblk = (Datablock) o;	
	}
	@Override
	public CyclicBarrier getCyclicBarrier() {
		return null;
	}
	@Override
	public void setCyclicBarrier(CyclicBarrier cb) {
	}

}
