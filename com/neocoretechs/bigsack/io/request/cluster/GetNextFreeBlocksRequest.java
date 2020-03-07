package com.neocoretechs.bigsack.io.request.cluster;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.pooled.Datablock;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
/**
 * This is a cluster parallel computation component of a tablespace wide request.
 * We forego the cyclicbarrier in favor of a countdownlatch that waits for responses from the
 * available masters, after they have received the results from each cluster node.
 * Copyright (C) NeoCoreTechs 2014
 * @author jg
 *
 */
public final class GetNextFreeBlocksRequest extends AbstractClusterWork implements CompletionLatchInterface, Serializable {

	private static final long serialVersionUID = 4504873855639883545L;
	private long nextFreeBlock = 0L;
	private transient IoInterface ioUnit;
	private Datablock d = new Datablock(DBPhysicalConstants.DATASIZE);
	private int tablespace;
	private transient CountDownLatch barrierCount;
	public GetNextFreeBlocksRequest(){}
	public GetNextFreeBlocksRequest(CountDownLatch barrierCount) {
		this.barrierCount = barrierCount;
	}
	@Override
	public void process() throws IOException {
		getNextFreeBlocks();
		barrierCount.countDown();
	}
	/**
	* Set the next free block position from reverse scan of blocks
	* next free will be set to -1 if there are no free blocks
	* @exception IOException if seek or size fails
	*/
	private void getNextFreeBlocks() throws IOException {
		synchronized(ioUnit) {
			// tablespace 0 end of rearward scan is block 2 otherwise 0, tablespace 0 has root node
			long endBlock = tablespace == 0 ? DBPhysicalConstants.DBLOCKSIZ : 0L;
			long endBl = ioUnit.Fsize();
			nextFreeBlock = -1L;
			while (endBl > endBlock) {
				ioUnit.Fseek(endBl - (long) DBPhysicalConstants.DBLOCKSIZ);
				d.read(ioUnit);
				if (d.getPrevblk() == -1L && d.getNextblk() == -1L && d.getBytesinuse() == 0) {
					endBl -= (long) DBPhysicalConstants.DBLOCKSIZ;
				} else {
					nextFreeBlock = endBl - DBPhysicalConstants.DBLOCKSIZ;
					break;
				}
			}
			if(nextFreeBlock != -1L) {
				nextFreeBlock = GlobalDBIO.makeVblock(tablespace, endBl);
			}
		}
	}
	@Override
	public long getLongReturn() {
		return nextFreeBlock;
	}

	@Override
	public Object getObjectReturn() {
		return new Long(nextFreeBlock);
	}
	/**
	 * This method is called by queueRequest to set the proper tablespace from IOManager 
	 * It is the default way to set the active IO unit
	 */
	@Override
	public void setIoInterface(IoInterface ioi) {
		this.ioUnit = ioi;	
	}
	/**
	 * This method also set by queueRequest
	 */
	@Override
	public void setTablespace(int tablespace) {
		this.tablespace = tablespace;
	}
	
	public String toString() {
		return getUUID()+",tablespace:"+tablespace+":GetNextFreeBlocksRequest:"+nextFreeBlock;
	}
	/**
	 * The latch will be extracted by the UDPMaster and when a response comes back it will be tripped
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
		nextFreeBlock = val;
	}

	@Override
	public void setObjectReturn(Object o) {
		nextFreeBlock = (Long) o;	
	}
	@Override
	public CyclicBarrier getCyclicBarrier() {
		return null;
	}
	@Override
	public void setCyclicBarrier(CyclicBarrier cb) {
	}
	@Override
	public boolean doPropagate() {
		return true;
	}

}
