package com.neocoretechs.bigsack.io.request.cluster;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.CountDownLatch;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.pooled.Datablock;
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
	public synchronized void process() throws IOException {
		getNextFreeBlocks();
		barrierCount.countDown();
	}
	/**
	* Set the next free block position from reverse scan of blocks
	* next free will be set to -1 if there are no free blocks
	* @exception IOException if seek or size fails
	*/
	private void getNextFreeBlocks() throws IOException {
		long endBlock = 0L;
		long endBl = ioUnit.Fsize();
		nextFreeBlock = -1L; // assume there are none
		while (endBl > endBlock) {
				ioUnit.Fseek(endBl - (long) DBPhysicalConstants.DBLOCKSIZ);
				d.read(ioUnit);
				if (d.getPrevblk() == -1L
					&& d.getNextblk() == -1L
					&& d.getBytesused() == 0
					&& d.getBytesinuse() == 0) {
					endBl -= (long) DBPhysicalConstants.DBLOCKSIZ;
					continue;
				} else {
					// this is it
					nextFreeBlock = ioUnit.Ftell();// the read position at the end of the block that is used, the new block
					break;
				}
		}
	}
	@Override
	public synchronized long getLongReturn() {
		return nextFreeBlock;
	}

	@Override
	public synchronized Object getObjectReturn() {
		return new Long(nextFreeBlock);
	}
	/**
	 * This method is called by queueRequest to set the proper tablespace from IOManager 
	 * It is the default way to set the active IO unit
	 */
	@Override
	public synchronized void setIoInterface(IoInterface ioi) {
		this.ioUnit = ioi;	
	}
	/**
	 * This method also set by queueRequest
	 */
	@Override
	public synchronized void setTablespace(int tablespace) {
		this.tablespace = tablespace;
	}
	
	public synchronized String toString() {
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

}
