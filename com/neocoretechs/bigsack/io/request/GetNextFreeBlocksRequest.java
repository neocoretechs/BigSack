package com.neocoretechs.bigsack.io.request;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.pooled.Datablock;
/**
 * This is an intent parallel computation component of a tablespace wide request.
 * We are using a CyclicBarrier set up with the number of tablepsaces and after each thread
 * computes the size of the tablespace it will await the barrier synch. 
 * Once released from barrier synch a countdown latch is decreased which activates the multi
 * threading IO manager countdown latch waiter when count reaches 0, thereby releasing the thread
 * to accumulate the results from each worker.
 * The barrier synch is present to achieve a consistent state upon the invocation of the 
 * final tally. 
 * Copyright (C) NeoCoreTechs 2014
 * @author jg
 *
 */
public final class GetNextFreeBlocksRequest implements IoRequestInterface {
	private long nextFreeBlock = 0L;
	private IoInterface ioUnit;
	private Datablock d = new Datablock(DBPhysicalConstants.DATASIZE);
	private CyclicBarrier barrierSynch;
	private int tablespace;
	private CountDownLatch barrierCount;
	public GetNextFreeBlocksRequest(CyclicBarrier barrierSynch, CountDownLatch barrierCount) {
		this.barrierSynch = barrierSynch;
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
		// wait at the barrier until all other tablespaces arrive at their result
		try {
			barrierSynch.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			throw new IOException(e);
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
		return "GetNextFreeBlocksRequest for tablespace "+tablespace;
	}

}
