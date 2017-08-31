package com.neocoretechs.bigsack.io.request;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.pooled.Datablock;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
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
	private final static boolean DEBUG = true;
	private long nextFreeBlock = -1L;
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
	public void process() throws IOException {
		long stime = System.currentTimeMillis();
		if( DEBUG ) {
			System.out.println("GetNextFreeBlocksRequest "+this+" start. ioUnit:"+ioUnit);
		}
		getNextFreeBlocks();
		if( DEBUG ) {
			System.out.println("GetNextFreeBlocksRequest "+this+" end in "+(System.currentTimeMillis()-stime)+" ms.");
		}
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
		// wait at the barrier until all other tablespaces arrive at their result
		try {
			barrierSynch.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			throw new IOException(e);
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
		return "GetNextFreeBlocksRequest for tablespace "+tablespace+" "+barrierSynch+" "+barrierCount+" next free block "+GlobalDBIO.valueOf(nextFreeBlock);
	}

}
