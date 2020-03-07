package com.neocoretechs.bigsack.io.request.cluster;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.pooled.Datablock;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;


public final class GetNextFreeBlockRequest extends AbstractClusterWork implements CompletionLatchInterface, Serializable {
	private static final long serialVersionUID = 6443933700951305345L;
	private static final boolean DEBUG = false;
	private transient IoInterface ioUnit;
	Datablock d = new Datablock(DBPhysicalConstants.DATASIZE);
	private int tablespace;
	private long nextFreeBlock = 0L;
	private transient CountDownLatch barrierCount;
	public GetNextFreeBlockRequest(){}
	public GetNextFreeBlockRequest(CountDownLatch barrierCount, long prevFreeBlk) {
		this.barrierCount = barrierCount;
		nextFreeBlock = prevFreeBlk;
	}
	
	@Override
	public void process() throws IOException {
		getNextFreeBlock();
		barrierCount.countDown();
	}

	/**
	* Return the first available block that can be acquired for write. Use the previous free block as guide unless
	* its not initialized, ie, not -1. Take the next physical block after the previous one, if that one is eligible
	* @param tblsp The tablespace
	* @return The block available as a real, not virtual, block in this tablespace
	* @exception IOException if IO problem
	*/
	private void getNextFreeBlock() throws IOException {
		long newLen = 0;
		long tsize = 0;
		synchronized(ioUnit) {
			tsize = ioUnit.Fsize();
			if( DEBUG )
				System.out.println("GetNextFreeBlockRequest CURRENT block:"+this);
			// nextFreeBlock set to previous free block in ctor
			if( nextFreeBlock != -1L) {
				// we have a good previous 'next block', so attempt to move forward from that
				long tblock = GlobalDBIO.getBlock(nextFreeBlock) + (long) DBPhysicalConstants.DBLOCKSIZ;
				nextFreeBlock = GlobalDBIO.makeVblock(tablespace, tblock);
				// If the new address exceeds the end, extend, otherwise we are done
				if(GlobalDBIO.getBlock(nextFreeBlock) >= tsize) {
					// extend tablespace in pool-size increments
					newLen = tsize + (long) (DBPhysicalConstants.DBLOCKSIZ * DBPhysicalConstants.DBUCKETS);
					if( DEBUG )
						System.out.println("GetNextFreeBlockRequest next free block to EXTEND:"+this+" from size:"+tsize+" to "+newLen);
				} else {
					if( DEBUG )
						System.out.println("GetNextFreeBlockRequest EXIT with:"+this);
					return;
				}
			// We never had a good block here, do a rearward scan to find end of data
			} else {
				// try a backward scan of tablespace to find one
				// tablespace 0 end of rearward scan is block 2 otherwise 0, tablespace 0 has root node
				long endBlock = tablespace == 0 ? DBPhysicalConstants.DBLOCKSIZ : 0L;
				long endBl = tsize;
				while (endBl > endBlock) {
					ioUnit.Fseek(endBl - (long) DBPhysicalConstants.DBLOCKSIZ);
					d.read(ioUnit);
					if (d.getPrevblk() == -1L && d.getNextblk() == -1L && (d.getBytesinuse() == 0 || d.getBytesused() == 0)) {
						endBl -= (long) DBPhysicalConstants.DBLOCKSIZ;
					} else {
						nextFreeBlock = endBl;
						break;
					}
				}
				// If we came out with a valid block, we have only to make it a Vblock and we are finished.
				// If we made it back to the beginning, there is no data so set to our endblock
				if( endBl == endBlock) {
					nextFreeBlock = endBlock;
				}
				if(nextFreeBlock != -1L) {
					nextFreeBlock = GlobalDBIO.makeVblock(tablespace, nextFreeBlock);
					if( DEBUG )
						System.out.println("GetNextFreeBlockRequest exiting with:"+this);
					return;
				}
				// no next free, extend tablespace and set next free to prev end
				nextFreeBlock = GlobalDBIO.makeVblock(tablespace,tsize);
				// extend tablespace in pool-size increments
				newLen = tsize + (long) (DBPhysicalConstants.DBLOCKSIZ * DBPhysicalConstants.DBUCKETS);
			}
			if( DEBUG )
				System.out.println("GetNextFreeBlockRequest NO FREE, EXTEND:"+this+" size:"+tsize+" setting to "+newLen);
			ioUnit.Fset_length(newLen);
			// set next free right after end of old, in the first of our new space
			nextFreeBlock = GlobalDBIO.makeVblock(tablespace, tsize + DBPhysicalConstants.DBLOCKSIZ);
			// write new extended buffer blocks
			while (tsize < newLen) {
				ioUnit.Fseek(tsize);
				d.write(ioUnit);
				tsize += (long) DBPhysicalConstants.DBLOCKSIZ;
			}
			ioUnit.Fforce(); // flush on block creation
			// new block should be the on right after the old end
			if( DEBUG )
				System.out.println("GetNextFreeBlockRequest exiting with:"+this);
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
	@Override
	public void setTablespace(int tablespace) {
		this.tablespace = tablespace;
	}
	public String toString() {
		return getUUID()+",tablespace:"+tablespace+"GetNextFreeBlockRequest:"+nextFreeBlock;
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
