package com.neocoretechs.bigsack.io.request.cluster;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.pooled.Datablock;


public final class GetNextFreeBlockRequest extends AbstractClusterWork implements CompletionLatchInterface, Serializable {
	private static final long serialVersionUID = 6443933700951305345L;
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
	public synchronized void process() throws IOException {
		getNextFreeBlock();
		barrierCount.countDown();
	}
	/**
	* Return the first available block that can be acquired for write
	* @param tblsp The tablespace
	* @return The block available as a real, not virtual, block in this tablespace
	* @exception IOException if IO problem
	*/
	private void getNextFreeBlock() throws IOException {
		if( nextFreeBlock != -1) {
			nextFreeBlock  += (long) DBPhysicalConstants.DBLOCKSIZ;
			long tsize = ioUnit.Fsize();
			if (nextFreeBlock >= tsize) {
				// extend tablespace in pool-size increments
				long newLen = tsize + (long) (DBPhysicalConstants.DBLOCKSIZ * DBPhysicalConstants.DBUCKETS);
				ioUnit.Fset_length(newLen);
				while (tsize < newLen) {
					ioUnit.Fseek(tsize);
					d.write(ioUnit);
					tsize += (long) DBPhysicalConstants.DBLOCKSIZ;
				}
				ioUnit.Fforce(); // flush on block creation
			}
		} else {
			// no next free, extend tablespace and set next free to prev end
			long tsize = ioUnit.Fsize();
			nextFreeBlock = tsize;
			// extend tablespace in pool-size increments
			long newLen = tsize + (long) (DBPhysicalConstants.DBLOCKSIZ * DBPhysicalConstants.DBUCKETS);
			ioUnit.Fset_length(newLen);
			while(tsize < newLen) {
				ioUnit.Fseek(tsize);
				d.write(ioUnit);
				tsize += (long) DBPhysicalConstants.DBLOCKSIZ;
			}
			ioUnit.Fforce(); // flush on block creation
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
	@Override
	public synchronized void setTablespace(int tablespace) {
		this.tablespace = tablespace;
	}
	public synchronized String toString() {
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

}
