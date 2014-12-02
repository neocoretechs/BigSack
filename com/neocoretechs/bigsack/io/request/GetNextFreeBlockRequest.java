package com.neocoretechs.bigsack.io.request;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.pooled.Datablock;

public class GetNextFreeBlockRequest implements IoRequestInterface {
	private long nextFreeBlock = 0L;
	private IoInterface ioUnit;
	Datablock d = new Datablock(DBPhysicalConstants.DATASIZE);
	private int tablespace;
	private CountDownLatch barrierCount;
	public GetNextFreeBlockRequest(CountDownLatch barrierCount) {
		this.barrierCount = barrierCount;
	}
	
	@Override
	public synchronized void process() throws IOException {
		nextFreeBlock = getNextFreeBlock();
		barrierCount.countDown();
	}
	/**
	* Return the first available block that can be acquired for write
	* @param tblsp The tablespace
	* @return The block available as a real, not virtual, block in this tablespace
	* @exception IOException if IO problem
	*/
	private long getNextFreeBlock() throws IOException {
		long tsize = ioUnit.Fsize();
		nextFreeBlock  += (long) DBPhysicalConstants.DBLOCKSIZ;
		if (nextFreeBlock >= tsize) {
			// extend tablespace in pool-size increments
			long newLen = tsize + (long) (DBPhysicalConstants.DBLOCKSIZ
						* DBPhysicalConstants.DBUCKETS);
			ioUnit.Fset_length(newLen);
			while (tsize < newLen) {
				ioUnit.Fseek(tsize);
				d.write(ioUnit);
				tsize += (long) DBPhysicalConstants.DBLOCKSIZ;
			}
			ioUnit.Fforce(); // flush on block creation
		}
		return nextFreeBlock;
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
		return "GetNextFreeBlockRequest for tablespace "+tablespace;
	}

}
