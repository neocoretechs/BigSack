package com.neocoretechs.bigsack.io.request;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.pooled.Datablock;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
/**
 * Deal with virtual blocks and extract block. Pass the previous free block from each
 * respective tablespace in the request constructor
 * @author jg
 *
 */
public final class GetNextFreeBlockRequest implements IoRequestInterface {
	private static boolean DEBUG = false;
	private IoInterface ioUnit;
	Datablock d = new Datablock(DBPhysicalConstants.DATASIZE);
	private int tablespace;
	private long nextFreeBlock = -1L;
	private CountDownLatch barrierCount;
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
		synchronized(ioUnit) {
		if( DEBUG )
			System.out.println("GetNextFreeBlockRequest CURRENT block:"+this);
		if( nextFreeBlock != -1L) {
			long tblock = GlobalDBIO.getBlock(nextFreeBlock) + (long) DBPhysicalConstants.DBLOCKSIZ;
			nextFreeBlock = GlobalDBIO.makeVblock(tablespace, tblock);
			long tsize = ioUnit.Fsize();
			if( DEBUG )
				System.out.println("GetNextFreeBlockRequest FOUND:"+this+" size:"+tsize);
			if (GlobalDBIO.getBlock(nextFreeBlock) >= tsize) {
				// extend tablespace in pool-size increments
				long newLen = tsize + (long) (DBPhysicalConstants.DBLOCKSIZ * DBPhysicalConstants.DBUCKETS);
				if( DEBUG )
					System.out.println("GetNextFreeBlockRequest FOUND EXTEND:"+this+" size:"+tsize+" setting to "+newLen);
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
			nextFreeBlock = GlobalDBIO.makeVblock(tablespace,tsize);
			// extend tablespace in pool-size increments
			long newLen = tsize + (long) (DBPhysicalConstants.DBLOCKSIZ * DBPhysicalConstants.DBUCKETS);
			if( DEBUG )
				System.out.println("GetNextFreeBlockRequest NO FREE, EXTEND:"+this+" size:"+tsize+" setting to "+newLen);
			ioUnit.Fset_length(newLen);
			while(tsize < newLen) {
				ioUnit.Fseek(tsize);
				d.write(ioUnit);
				tsize += (long) DBPhysicalConstants.DBLOCKSIZ;
			}
			ioUnit.Fforce(); // flush on block creation
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
	@Override
	public void setTablespace(int tablespace) {
		this.tablespace = tablespace;
	}
	public String toString() {
		return "GetNextFreeBlockRequest for "+ioUnit.Fname()+" tablespace "+tablespace+" next free block "+(nextFreeBlock == -1 ? "Empty" : GlobalDBIO.valueOf(nextFreeBlock));
	}

}
