package com.neocoretechs.bigsack.io.request;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.pooled.Datablock;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
/**
 * Deal with virtual blocks and extract block
 * @author jg
 *
 */
public final class GetNextFreeBlockRequest implements IoRequestInterface {
	private static boolean DEBUG = false;
	private IoInterface ioUnit;
	Datablock d = new Datablock(DBPhysicalConstants.DATASIZE);
	private int tablespace;
	private long nextFreeBlock = 0L;
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
	* Return the first available block that can be acquired for write
	* @param tblsp The tablespace
	* @return The block available as a real, not virtual, block in this tablespace
	* @exception IOException if IO problem
	*/
	private void getNextFreeBlock() throws IOException {
		if( nextFreeBlock != -1) {
			long tblock = GlobalDBIO.getBlock(nextFreeBlock) + (long) DBPhysicalConstants.DBLOCKSIZ;
			nextFreeBlock = GlobalDBIO.makeVblock(tablespace, tblock);
			long tsize = ioUnit.Fsize();
			if( DEBUG )
				System.out.println("GetNextFreeBlockRequest FOUND:"+GlobalDBIO.valueOf(nextFreeBlock)+" size:"+tsize);
			if (GlobalDBIO.getBlock(nextFreeBlock) >= tsize) {
				// extend tablespace in pool-size increments
				long newLen = tsize + (long) (DBPhysicalConstants.DBLOCKSIZ * DBPhysicalConstants.DBUCKETS);
				if( DEBUG )
					System.out.println("GetNextFreeBlockRequest FOUND EXTEND:"+GlobalDBIO.valueOf(nextFreeBlock)+" size:"+tsize+" setting to "+newLen);
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
				System.out.println("GetNextFreeBlockRequest NO FREE, EXTEND:"+GlobalDBIO.valueOf(nextFreeBlock)+" size:"+tsize+" setting to "+newLen);
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
		return "GetNextFreeBlockRequest for tablespace "+tablespace+" next free block "+GlobalDBIO.valueOf(nextFreeBlock);
	}

}
