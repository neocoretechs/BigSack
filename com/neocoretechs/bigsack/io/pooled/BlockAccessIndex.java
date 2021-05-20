package com.neocoretechs.bigsack.io.pooled;
import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.neocoretechs.bigsack.DBPhysicalConstants;
/*
* Copyright (c) 2003, NeoCoreTechs
* All rights reserved.
* Redistribution and use in source and binary forms, with or without modification, 
* are permitted provided that the following conditions are met:
*
* Redistributions of source code must retain the above copyright notice, this list of
* conditions and the following disclaimer. 
* Redistributions in binary form must reproduce the above copyright notice, 
* this list of conditions and the following disclaimer in the documentation and/or
* other materials provided with the distribution. 
* Neither the name of NeoCoreTechs nor the names of its contributors may be 
* used to endorse or promote products derived from this software without specific prior written permission. 
* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED 
* WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A 
* PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR 
* ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
* TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
* HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT 
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED 
* OF THE POSSIBILITY OF SUCH DAMAGE.
*
*/
/**
* Holds the page block buffer for one block.  Also contains current
* read/write position for block.  Controls access and enforces
* overwrite rules.  Used as entries in buffer pool that have the virtual block number blockNum as key.
* We intentionally do not attach a tablespace because these entries can move around at will.
* @author Groff
*/
@SuppressWarnings("rawtypes")
public final class BlockAccessIndex implements Comparable, Serializable {
	private static boolean DEBUG = false;
	private static final long serialVersionUID = -7046561350843262757L;
	private Datablock blk;
	private transient int accesses = 0;
	private long blockNum = -1L;
	protected short byteindex = -1;
	public static long expiryTimeDelta = 5000;
	transient private long expiryTime; // 5000 ms cache expiration time
	//private transient ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	public BlockAccessIndex(boolean init) throws IOException {
		expiryTime = System.currentTimeMillis() + expiryTimeDelta;
		if(init) init();
	}
	/** This constructor used for setting search templates */
	public BlockAccessIndex() {
		expiryTime = System.currentTimeMillis() + expiryTimeDelta;
	}
    /**
     * Check cache expiration for SoftReference cache.<p/>
     * If expiration time delta is reached AND accesses = 0 AND block is not incore AND not root block
     * @return true if expired and can be removed from cache
     */
    public boolean isExpired() {
        return (System.currentTimeMillis() > expiryTime && accesses == 0 && !blk.isIncore() && blockNum != 0L);
    }
	/** This method can be used for ThreadLocal post-init after using default ctor 
	 * @throws IOException if block superceded a block under write or latched 
	 * */
	private synchronized void init() throws IOException {
		//if( lock.isWriteLocked() ) {
		//	throw new IOException("init() Attempt to unlock write locked Datablock "+this);
		//}
		//if( lock.getReadLockCount() > 0) {
		//	System.out.println("init() read lock count:"+lock.getReadLockCount());
		//	lock.readLock().unlock();
		//}
		setBlk(new Datablock(DBPhysicalConstants.DATASIZE));
	}
	
	/**
	 * Reset the block, set default block headers. Set byteindex to 0.
	 * @param clearAccess True to clear the accesses latch
	 */
	public synchronized void resetBlock(boolean clearAccess) {
		//if( lock.isWriteLocked() )
		//	lock.writeLock().unlock();
		//if( lock.getReadLockCount() > 0) {
		//	System.out.println("resetBlock() read lock count:"+lock.getReadLockCount());
		//	lock.readLock().unlock();
		//}
		if( clearAccess )
			accesses = 0;
		byteindex = 0;
		blk.resetBlock();
	}
	/**
	 * Get the number of accesses
	 * @return
	 */
	public synchronized int getAccesses() {
		return accesses;
	}
	/**
	 * Increment the access counter
	 */
	public synchronized void addAccess() {
		//if( !lock.isWriteLocked() )
		//		lock.writeLock().lock();
		++accesses;
		expiryTime = System.currentTimeMillis() + expiryTimeDelta;
		//if( accesses > 1 ) {
		//	System.out.println("BlockAccessIndex.addAccess access > 1 "+this);
		//	new Throwable().printStackTrace();
		//}
	}
	/**
	 * Decrement the accesses, if block is incore, under write, we wont allow accesses to go to zero.<p/>
	 * Neither will we allow accesses to go negative.
	 * @return the number of accesses after decrement, or after we reject decrement.
	 * @throws IOException
	 */
	public synchronized int decrementAccesses() throws IOException {
		if( accesses == 1 && blk.isIncore() )
			return accesses;
		if (accesses > 0 ) {
			--accesses;
		}
		//if( accesses == 0 && lock.isWriteLocked()) {
		//	if( DEBUG )
		//		System.out.println("BlockAccessIndex.decrementAccesses:"+lock+" "+Thread.currentThread()+" holds this lock:"+lock.isWriteLockedByCurrentThread()+" locks:"+lock.getWriteHoldCount()+" queue:"+lock.getQueueLength());
		//	lock.writeLock().unlock();
		//}
		//else {
			//if( blk.isIncore() ) {
				//System.out.println("Accesses to 0 with incore latched:"+this);
				//new Throwable().printStackTrace();
			//}
		//}
		return accesses;
	}
	
	public synchronized String toString() {
		StringBuilder db = new StringBuilder("BlockAccessIndex:");
		db.append(GlobalDBIO.valueOf(blockNum));
		db.append(" data ");
		db.append(blk == null ?  "null block" : blk.toBriefString());
		db.append(" accesses:");
		db.append(accesses);
		db.append(" byteindex:");
		db.append(byteindex);
		db.append(" inLog:");
		db.append(blk == null ?  "null block" : blk.isInlog());
		db.append(" Expires:");
		db.append(new Date(expiryTime));
		db.append(" is Expired:");
		db.append(isExpired());
		db.append(".");
		return db.toString();
	}

	public synchronized long getBlockNum() {
		return blockNum;
	}
	/**
	 * Set the block number and make sure we are not overwriting a previous entry that is active.
	 * It should have 1 access, it should not be incore or in the log. Setting an invalid block also fails assertion
	 * @param bnum
	 * @throws IOException
	 */
	public synchronized void setBlockNumber(long bnum) throws IOException {
		assert (bnum != -1L) : "****Attempt to set block number invalid";
	
		//if( GlobalDBIO.valueOf(bnum).equals("Tablespace_1_114688"))
		//	System.out.println("BlockAccessIndex.setBlockNum. Tablespace_1_114688");
		
		// add an access, latch immediately
		addAccess();
		// We are about to replace the current block, make sure it is not under write or latched by someone else
		// or we would be trashing data. We already latched it so there should be only 1
		if( accesses > 1 ) 
			throw new IOException("****Attempt to overwrite latched block, accesses "+accesses+" for buffer "+this+" with "+bnum);
		/*
		if (bnum == blockNum) {
				byteindex = 0;
				return;
		}
		// If its been written but not yet in log, write it
		
		if (blk.isIncore() && !blk.isInlog()) {
				globalIO.getUlog().writeLog(this);
		}
		 */
		blockNum = bnum;
		byteindex = 0;
	
		/*
		globalIO.getIOManager().FseekAndRead(blockNum, blk);
		
		//if( GlobalDBIO.valueOf(bnum).equals("Tablespace_1_114688"))
		//	System.out.println("BlockAccessIndex.setBlockNum. Tablespace_1_114688"+this+" "+blk.blockdump());
		assert(blk.getBytesinuse() > 0 && blk.getBytesused() > 0) : "BlockAccessIndex.setBlockNum unusable block returned from read at "+GlobalDBIO.valueOf(blockNum)+" "+blk.blockdump();
		*/
	}

	@Override
	public synchronized int compareTo(Object o) {
		return new Long(blockNum).compareTo(((BlockAccessIndex) o).blockNum);
	}
	
	@Override
	public synchronized boolean equals(Object o) {
		return (blockNum == ((BlockAccessIndex) o).blockNum);
	}

	@Override
	public synchronized int hashCode() {
		return (int) new Long(blockNum).hashCode();
	}
	
	public synchronized Datablock getBlk() {
		return blk;
	}
	/**
	 * Set the presumably latched blockaccessindex with the datablock of data.
	 * Check to make sure the previous block is not in danger 
	 * @param blk
	 */
	private synchronized void setBlk(Datablock blk) throws IOException {
		// blocks not same and not first, check for condition of the block we are replacing
		if( blk.isIncore() ) 
			throw new IOException("****Attempt to overwrite block in core for buffer "+this);
		blk.setIncore(false);
		blk.setInlog(false);
		// We are about to replace the current block, make sure it is not under write or latched by someone else
		// or we would be trashing data. We already latched it so there should be only 1
		if( accesses > 1 ) 
			throw new IOException("****Attempt to overwrite latched block, accesses "+accesses+" for buffer "+this);
		this.blk = blk;
	}
	public synchronized short getByteindex() {
		return byteindex;
	}
	public synchronized short setByteindex(short byteindex) {
		this.byteindex = byteindex;
		return byteindex;
	}
}
