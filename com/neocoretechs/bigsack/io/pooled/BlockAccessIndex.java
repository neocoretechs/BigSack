package com.neocoretechs.bigsack.io.pooled;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

import java.util.Date;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.btree.BTNode;
import com.neocoretechs.bigsack.btree.BTreeKeyPage;
import com.neocoretechs.bigsack.btree.BTreeRootKeyPage;
import com.neocoretechs.bigsack.hashmap.HMapChildRootKeyPage;
import com.neocoretechs.bigsack.hashmap.HMapKeyPage;
import com.neocoretechs.bigsack.hashmap.HMapMain;
import com.neocoretechs.bigsack.hashmap.HMapRootKeyPage;
import com.neocoretechs.bigsack.io.Optr;
import com.neocoretechs.bigsack.keyvaluepages.KeyPageInterface;
import com.neocoretechs.bigsack.keyvaluepages.KeyValue;
import com.neocoretechs.bigsack.keyvaluepages.NodeInterface;
import com.neocoretechs.bigsack.keyvaluepages.RootKeyPageInterface;
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
* Class instances reside in the managed buffer pool via soft reference and undergo expiration and access control.<p/>
* This class provides the link between deep store and the object model for the key/value instances.<p/>
* Holds the page block buffer for one block. The primary payload is in an instance of {@link Datablock}.<p/> 
* Maintains the index that indicates the read/write position for the block.<p/>  Controls access and enforces
* overwrite rules.<p/>  Used as entries in buffer pool that have the virtual block number blockNum as key.
* The virtual block number contains the tablespace in the 3 highest bits and the other 29 bits hold the file byte position.<p/>
* This class contains a number of static factory methods to deliver wrappers of instances of this class 
* for the various key/value store implementations.<p/>
* @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
*/
@SuppressWarnings("rawtypes")
public final class BlockAccessIndex implements Comparable, Serializable {
	private static final long serialVersionUID = -7046561350843262757L;
	private static boolean DEBUG = false;
	//private boolean DEBUGPUTKEY;
	//private boolean DEBUGPUTDATA;
	private Datablock blk;
	private transient int accesses = 0;
	private long blockNum = -1L;
	protected short byteindex = -1;
	public static long expiryTimeDelta = 60000;
	transient private long expiryTime; // ms cache expiration time
	//private transient ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	transient private GlobalDBIO sdbio;
	/**
	 * If init is true create a new {@link Datablock} and sets this to the new block.
	 * @param sdbio
	 * @param init
	 * @throws IOException
	 */
	protected BlockAccessIndex(GlobalDBIO sdbio, boolean init) throws IOException {
		this.setSdbio(sdbio);
		startExpired();
		if(init) init();
	}
	/** This constructor used for setting search templates */
	protected BlockAccessIndex(GlobalDBIO sdbio) {
		this.setSdbio(sdbio);
		startExpired();
	}
	/**
	 * Constructor for free block accumulation
	 * @param sdbio
	 * @param blockNum
	 * @param blk
	 */
	protected BlockAccessIndex(GlobalDBIO sdbio, long blockNum, Datablock blk) {
		this.setSdbio(sdbio);
		this.blockNum = blockNum;
		this.blk = blk;
		startExpired();
	}
    /**
     * Check cache expiration for SoftReference cache.<p/>
     * If expiration time delta is reached AND accesses = 0 AND AND not root block, block is expired.<p/>
     * If the block is in core (updated) and not in undo log, create a log entry before we expire this block.<p/>
     * If we write a log entry, inlog is set true and incore is set false.
     * @return true if expired and can be removed from cache
     */
    public boolean isExpired() {
        if(System.currentTimeMillis() > expiryTime && !blk.isIncore() && accesses == 0 && blockNum != 0L) { // dont expire tablespace 0, block 0
        	return true;
        }
        return false;
    }
    /**
     * When we move from free block list to active block list, start the timer for block flush
     */
    public void startExpired() {
    	expiryTime = System.currentTimeMillis() + expiryTimeDelta;
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
	 * Since our constructors are protected to limit creation of pages to buffer pool, 
	 * this special factory method is used to aid the recovery manager.
	 * @param globalIO
	 * @return
	 * @throws IOException 
	 */
	public static BlockAccessIndex createPageForRecovery(GlobalDBIO globalIO) throws IOException {
		return new BlockAccessIndex(globalIO, true);
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
		if( accesses == 0)
			expiryTime = System.currentTimeMillis() + expiryTimeDelta;
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
		StringBuilder db = new StringBuilder("BlockAccessIndex page:");
		db.append(GlobalDBIO.valueOf(blockNum));
		db.append(" data ");
		db.append(blk == null ?  "null block" : blk.toBriefString());
		db.append(" accesses:");
		db.append(accesses);
		db.append(" byteindex:");
		db.append(byteindex);
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

	
	/**
	 * Get the stream from the buffer pool for the blockNum in 'this'.
	 * @return The DataInputStream to read from buffer pool block.
	 */
	public DataInputStream getDBStream() {
		return GlobalDBIO.getBlockInputStream(this);
	}
	/**
	 * Ensure this block is set to updated for storage upon flush or commit
	 */
	public void setUpdated() {
		blk.setIncore(true);
		// We are about to replace the current block, make sure it is not under write or latched by someone else
		// or we would be trashing data. We already latched it so there should be only 1
		if( accesses == 0 ) 
			addAccess();	
	}
	
	public boolean isUpdated() {
		return blk.isIncore();
	}
	/**
	 * @return the sdbio
	 */
	public GlobalDBIO getSdbio() {
		return sdbio;
	}
	/**
	 * @param sdbio the sdbio to set
	 */
	public void setSdbio(GlobalDBIO sdbio) {
		this.sdbio = sdbio;
	}

	
}
