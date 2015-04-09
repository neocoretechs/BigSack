package com.neocoretechs.bigsack.io.pooled;
import java.io.IOException;
import java.io.Serializable;

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

	public BlockAccessIndex(boolean init) throws IOException {
		if(init) init();

	}
	/** This constructor used for setting search templates */
	public BlockAccessIndex() {
	}
	/** This method can be used for ThreadLocal post-init after using default ctor 
	 * @throws IOException if block superceded a block under write or latched 
	 * */
	public synchronized void init() throws IOException {
		setBlk(new Datablock(DBPhysicalConstants.DATASIZE));
	}
	
	public synchronized void resetBlock() {
		accesses = 0;
		byteindex = 0;
		blk.resetBlock();
	}
	
	public synchronized int getAccesses() {
		return accesses;
	}
	
	synchronized void addAccess() {
		++accesses;
		//if( accesses > 1 ) {
		//	System.out.println("BlockAccessIndex.addAccess access > 1 "+this);
		//	new Throwable().printStackTrace();
		//}
	}
	
	public synchronized int decrementAccesses() throws IOException {
		if( accesses == 1 && blk.isIncore() )
			return accesses;
		if (accesses > 0 )
			--accesses;
		//else {
			//if( blk.isIncore() ) {
				//System.out.println("Accesses to 0 with incore latched:"+this);
				//new Throwable().printStackTrace();
			//}
		//}
		return accesses;
	}
	
	public synchronized String toString() {
		String db = "BlockAccessIndex: ";
		db += " data "
			+ blk == null ?  "null block" : blk.toBriefString()
			+ " accesses:"
			+ accesses
			+ " byteindex:"
			+ byteindex
			+ " inLog:"
			+ blk.isInlog()
			+ "."
			;
		return db;
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
		if (blockNum < ((BlockAccessIndex) o).blockNum)
			return -1;
		if (blockNum > ((BlockAccessIndex) o).blockNum)
			return 1;
		return 0;
	}
	@Override
	public synchronized boolean equals(Object o) {
		return (blockNum == ((BlockAccessIndex) o).blockNum);
	}
	/**
	 * If the buffers are per tablespace and we are below 4 gig this should be unique
	 * and offer optimum distribution
	 */
	@Override
	public synchronized int hashCode() {
		return (int) (0xFFFFFFFF & blockNum);
	}
	public synchronized Datablock getBlk() {
		return blk;
	}
	/**
	 * Set the presumably latched blockaccessindex with the datablock of data.
	 * Check to make sure the previous block is not in danger 
	 * @param blk
	 */
	public synchronized void setBlk(Datablock blk) throws IOException {
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
