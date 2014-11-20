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
* overwrite rules.  Used as entries in TreeMap that have the virtual
* block number blockNum as key
* @author Groff
*/
@SuppressWarnings("rawtypes")
public final class BlockAccessIndex implements Comparable, Serializable {
	private static final long serialVersionUID = -7046561350843262757L;
	private Datablock blk;
	private transient int accesses = 0;
	private long blockNum = -1L;
	protected short byteindex = -1;

	private transient GlobalDBIO globalIO;

	public BlockAccessIndex(GlobalDBIO globalIO) {
		setBlk(new Datablock(DBPhysicalConstants.DATASIZE));
		this.globalIO = globalIO;
	}
	/** This constructor used for setting search templates */
	public BlockAccessIndex() {
	}

	int getAccesses() {
		return accesses;
	}
	public void addAccess() {
		++accesses;
	}
	public int decrementAccesses() throws IOException {
		if (accesses > 0)
			--accesses;
		//if (accesses == 0 && blk.isIncore() && !blk.isInlog()) {
		//	globalIO.getUlog().writeLog(this); // wait for 'applyChange' method of 'Loggable' to push block to raw store
			//globalIO.FseekAndWrite(blockNum, blk);
		//}
		return accesses;
	}
	
	public String toString() {
		String db = "BlockAccessIndex: database ";
		if( globalIO != null ) {
			db += globalIO.getDBName()
					+ " block "
					+ GlobalDBIO.valueOf(blockNum);
		} else
			db += "NULL";
		db += " data "
			+ blk == null ?  "null block" : blk.toBriefString()
			+ " accesses: "
			+ accesses
			+ " byteindex "
			+ byteindex
			;
		return db;
	}

	public long getBlockNum() {
		return blockNum;
	}
	public void setBlockNum(long bnum) throws IOException {
		assert (blockNum != -1L);
		if (bnum == blockNum) {
				byteindex = 0;
				return;
		}
		// blocks not same and not first
		if (blk.isIncore() && !blk.isInlog()) {
				globalIO.getUlog().writeLog(this);
		}
		if (accesses > 0)
				throw new IOException("Attempt to read into allocated block "
						+ this
						+ " with "
						+ bnum);
		blockNum = bnum;
		byteindex = 0;
		globalIO.FseekAndRead(blockNum, blk);
	}
	/**
	* Used to locate elements in tables
	* @param tbn The template block number to search for
	*/
	public void setTemplateBlockNumber(long tbn) {
		blockNum = tbn;
	}
	
	public int compareTo(Object o) {
		if (blockNum < ((BlockAccessIndex) o).blockNum)
			return -1;
		if (blockNum > ((BlockAccessIndex) o).blockNum)
			return 1;
		return 0;
	}

	public boolean equals(Object o) {
		return (blockNum == ((BlockAccessIndex) o).blockNum);
	}
	public Datablock getBlk() {
		return blk;
	}
	public void setBlk(Datablock blk) {
		this.blk = blk;
	}
	public short getByteindex() {
		return byteindex;
	}
	public short setByteindex(short byteindex) {
		this.byteindex = byteindex;
		return byteindex;
	}
}
