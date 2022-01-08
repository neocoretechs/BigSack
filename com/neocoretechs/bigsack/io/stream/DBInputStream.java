package com.neocoretechs.bigsack.io.stream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;

import com.neocoretechs.bigsack.io.IoManagerInterface;
import com.neocoretechs.bigsack.io.MultithreadedIOManager;
import com.neocoretechs.bigsack.io.Optr;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.pooled.MappedBlockBuffer;
/*
* Copyright (c) 1998,2003, NeoCoreTechs
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
* InputStream reading directly from the DB blocks
* obviates need for intermediate byte array
* @author Groff
*/
public final class DBInputStream extends InputStream {
	public static boolean DEBUG = true;
	MappedBlockBuffer blockBuffer;
	BlockAccessIndex lbai;
	private DataInputStream DBInput = null;
	
	public DBInputStream(BlockAccessIndex tlbai, MappedBlockBuffer tsdbio) {
		if(tsdbio.getTablespace() != GlobalDBIO.getTablespace(tlbai.getBlockNum()))
			throw new RuntimeException("Tablespace "+tsdbio.getTablespace()+" to block "+GlobalDBIO.valueOf(tlbai.getBlockNum())+" mismatch for DBInputStream");
		lbai = tlbai;
		blockBuffer = tsdbio;
		lbai.setByteindex((short) 0);
		if(DEBUG)
			System.out.printf("%s.c'tor %s%n", this.getClass().getName(),lbai);
	}
	
	public DBInputStream(Optr optr, IoManagerInterface tsdbio) throws IOException {
		blockBuffer = tsdbio.getBlockBuffer(GlobalDBIO.getTablespace(optr.getBlock()));
		lbai = blockBuffer.findOrAddBlock(optr.getBlock(), true);
		lbai.setByteindex(optr.getOffset());
		if(DEBUG)
			System.out.printf("%s.c'tor %s%n", this.getClass().getName(),lbai);
	}
	
	public synchronized BlockAccessIndex getBlockAccessIndex() {
		return lbai;
	}
	
	public synchronized DataInputStream getDBInput() {
		assert(lbai != null) : "BlockStream has null BlockAccessIndex for tablespace:"+blockBuffer.getTablespace();
		if(DBInput == null)
			DBInput = new DataInputStream(this);
		return DBInput;
	}
	
	@Override
	public void close() throws IOException {
		if(lbai != null)
			lbai.decrementAccesses();
	}
	
	@Override
	/**
	 * Return the number available in this block, the bytesinuse - current, but ONLY for this block.<p/>
	 * Primary use case is to determine if we are going to advance to next block on subsequent read or
	 * determine of any data exists in this block. If we have zero bytes available this block, but a next block is linked
	 * return 1.
	 */
	public int available() {
		int navail = (lbai.getBlk().getBytesused() - lbai.getByteindex());
		if(navail < 0)
			navail = 0;
		if(navail == 0 && lbai.getBlk().getNextblk() != -1)
			navail = 1;
		if(DEBUG)
			System.out.printf("%s.available = %d %s%n", this.getClass().getName(),navail,lbai);
		return navail;
	}
	
	@Override
	public int read(byte[] b) throws IOException {
		int nread = read(b, 0, b.length);
		if(DEBUG)
			System.out.printf("%s.read %d %s%n",this.getClass().getName(),nread,lbai);
		return nread;
	}
		
	//Reads bytes from this byte-input stream into the specified byte array, starting at the given offset.
	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		int nread = readn(b, off, len);
		if(DEBUG)
			System.out.printf("%s.read %d offset %d %s%n",this.getClass().getName(),nread,off,lbai);
		return nread;
	}
		
	@Override
	public int read() throws IOException {
		int nread = readi();
		if(DEBUG)
			System.out.printf("%s.read %d %s%n",this.getClass().getName(),nread,lbai);
		return nread;
	}

	@Override
	public long skip(long len) throws IOException {
		if(DEBUG)
			System.out.printf("%s.skip %d %s%n",this.getClass().getName(),len,lbai);
		long actual = 0L;
		for(int i = 0; i < len; i++) {
				if(read() == -1)
					break;
				++actual;
		}
		return actual;
	}
	/**
	* seek_fwd - long seek forward from current spot. If we change blocks, notify the observers
	* @param offset offset from current
	* @exception IOException If we cannot acquire next block
	*/
	public synchronized boolean seek_fwd(BlockAccessIndex tbai, long offset) throws IOException {
		if(blockBuffer.getTablespace() != GlobalDBIO.getTablespace(tbai.getBlockNum()))
			throw new RuntimeException("Tablespace "+blockBuffer.getTablespace()+" to block "+GlobalDBIO.valueOf(tbai.getBlockNum())+" mismatch for DBInputStream");
		if(DEBUG)
			System.out.printf("%s.seek_fwd %d %s%n",this.getClass().getName(),offset,lbai);
		long runcount = offset;
		lbai = tbai;
		do {
			if (runcount >= (lbai.getBlk().getBytesused() - lbai.getByteindex())) {
				runcount -= (lbai.getBlk().getBytesused() - lbai.getByteindex());
				lbai = blockBuffer.getnextblk(lbai);
				if(lbai.getBlk().getNextblk() == -1)
					return false;
			} else {
				lbai.setByteindex((short) (lbai.getByteindex() + runcount));
				runcount = 0;
			}
		} while (runcount > 0);
		return true;
	
	}

	/**
	* readn - read n bytes from pool. will start on the passed BlockAccessIndex filling buf from offs until numbyte.
	* Records spanning blocks will be successively read until buffer is full
	* @param buf byte buffer to fill
	* @param numbyte number of bytes to read
	* @return number of bytes read, -1 if we reach end of stream and/or there is no next block
	* @exception IOException If we cannot acquire next block
	*/
	private synchronized int readn(byte[] buf, int offs, int numbyte) throws IOException {
		BlockAccessIndex tblk;
		int i = offs, runcount = numbyte, blkbytes;
		// see if we need the next block to start
		// and flag our position
		if (lbai.getByteindex() >= lbai.getBlk().getBytesused()-1)
			if((tblk=blockBuffer.getnextblk(lbai)) != null) {
				lbai=tblk;
			} else {
				return -1;
			}
		for (;;) {
			blkbytes = lbai.getBlk().getBytesused() - lbai.getByteindex();
			if (runcount > blkbytes) {
				runcount -= blkbytes;
				System.arraycopy(
					lbai.getBlk().getData(),
					lbai.getByteindex(),
					buf,
					i,
					blkbytes);
				lbai.setByteindex((short) (lbai.getByteindex() + (short)blkbytes));
				i += blkbytes;
				if ((tblk=blockBuffer.getnextblk(lbai)) != null) {
					lbai=tblk;
				} else {
					return (i != 0 ? (i-offs) : -1);
				}
			} else {
				System.arraycopy(
					lbai.getBlk().getData(),
					lbai.getByteindex(),
					buf,
					i,
					runcount);
				lbai.setByteindex((short) (lbai.getByteindex() + runcount));
				i += runcount;
				return (i != 0 ? (i-offs) : -1);
			}
		}
	}
	

	/**
	* readn - read n bytes from pool
	* @param buf byte buffer to fill
	* @param numbyte number of bytes to read
	* @return number of bytes read
	* @exception IOException If we cannot acquire next block
	*/
	public synchronized int read(BlockAccessIndex tbai, ByteBuffer buf, int numbyte) throws IOException {
		if(blockBuffer.getTablespace() != GlobalDBIO.getTablespace(tbai.getBlockNum()))
			throw new RuntimeException("Tablespace "+blockBuffer.getTablespace()+" to block "+GlobalDBIO.valueOf(tbai.getBlockNum())+" mismatch for DBInputStream");
		lbai = tbai;
		BlockAccessIndex tblk;
		int i = 0, runcount = numbyte, blkbytes;
		// see if we need the next block to start
		// and flag our position
		if (lbai.getByteindex() >= lbai.getBlk().getBytesused()-1)
			if((tblk=blockBuffer.getnextblk(lbai)) != null) {
				lbai=tblk;
			} else {
				return (i != 0 ? i : -1);
			}
		for (;;) {
			blkbytes = lbai.getBlk().getBytesused() - lbai.getByteindex();
			if (runcount > blkbytes) {
				runcount -= blkbytes;
				buf.position(i);
				buf.put(lbai.getBlk().getData(), lbai.getByteindex(), blkbytes);
				lbai.setByteindex((short) (lbai.getByteindex() + (short)blkbytes));
				i += blkbytes;
				if((tblk=blockBuffer.getnextblk(lbai)) != null) {
					lbai=tblk;
				} else {
					return (i != 0 ? i : -1);
				}
			} else {
				buf.position(i);
				buf.put(lbai.getBlk().getData(), lbai.getByteindex(), runcount);
				lbai.setByteindex((short) (lbai.getByteindex() + runcount));
				i += runcount;
				return (i != 0 ? i : -1);
			}
		}
	}
	/**
	* readi - read 1 byte from pool.
	* This method designed to be called from DBInput.
	* @return the byte as integer for InputStream
	* @exception IOException If we cannot acquire next block
	*/
	private synchronized int readi() throws IOException {
		BlockAccessIndex tblk;
		// see if we need the next block to start
		// and flag our position
		if (lbai.getByteindex() >= lbai.getBlk().getBytesused()-1) {
			if((tblk=blockBuffer.getnextblk(lbai)) == null) {
				return -1;
			}
			lbai = tblk;
		}
		int ret = lbai.getBlk().getData()[lbai.getByteindex()] & 255;
		lbai.setByteindex((short) (lbai.getByteindex() + 1));
		return ret;
	}
	
	@Override
	public synchronized String toString() {
		return "BlockStream for tablespace "+blockBuffer.getTablespace()+" with block "+(lbai == null ? "UNASSIGNED" : lbai)+" and blocks in buffer:"+blockBuffer.size();
	}

	public void setBlockAccessIndex(BlockAccessIndex bai) {
		if(blockBuffer.getTablespace() != GlobalDBIO.getTablespace(bai.getBlockNum()))
			throw new RuntimeException("Tablespace "+blockBuffer.getTablespace()+" to block "+GlobalDBIO.valueOf(bai.getBlockNum())+" mismatch for DBInputStream");
		lbai = bai;
		lbai.setByteindex((short) 0);
	}
	
	public void setBlockAccessIndex(BlockAccessIndex bai, short byteindex) {
		if(blockBuffer.getTablespace() != GlobalDBIO.getTablespace(bai.getBlockNum()))
			throw new RuntimeException("Tablespace "+blockBuffer.getTablespace()+" to block "+GlobalDBIO.valueOf(bai.getBlockNum())+" mismatch for DBInputStream");
		lbai = bai;	
		lbai.setByteindex(byteindex);
	}
}
