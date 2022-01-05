package com.neocoretechs.bigsack.io.stream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.IoManagerInterface;
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
* OutputStream writing directly from the DB blocks
* obviates need for intermediate byte array
* @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
*/
public final class DBOutputStream extends OutputStream {
	private static boolean DEBUG = false;
	MappedBlockBuffer blockBuffer;
	BlockAccessIndex lbai;
	private DataOutputStream DBOutput = null;
	public DBOutputStream(BlockAccessIndex tlbai, MappedBlockBuffer tsdbio) {
		if(tsdbio.getTablespace() != GlobalDBIO.getTablespace(tlbai.getBlockNum()))
			throw new RuntimeException("Tablespace "+tsdbio.getTablespace()+" to block "+GlobalDBIO.valueOf(tlbai.getBlockNum())+" mismatch for DBOutputStream");
		lbai =  tlbai;
		lbai.setByteindex((short) 0);
		//tlbai.getBlk().setIncore(true);
		blockBuffer = tsdbio;
	}
	
	public DBOutputStream(Optr optr, IoManagerInterface tsdbio) throws IOException {
		blockBuffer = tsdbio.getBlockBuffer(GlobalDBIO.getTablespace(optr.getBlock()));
		lbai = blockBuffer.findOrAddBlock(optr.getBlock(), true);
		lbai.setByteindex(optr.getOffset());
		if(DEBUG)
			System.out.printf("%s.c'tor %s%n", this.getClass().getName(),lbai);
	}
	
	@Override
	public void write(byte[] b) throws IOException {
		writen(b, b.length);
	}
		
	//Reads bytes from this byte-input stream into the specified byte array, starting at the given offset.
	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		for(int i = off; i < len; i++)
			writei(b[i]);
	}
		
	@Override
	public void write(int b) throws IOException {
		writei(b);
	}
	
	@Override
	public void close() throws IOException {
		if(lbai != null)
			lbai.decrementAccesses();
	}
	
	public BlockAccessIndex getBlockAccessIndex() {
		return lbai;
	}
	/**
	* writen -  write n bytes to pool.  This
	* will overwrite to next block if necessary, or allocate from end
	* @param buf byte buffer to write
	* @param numbyte number of bytes to write
	* @return number of bytes written
	* @exception IOException if can't acquire new block
	*/
	private synchronized int writen(byte[] buf, int numbyte) throws IOException {
		int i = 0, runcount = numbyte, blkbytes;
		BlockAccessIndex tblk;
		// see if we need the next block to start
		// and flag our position, tblk has passed block or a new acquired one
		if (lbai.getByteindex() >= DBPhysicalConstants.DATASIZE) {
			if ((tblk=blockBuffer.getnextblk(lbai)) == null) { // no room in passed block, no next block, acquire one
				lbai = blockBuffer.acquireNewBlk(lbai);
			} else {
				lbai = tblk;
			}
		} 
		// Iterate, reducing the byte count in buffer by room in each block
		for (;;) {
			blkbytes = DBPhysicalConstants.DATASIZE - lbai.getByteindex();
			if(DEBUG)
				System.out.printf("Writing %d to tblk:%s%n",blkbytes, lbai);
			if (runcount > blkbytes) {  //overflow block
				runcount -= blkbytes;
				System.arraycopy(
					buf,
					i,
					lbai.getBlk().getData(),
					lbai.getByteindex(),
					blkbytes);
				lbai.setByteindex((short) (lbai.getByteindex() + (short)blkbytes));
				i += blkbytes;
				lbai.getBlk().setBytesused(DBPhysicalConstants.DATASIZE);
				//update control info
				lbai.getBlk().setBytesinuse(DBPhysicalConstants.DATASIZE);
				lbai.getBlk().setIncore(true);
				lbai.getBlk().setInlog(false);
				if((tblk=blockBuffer.getnextblk(lbai)) == null) { // no linked block to write into? get one
					lbai = blockBuffer.acquireNewBlk(lbai);
				} else {
					lbai = tblk;
				}
				// now tblk has next block in chain or new acquired block
			} else { // we can fit the remainder of buffer in this block
				System.arraycopy(
					buf,
					i,
					lbai.getBlk().getData(),
					lbai.getByteindex(),
					runcount);
				lbai.setByteindex((short) (lbai.getByteindex() + runcount));
				i += runcount;
				if (lbai.getByteindex() > lbai.getBlk().getBytesused()) {
					//update control info
					lbai.getBlk().setBytesused(lbai.getByteindex());
					lbai.getBlk().setBytesinuse(lbai.getBlk().getBytesused());
				}
				lbai.getBlk().setIncore(true);
				lbai.getBlk().setInlog(false);
				return i;
			}
		}
	}
	/**
	* writen -  write n bytes to pool.  This will overwrite to next block if necessary, or allocate from end
	* The blocks written have their 'inCore' property set to true and their 'inLog' property set to false.
	* The is used in the Seekable DB channel that moves data from store to pool
	* @param buf byte buffer to write
	* @param numbyte number of bytes to write
	* @return number of bytes written
	* @exception IOException if can't acquire new block
	*/
	public synchronized int write(BlockAccessIndex tbai, ByteBuffer buf, int numbyte) throws IOException {
		if(blockBuffer.getTablespace() != GlobalDBIO.getTablespace(tbai.getBlockNum()))
			throw new RuntimeException("Tablespace "+blockBuffer.getTablespace()+" to block "+GlobalDBIO.valueOf(tbai.getBlockNum())+" mismatch for DBOutputStream");
		lbai = tbai;
		BlockAccessIndex tblk = null;
		int i = 0, runcount = numbyte, blkbytes;
		// sets the incore to true and the inlog to false on both blocks
		// see if we need the next block to start
		// and flag our position, tblk has passed block or a new acquired one
		if (lbai.getByteindex() >= DBPhysicalConstants.DATASIZE) {
			if ((tblk=blockBuffer.getnextblk(lbai)) == null) { // no room in passed block, no next block, acquire one
				lbai = blockBuffer.acquireNewBlk(lbai);
			} else { // we have some room in the passed block
				lbai = tblk;
			}
		}
		//
		for (;;) {
			blkbytes = DBPhysicalConstants.DATASIZE - lbai.getByteindex();
			if(DEBUG)
				System.out.printf("Writing %d to tblk:%s buffer:%s%n",blkbytes, lbai, buf);
			if (runcount > blkbytes) {
				runcount -= blkbytes;
				buf.position(i);
				buf.get(lbai.getBlk().getData(), lbai.getByteindex(), blkbytes);
				lbai.setByteindex((short) (lbai.getByteindex() + (short)blkbytes));
				i += blkbytes;
				lbai.getBlk().setBytesused(DBPhysicalConstants.DATASIZE);
				//update control info
				lbai.getBlk().setBytesinuse(DBPhysicalConstants.DATASIZE);
				lbai.getBlk().setIncore(true);
				lbai.getBlk().setInlog(false);
				if ((tblk=blockBuffer.getnextblk(lbai)) == null) {
					lbai = blockBuffer.acquireNewBlk(lbai);
				} else {
					lbai = tblk;
				}
			} else {
				buf.position(i);
				buf.get(lbai.getBlk().getData(), lbai.getByteindex(), runcount);
				lbai.setByteindex((short) (lbai.getByteindex() + runcount));
				i += runcount;
				if (lbai.getByteindex() >= lbai.getBlk().getBytesused()) {
					//update control info
					lbai.getBlk().setBytesused(lbai.getByteindex());
					lbai.getBlk().setBytesinuse(lbai.getBlk().getBytesused());
				}
				lbai.getBlk().setIncore(true);
				lbai.getBlk().setInlog(false);
				return i;
			}
		}
	}

	/**
	* writei -  write 1 byte to pool.
	* This method designed to be called from DBOutput.
	* Will overwrite to next blk if necessary.
	* @param byte to write
	* @exception IOException If cannot acquire new block
	*/
	private synchronized void writei(int tbyte) throws IOException {
		BlockAccessIndex tblk;
		// see if we need the next block to start
		// and flag our position
		if (lbai.getByteindex() >= DBPhysicalConstants.DATASIZE) {
			if ((tblk=blockBuffer.getnextblk(lbai)) == null) { // no room in passed block, no next block, acquire one
				lbai = blockBuffer.acquireNewBlk(lbai);
			} else { // we have some room in the passed block
				lbai = tblk;
			}
		}
		if (!lbai.getBlk().isIncore())
			lbai.getBlk().setIncore(true);
		lbai.getBlk().getData()[lbai.getByteindex()] = (byte) tbyte;
		lbai.setByteindex((short) (lbai.getByteindex() + 1));
		if (lbai.getByteindex() > lbai.getBlk().getBytesused()) {
			//update control info
			lbai.getBlk().setBytesused( lbai.getByteindex()) ;
			lbai.getBlk().setBytesinuse(lbai.getBlk().getBytesused());
		}
	}

	/**
	* delete -  delete n bytes from object / directory. Item may span a block so we
	* adjust the pointers across block boundaries if necessary. Numerous sanity checks along the way.
	* @param osize number bytes to delete
	* @return true if success
	* @exception IOException If we cannot write block, or we attempted to seek past the end of a chain, or if the high water mark and total bytes used did not ultimately agree.
	*/
	public synchronized void delete(int osize) throws IOException {
		//System.out.println("MappedBlockBuffer.deleten:"+lbai+" size:"+osize);
		BlockAccessIndex tblk;
		int runcount = osize;
		if (osize <= 0)
			throw new IOException("Attempt to delete object with size invalid: " + osize);
		//
		// Handle the case where the entry we want to delete can be contained within one block
		// we are not altering the high water mark because the entry falls between the beginning and high water
		// and there may be another entry between it and high water
		//
		if( (((int)lbai.getByteindex()) + runcount) < ((int)lbai.getBlk().getBytesused())) {
			lbai.getBlk().setBytesinuse((short) (((int)(lbai.getBlk().getBytesinuse()) - runcount)) ); // reduce total bytes being used by delete amount
			// assertion did everything make sense at the end?
			if(lbai.getBlk().getBytesinuse() < 0)
				throw new IOException(this.toString() + " "+lbai+" negative bytesinuse from runcount:"+runcount+" delete size:"+osize);
			lbai.getBlk().setIncore(true);
			lbai.getBlk().setInlog(false);
			return;
		}
		//
		// The following case works for all contiguous chunks, however,it DOES NOT work for non contiguous chunks
		// where an entry is between the one to be deleted and high water mark. as in byteindex = 64 and osiz = 32
		// bytesused = 128 high water and bytesinuse = 64, 2 32 byte entries. in that case bspan = 96, dspan = 64
		// bytesused comes out 64 and bytesinuse comes out 0, and the entry after the one we want gone disappears.
		// that case should have been handled above.
		//
		do {
			//
			int bspan = ((int)lbai.getByteindex()) + runcount; // current delete amount plus start of delete
			int dspan = ((int)lbai.getBlk().getBytesused()) - ((int)lbai.getByteindex()); // (high water mark bytesused - index) total available to delete this page
			if( bspan < dspan ) { // If the total we want to delete plus start, does not exceed total this page, set to delete remaining runcount
				dspan = runcount;
			} else {
				// reduce bytesused by total this page, set high water mark back since we exceeded it
				lbai.getBlk().setBytesused( (short) (((int)(lbai.getBlk().getBytesused()) - dspan)) );
			}
			//System.out.println("runcount="+runcount+" dspan="+dspan+" bspan="+bspan);
			runcount = runcount - dspan; //reduce runcount by total available to delete this page
			lbai.getBlk().setBytesinuse((short) (((int)(lbai.getBlk().getBytesinuse()) - dspan)) ); // reduce total bytes being used by delete amount
			//
			// assertion did everything make sense at the end?
			if(lbai.getBlk().getBytesinuse() < 0)
				throw new IOException(this.toString() + " "+lbai+" negative bytesinuse from runcount:"+runcount+" delete size:"+osize);
			if(lbai.getBlk().getBytesused() < 0)
				throw new IOException(this.toString() + " "+lbai+" negative bytesused from runcount "+runcount+" delete size:"+osize);
			// high water mark 0, but bytes used for data is not, something went horribly wrong
			if(lbai.getBlk().getBytesinuse() == 0) { //if total bytes used is 0, reset high water mark to 0 to eventually reclaim block
				lbai.getBlk().setBytesused((short)0);
				lbai.setByteindex((short)0);	
			}
			//
			lbai.getBlk().setIncore(true);
			lbai.getBlk().setInlog(false);
			if(runcount > 0) { // if we have more to delete
				tblk = blockBuffer.getnextblk(lbai);
				// another sanity check
				if(tblk == null)
					throw new IOException(
						"Attempted delete past end of block chain for "+ osize + " bytes total, with remaining runcount "+runcount+" in "+ lbai);
				// we have to unlink this from the next block
				lbai = tblk;
				lbai.getBlk().setNextblk(-1L);
				lbai.setByteindex((short) 0);// start at the beginning of the next block to continue delete, or whatever
			}
		} while( runcount > 0); // while we still have more to delete
	}
	
	public synchronized DataOutputStream getDBOutput() {
		assert(lbai != null) : "BlockStream has null BlockAccessIndex for tablespace:"+blockBuffer.getTablespace();
		if(DBOutput == null)
			DBOutput = new DataOutputStream(this);
		return DBOutput;
	}
	
	@Override
	public synchronized String toString() {
		return "BlockStream for tablespace "+blockBuffer.getTablespace()+" with block "+(lbai == null ? "UNASSIGNED" : lbai)+" and blocks in buffer:"+blockBuffer.size();
	}

	public void setBlockAccessIndex(BlockAccessIndex bai) {
		if(blockBuffer.getTablespace() != GlobalDBIO.getTablespace(bai.getBlockNum()))
			throw new RuntimeException("Tablespace "+blockBuffer.getTablespace()+" to block "+GlobalDBIO.valueOf(bai.getBlockNum())+" mismatch for DBOutputStream");
		lbai = bai;
		lbai.setByteindex((short) 0);
	}
	
	public void setBlockAccessIndex(BlockAccessIndex bai, short index) {
		if(blockBuffer.getTablespace() != GlobalDBIO.getTablespace(bai.getBlockNum()))
			throw new RuntimeException("Tablespace "+blockBuffer.getTablespace()+" to block "+GlobalDBIO.valueOf(bai.getBlockNum())+" mismatch for DBOutputStream");
		lbai = bai;
		lbai.setByteindex(index);
	}
}
