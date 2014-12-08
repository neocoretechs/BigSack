package com.neocoretechs.bigsack.io.pooled;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.Optr;
import com.neocoretechs.bigsack.io.stream.DBInputStream;
import com.neocoretechs.bigsack.io.stream.DBOutputStream;

public class OffsetDBIO extends BlockDBIO implements OffsetDBIOInterface {
	private static final boolean DEBUG = false;
	// DataStream for DB I/O
	private DataInputStream DBInput;
	private DataOutputStream DBOutput;
	
	public OffsetDBIO(String objname, boolean create, long transId) throws IOException {
		super(objname, create, transId);
		DBInput = new DataInputStream(new DBInputStream(this));
		DBOutput = new DataOutputStream(new DBOutputStream(this));
	}
	
	public DataInputStream getDBInput() {
		return DBInput;
	}
	public DataOutputStream getDBOutput() {
		return DBOutput;
	}

	/**
	* seek_fwd - long seek forward from current spot
	* @param offset offset from current
	* @exception IOException If we cannot acquire next block
	*/
	public boolean seek_fwd(long offset) throws IOException {
		long runcount = offset;
		do {
			if (runcount >= (this.getBlk().getBytesused() - this.getByteindex())) {
				runcount -= (this.getBlk().getBytesused() - this.getByteindex());
				if (!getnextblk())
					return false;
			} else {
				moveByteindex((short)runcount);
				runcount = 0;
			}
		} while (runcount > 0);
		return true;
	}
	/**
	* seek_fwd - short seek forward from cur
	* @param offset offset from current
	* @exception IOException If we cannot acquire next block
	*/
	public boolean seek_fwd(short offset) throws IOException {
		short runcount = offset;
		do {
			if (runcount >= (this.getBlk().getBytesused() - this.getByteindex())) {
				runcount -= (this.getBlk().getBytesused() - this.getByteindex());
				if (!getnextblk())
					return false;
			} else {
				moveByteindex((short) runcount);
				runcount = 0;
			}
		} while (runcount > 0);
		return true;
	}
	/**
	* readn - read n bytes from pool
	* @param buf byte buffer to fill
	* @param numbyte number of bytes to read
	* @return number of bytes read
	* @exception IOException If we cannot acquire next block
	*/
	int readn(byte[] buf, int numbyte) throws IOException {
		int i = 0, runcount = numbyte, blkbytes;
		// see if we need the next block to start
		// and flag our position
		if (this.getByteindex() >= this.getBlk().getBytesused())
			if (!getnextblk())
				return i;
		for (;;) {
			blkbytes = this.getBlk().getBytesused() - this.getByteindex();
			if (runcount > blkbytes) {
				runcount -= blkbytes;
				System.arraycopy(
					this.getBlk().data,
					this.getByteindex(),
					buf,
					i,
					blkbytes);
				moveByteindex((short)blkbytes);
				i += blkbytes;
				if (!getnextblk())
					return i;
			} else {
				System.arraycopy(
					this.getBlk().data,
					this.getByteindex(),
					buf,
					i,
					runcount);
				moveByteindex((short)runcount);
				i += runcount;
				return i;
			}
		}
	}
	/**
	* readi - read 1 byte from pool.
	* This method designed to be called from DBInput.
	* @return the byte as integer for InputStream
	* @exception IOException If we cannot acquire next block
	*/
	public int readi() throws IOException {
		// see if we need the next block to start
		// and flag our position
		if (this.getByteindex() >= this.getBlk().getBytesused()) {
			if (getnextblk())
				return -1;
		}
		int ret = this.getBlk().data[this.getByteindex()] & 255;
		incrementByteindex();
		return ret;
	}
	/**
	* writen -  write n bytes to pool.  This
	* will overwrite to next block if necessary, or allocate from end
	* @param buf byte buffer to write
	* @param numbyte number of bytes to write
	* @return number of bytes written
	* @exception IOException if can't acquire new block
	*/
	int writen(byte[] buf, int numbyte) throws IOException {
		int i = 0, runcount = numbyte, blkbytes;
		// see if we need the next block to start
		// and flag our position
		if (this.getByteindex() >= DBPhysicalConstants.DATASIZE)
			if (!getnextblk())
				acquireBlock();
		//
		for (;;) {
			blkbytes = DBPhysicalConstants.DATASIZE - this.getByteindex();
			if (runcount > blkbytes) {
				runcount -= blkbytes;
				System.arraycopy(
					buf,
					i,
					this.getBlk().data,
					this.getByteindex(),
					blkbytes);
				this.moveByteindex( (short) blkbytes );
				i += blkbytes;
				this.getBlk().setBytesused(DBPhysicalConstants.DATASIZE);
				//update control info
				this.getBlk().setBytesinuse(DBPhysicalConstants.DATASIZE);
				this.getBlk().setIncore(true);
				this.getBlk().setInlog(false);
				if (!getnextblk())
					acquireBlock();
			} else {
				System.arraycopy(
					buf,
					i,
					this.getBlk().data,
					this.getByteindex(),
					runcount);
				this.moveByteindex((short)runcount);
				i += runcount;
				if (this.getByteindex() > this.getBlk().getBytesused()) {
					//update control info
					this.getBlk().setBytesused(this.getByteindex());
					this.getBlk().setBytesinuse(this.getBlk().getBytesused());
				}
				this.getBlk().setIncore(true);
				this.getBlk().setInlog(false);
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
	public void writei(int tbyte) throws IOException {
		// see if we need the next block to start
		// and flag our position
		if (this.getByteindex() >= DBPhysicalConstants.DATASIZE)
			if (!getnextblk())
				acquireBlock();
		if (!this.getBlk().isIncore())
			this.getBlk().setIncore(true);
		if (this.getBlk().isInlog())
			this.getBlk().setInlog(false);
		this.getBlk().data[this.getByteindex()] = (byte) tbyte;
		if (this.getByteindex() + 1 > this.getBlk().getBytesused()) {
			//update control info
			this.getBlk().setBytesused( this.getByteindex()) ;
			this.getBlk().setBytesinuse(this.getBlk().getBytesused());
		}
	}

	/**
	* deleten -  delete n bytes from object / directory
	* @param osize number bytes to delete
	* @return true if success
	* @exception IOException If we cannot write block
	*/
	boolean deleten(int osize) throws IOException {
		int runcount = osize;
		if (osize <= 0)
			throw new IOException("object size invalid: " + osize);
		long nextblk; //running count of object size,next
		for (;;) {
			// bytesused is high water mark, bytesinuse is # bytes occupied by data
			// we assume contiguous data
			// this case spans whole block or block to end
			//
			nextblk = this.getBlk().getNextblk();
			int bspan = (this.getBlk().getBytesused() - this.getByteindex());
			if (runcount >= bspan) {
				runcount -= bspan;
				this.getBlk().setBytesinuse((short) (this.getBlk().getBytesinuse() - bspan));
				// delete contiguously to end of block
				// byteindex is start of del entry
				// which is new high water byte count
				// since everything to end of block is going
				this.getBlk().setBytesused(this.getByteindex());
			} else {
				// we span somewhere in block to not end
				this.getBlk().setBytesinuse((short) (this.getBlk().getBytesinuse() - runcount));
				runcount = 0;
			}
			// assertion
			if (this.getBlk().getBytesinuse() < 0)
				throw new IOException(this.toString() + " negative bytesinuse "+this.getBlk().getBytesinuse()+" from runcount "+runcount);
			//
			this.getBlk().setIncore(true);
			this.getBlk().setInlog(false);
			//
			if (runcount > 0) {
				if (nextblk == -1L)
					throw new IOException(
						"attempted delete past end of chain for "
							+ osize
							+ " bytes in "
							+ getBlockIndex());
				findOrAddBlock(nextblk);
			} else
				break;

		}
		return true;
	}
	

	@Override
	public void setByteindex(short tindex) {
		getBlockIndex().setByteindex(tindex);
		
	}


}
