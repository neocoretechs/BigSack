package com.neocoretechs.bigsack.io.channel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.pooled.OffsetDBIOInterface;
/**
 * This class bridges the block pool and the computational elements.
 * It functions as a bytechannel into the block pool to read and write 
 * pages stored there. The primary storage buffers are ByteBuffer. The serialization
 * of objects is performed by acquiring streams through the 'Channels' nio class.
 * @author jg
 *
 */
public final class DBSeekableByteChannel implements SeekableByteChannel {
	private static boolean DEBUG = false;
	private OffsetDBIOInterface sdbio;
	private int tablespace;
	private long blockNum;
	private int position = 0;
	private long size = 0;
	public DBSeekableByteChannel(OffsetDBIOInterface sdbio, int tablespace) {
		this.sdbio = sdbio;
		this.tablespace = tablespace;
	}
	/**
	 * Our block number is a virtual block, we need to go back through the
	 * request chain for many operations so we stick with a vblock
	 * @param bnum
	 * @throws IOException
	 */
	public void setBlockNumber(long bnum) throws IOException {
		if( DEBUG ) {
			System.out.println("DBSeekableByteChannel set block to "+bnum+" "+GlobalDBIO.valueOf(bnum));
		}
		assert(GlobalDBIO.getTablespace(bnum) == tablespace) : "Byte channel requested for conflicting tablespace "+GlobalDBIO.valueOf(bnum)+" "+tablespace;
		this.blockNum = bnum;
		sdbio.objseek(blockNum);
		position = 0;
		size = 0;
	}
	
	public int getTablespace() { return tablespace; }
	
	@Override
	public void close() throws IOException {
		size = 0;
		position = 0;
	}

	@Override
	public boolean isOpen() {
		return true;
	}

	@Override
	public long position() throws IOException {
		return position;
	}

	@Override
	public SeekableByteChannel position(long arg0) throws IOException {
		sdbio.objseek(blockNum);
		position = (int) arg0;
		if( DEBUG )
			System.out.println("DBSeekableBytechannel position invoked for "+this);
		if( position > 0 )
				sdbio.seek_fwd(position);
		return this;
	}

	@Override
	public int read(ByteBuffer arg0) throws IOException {
		sdbio.objseek(blockNum);
		sdbio.seek_fwd(position);
		size = sdbio.readn(arg0, arg0.limit());
		if( DEBUG )
			System.out.print("/// DBSeekableByteChannel read:"+this+" into "+arg0+" ///");
		position += size;
		return (int) (size == 0 ? -1: size);
	}

	@Override
	public long size() throws IOException {
		if( DEBUG )
			System.out.println("DBSeekableBytechannel size invoked for "+this);
		return size;
	}

	@Override
	public SeekableByteChannel truncate(long arg0) throws IOException {
		if( DEBUG )
			System.out.println("DBSeekableBytechannel truncate invoked for "+this+" to "+arg0);
		throw new IOException("not supported");
	}

	@Override
	public int write(ByteBuffer arg0) throws IOException {
		if( DEBUG )
			System.out.println("DBSeekableBytechannel write invoked for "+this+" from "+arg0);
		size = sdbio.writen(arg0, arg0.limit());
		position += size;
		return (int) size;
	}
	
	@Override
	public String toString() {
		return "DBSeekableByteChannel block:"+GlobalDBIO.valueOf(blockNum)+" local pos:"+position+" local size:"+size+" tablespace:"+tablespace+" io:"+sdbio.toString();
	}

}
