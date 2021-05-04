package com.neocoretechs.bigsack.io.channel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
//import java.nio.channels.SeekableByteChannel;

import com.neocoretechs.bigsack.io.MultithreadedIOManager;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;

/**
 * This class bridges the block pool and the computational elements.
 * It functions as a bytechannel into the block pool to read and write 
 * pages stored there. The primary storage buffers are ByteBuffer. The serialization
 * of objects is performed by acquiring streams through the 'Channels' nio class.
 * @author jg
 *
 */
public final class DBSeekableByteChannel implements ByteChannel {
	private static boolean DEBUG = false;
	private MultithreadedIOManager sdbio;
	private BlockAccessIndex lbai;
	private long blockNum;
	private int position = 0;
	private long size = 0;
	public DBSeekableByteChannel(BlockAccessIndex tlbai, MultithreadedIOManager tsdbio) {
		lbai = tlbai;
		sdbio = tsdbio;
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
		//assert(GlobalDBIO.getTablespace(bnum) == ) : "Byte channel requested for conflicting tablespace "+GlobalDBIO.valueOf(bnum)+" "+tablespace;
		this.blockNum = bnum;
		sdbio.objseek(blockNum);
		position = 0;
		size = 0;
	}
	
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
	public int read(ByteBuffer arg0) throws IOException {
		int tablespace = GlobalDBIO.getTablespace(blockNum);
		sdbio.objseek(blockNum);
		sdbio.seek_fwd(tablespace, position);
		size = sdbio.getBlockBuffer(tablespace).readn(lbai, arg0, arg0.limit());
		if( DEBUG )
			System.out.print("/// DBSeekableByteChannel read:"+this+" into "+arg0+" ///");
		position += size;
		return (int) (size == 0 ? -1: size);
	}
	
	@Override
	public String toString() {
		return "DBSeekableByteChannel block:"+GlobalDBIO.valueOf(blockNum)+" local pos:"+position+" local size:"+size+" tablespace:"+lbai+" io:"+sdbio.toString();
	}
	@Override
	public int write(ByteBuffer arg0) throws IOException {
		throw new IOException("Non writeable channel");
	}

}
