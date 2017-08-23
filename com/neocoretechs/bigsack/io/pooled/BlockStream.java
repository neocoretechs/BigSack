package com.neocoretechs.bigsack.io.pooled;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.neocoretechs.bigsack.io.stream.DBInputStream;

/**
 * This class encapsulates the BlockAccessIndex cursor blocks for each tablespace and the corresponding 
 * DB Streams to access them.
 * There is a simple paradigm at work here, we carry a single block access index in another class and use it
 * to cursor through the blocks as we access them. The BlockStream class has the BlockAccessIndex and DBStream
 * for each tablespace. The cursor window block is read and written from deep store and buffer pool.
 * These blocks occupy the shadow world between the buffer pool and deep store. We create them by taking from the
 * freechain and those blocks taken are backed by an actual page brought into the buffer pool. 
 * The pool page is blank though and the contents of these buffers are used to fill them.
 * @author jg
 *
 */
public final class BlockStream {
	private BlockAccessIndex lbai;
	// DataStream for DB I/O
	private DataInputStream DBInput = null;
	
	private MappedBlockBuffer blockIO;
	private int tablespace;
	public BlockStream(int tablespace, MappedBlockBuffer blockBuffer) throws IOException {
		this.blockIO = blockBuffer;
		this.tablespace = tablespace;
		lbai = new BlockAccessIndex(true);
	}
	public synchronized BlockAccessIndex getLbai() {
		return lbai;
	}
	/**
	 * Delegate addAcces to setBlockNumber of BlockAccessIndex
	 * @param lbai
	 */
	public synchronized void setLbai(BlockAccessIndex lbai) {
		this.lbai = lbai;
		this.lbai.byteindex = 0;
	}
	public synchronized DataInputStream getDBInput() {
		if( DBInput != null )
			try {
				DBInput.close();
			} catch (IOException e) {}
		if( lbai == null ) throw new RuntimeException("DBInputStream uninitialized for BlockStream tablespace "+tablespace+" db"+blockIO.getGlobalIO().getDBName());
		DBInput = new DataInputStream(new DBInputStream(lbai, blockIO));
		return DBInput;
	}
	
	@Override
	public synchronized String toString() {
		return "BlockStream for tablespace "+tablespace+" with block "+(lbai == null ? "UNASSIGNED" : lbai)+" stream:"+DBInput+" and blocks in buffer:"+blockIO.size();
	}

}
