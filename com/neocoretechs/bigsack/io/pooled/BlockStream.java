package com.neocoretechs.bigsack.io.pooled;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.neocoretechs.bigsack.io.stream.DBInputStream;
import com.neocoretechs.bigsack.io.stream.DBOutputStream;

/**
 * This class encapsulates the BlockAccessIndex cursor which is the corresponding DB Streams to access block bytes as data.
 * The BlockStream class has the BlockAccessIndex and DBInputStream and DBOutputStream which are stream subclasses
 * that point into the block bytes. We can use them to read blocks as serialized objects or construct 
 * for each block. 
 * @author jg
 *
 */
public final class BlockStream implements BlockChangeEvent{
	private BlockAccessIndex lbai = null;
	// DataStream for DB I/O
	private DataInputStream DBInput = null;
	private DataOutputStream DBOutput = null;
	private DBInputStream dbInputStream = null;
	private DBOutputStream dbOutputStream = null;
	
	private MappedBlockBuffer blockIO;
	private int tablespace;
	public BlockStream(int tablespace, MappedBlockBuffer blockBuffer) throws IOException {
		this.blockIO = blockBuffer;
		this.tablespace = tablespace;
	}
	public synchronized BlockAccessIndex getBlockAccessIndex() {
		//assert(lbai != null) : "BlockStream has null BlockAccessIndex for tablespace:"+tablespace+" and "+blockIO;
		return lbai;
	}
	/**
	 * Set the current BlockAccessIndex block, reset byteindex of block to 0; 
	 * @param lbai
	 */
	public synchronized void setBlockAccessIndex(BlockAccessIndex lbai) {
		this.lbai = lbai;
		this.lbai.byteindex = 0;
		if( dbInputStream == null )
			dbInputStream = new DBInputStream(this.lbai, this.blockIO);
		else
			dbInputStream.replaceSource(this.lbai, this.blockIO);
		if( dbOutputStream == null )
			dbOutputStream = new DBOutputStream(this.lbai, this.blockIO);
		else
			dbOutputStream.replaceSource(this.lbai, this.blockIO);
	}
	
	public synchronized DataInputStream getDBInput() {
		assert(lbai != null) : "BlockStream has null BlockAccessIndex for tablespace:"+tablespace+" and "+blockIO;
		DBInput = new DataInputStream(dbInputStream);
		return DBInput;
	}
	
	public synchronized DataOutputStream getDBOutput() {
		assert(lbai != null) : "BlockStream has null BlockAccessIndex for tablespace:"+tablespace+" and "+blockIO;
		DBOutput = new DataOutputStream(dbOutputStream);
		return DBOutput;
	}
	
	@Override
	public synchronized String toString() {
		return "BlockStream for tablespace "+tablespace+" with block "+(lbai == null ? "UNASSIGNED" : lbai)+" and blocks in buffer:"+blockIO.size();
	}
	
	@Override
	public synchronized void blockChanged(int tablespace, BlockAccessIndex bai) {
		assert(this.tablespace == tablespace) : "Wrong BlockStream instance notified of BlockChangeEvent from BlockChangeEvent observable";
		setBlockAccessIndex(bai);
	}

}
