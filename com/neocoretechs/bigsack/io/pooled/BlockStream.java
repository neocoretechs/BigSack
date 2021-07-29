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
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2013,2021
 *
 */
public final class BlockStream implements BlockChangeEvent {
	public static boolean DEBUG = false;
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
		return lbai;
	}
	/**
	 * Set the current BlockAccessIndex block, reset byteindex of block to 0; 
	 * @param lbai The block from which the stream is created
	 */
	public synchronized void setBlockAccessIndex(BlockAccessIndex lbai) {
		this.setBlockAccessIndex(lbai, (short) 0);
	}
	
	/**
	 * Set the current BlockAccessIndex block, reset byteindex of block to passed 
	 * @param lbai The block from which the stream is created
	 * @param byteindex The initial byteindex for read or write position
	 */
	public synchronized void setBlockAccessIndex(BlockAccessIndex lbai, short byteindex) {
		this.lbai = lbai;
		this.lbai.byteindex = byteindex;
		if( dbInputStream == null )
			dbInputStream = new DBInputStream(this.lbai, this.blockIO);
		else
			dbInputStream.replaceSource(this.lbai, this.blockIO);
		if( dbOutputStream == null )
			dbOutputStream = new DBOutputStream(this.lbai, this.blockIO);
		else
			dbOutputStream.replaceSource(this.lbai, this.blockIO);
		if(DEBUG)
			System.out.printf("%s.setBlockAccessIndex new stream initiated for block:%s%n", this.getClass().getName(), this.lbai);
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
		if(DEBUG)
			System.out.printf("%s.blockChanged new tablespace=%d block=%s%n",this.getClass().getName(),tablespace,bai);
		setBlockAccessIndex(bai);
	}

}
