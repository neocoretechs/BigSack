package com.neocoretechs.bigsack.io;

import java.io.IOException;

import com.neocoretechs.bigsack.io.cluster.IOWorkerInterface;
import com.neocoretechs.bigsack.io.pooled.Datablock;
/**
 * This interface enforces the contract for IO managers that facilitate block level operations
 * @author jg
 *
 */
public interface IoManagerInterface {

	/**
	 * Return the first available block that can be acquired for write
	 * queue the request to the proper ioworker
	 * @param tblsp The tablespace
	 * @return The block available as a real, not virtual block
	 * @exception IOException if IO problem
	 */
	public long getNextFreeBlock(int tblsp) throws IOException;

	public void FseekAndWrite(long toffset, Datablock tblk)
			throws IOException;

	public void FseekAndWriteFully(long toffset, Datablock tblk)
			throws IOException;

	/**
	 * Queue a request to read int the passed block buffer 
	 * @param toffset The virtual block to read
	 * @param tblk The Datablock buffer to read into
	 * @throws IOException
	 */
	public void FseekAndRead(long toffset, Datablock tblk)
			throws IOException;

	/**
	 * Queue a request to read int the passed block buffer 
	 * @param toffset The virtual block to read
	 * @param tblk The Datablock buffer to read into
	 * @throws IOException
	 */
	public void FseekAndReadFully(long toffset, Datablock tblk)
			throws IOException;

	/**
	 * Set the initial free blocks after buckets created or bucket initial state
	 * Since our directory head gets created in block 0 tablespace 0, the next one is actually the start
	 */
	public void setNextFreeBlocks();

	/**
	 * Get first tablespace
	 * @return the position of the first byte of first tablespace
	 */
	public long firstTableSpace() throws IOException;

	/**
	 * Find the smallest tablespace for storage balance, we will always favor creating one
	 * over extending an old one
	 * @return tablespace
	 * @exception IOException if seeking new tablespace or creating fails
	 */
	public int findSmallestTablespace() throws IOException;

	/**
	 * If create is true, create only primary tablespace
	 * else try to open all existing
	 * @param fname String file name
	 * @param create true to create if not existing
	 * @exception IOException if problems opening/creating
	 * @return true if successful
	 * @see IoInterface
	 */
	public boolean Fopen(String fname, int L3cache, boolean create) throws IOException;
	
	public boolean Fopen(String fname, String remote, int L3cache, boolean create) throws IOException;

	public void Fopen() throws IOException;

	public void Fclose() throws IOException;

	public void Fforce() throws IOException;

	public boolean isNew();

	public IOWorkerInterface getIOWorker(int tblsp);

}