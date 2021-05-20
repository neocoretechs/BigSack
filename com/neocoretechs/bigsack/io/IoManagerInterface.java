package com.neocoretechs.bigsack.io;

import java.io.IOException;

import com.neocoretechs.bigsack.io.cluster.IOWorkerInterface;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.BlockStream;
import com.neocoretechs.bigsack.io.pooled.Datablock;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.pooled.ObjectDBIO;

/**
 * This interface enforces the contract for IO managers that facilitate block level operations.
 * The page buffers are managed here. Parallel operation on page buffers driven from here
 * @author jg
 *
 */
public interface IoManagerInterface {
	/**
	* We'll do this on a 'clear' of collection, reset all caches
	* Take the used block list, reset the blocks, move to to free list, then
	* finally clear the used block list. We do this during rollback to remove any modified
	*/
	public void forceBufferClear();

	/**
	* Add a block to table of blocknums and block access index.
	* Comes here for acquireBlock. No setting of block in BlockAccessIndex, no initial read
	* @param Lbn block number to add
	* @exception IOException if new dblock cannot be created
	*/
	public BlockAccessIndex addBlockAccessNoRead(Long Lbn) throws IOException;

	/**
	* findOrAddBlockAccess - find and return block in pool or bring
	* it in and add it to pool<br> Always ends up calling alloc, here or in addBlock.
	* @param bn The block number to add
	* @return The index into the block array
	* @exception IOException if cannot read
	*/
	public BlockAccessIndex findOrAddBlockAccess(long bn) throws IOException;

	/**
	* Get a block access control instance from L2 cache
	* @param tmpBai2 The template containing the block number, used to locate key
	* @return The key found or whatever set returns otherwise if nothing a null is returned
	* @throws IOException 
	*/
	public BlockAccessIndex getUsedBlock(long loc) throws IOException;
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
	 * Check the free block list for 0 elements. This is a demand response method guaranteed to give us a free block
	 * if 0, begin a search for an element that has 0 accesses
	 * if its in core and not yet in log, write through
	 * @throws IOException
	 */
	//public void freeupBlock() throws IOException;
	/**
	 * Commit the outstanding blocks and flush the buffer of pages at end
	 * @throws IOException
	 */
	public void commitBufferFlush() throws IOException;
	
	public void checkpointBufferFlush() throws IOException;

	public void directBufferWrite() throws IOException;
	
	/**
	 * Return the first available block that can be acquired for write
	 * queue the request to the proper ioworker
	 * @param tblsp The tablespace
	 * @return The block available as a real, not virtual block
	 * @exception IOException if IO problem
	 */
	public long getNextFreeBlock(int tblsp) throws IOException;
	
	public void getNextFreeBlocks() throws IOException;

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
	
	public long Fsize(int tablespace) throws IOException;

	public boolean isNew();

	public IOWorkerInterface getIOWorker(int tblsp);
	
	public BufferPool getBufferPool();
	
	public RecoveryLogManager getUlog(int tblsp);
	
	public ObjectDBIO getIO();

	public Optr getNewInsertPosition(Optr[] locs, int index, int nkeys, int length) throws IOException;
	
	public MappedBlockBuffer getBlockBuffer(int tablespace);

	public int objseek(Optr loc) throws IOException;

	public int deleten(Optr loc, int size) throws IOException;

	public BlockStream getBlockStream(int tblsp);

	public void writen(int tblsp, byte[] o, int osize) throws IOException;

	public int objseek(long iloc) throws IOException;

	public void deallocOutstandingRollback() throws IOException;
	public void deallocOutstandingCommit() throws IOException;
	public void deallocOutstanding() throws IOException;
	public void deallocOutstandingWriteLog(int tblsp) throws IOException;
	public void deallocOutstandingWriteLog(int tblsp, BlockAccessIndex lbai) throws IOException;
	public void deallocOutstanding(long pos) throws IOException;

	public void writeDirect(int tablespace, long block, Datablock blk) throws IOException;

	public void readDirect(int tablespace, long block, Datablock blk) throws IOException;

	public FreeBlockAllocator getFreeBlockAllocator();

}