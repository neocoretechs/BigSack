package com.neocoretechs.bigsack.io;

import java.io.IOException;
import java.util.ArrayList;

import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.BlockStream;
import com.neocoretechs.bigsack.io.pooled.BufferPool;
import com.neocoretechs.bigsack.io.pooled.Datablock;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.pooled.MappedBlockBuffer;

/**
 * This interface enforces the contract for IO managers that facilitate block level operations.
 * The page buffers are managed here. Parallel operation on page buffers driven from here
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
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
	
	public void checkpointBufferFlush() throws IOException, IllegalAccessException;

	public void directBufferWrite() throws IOException;
	
	public void getNextFreeBlocks() throws IOException;
	/**
	 * Seek the virtual offset and read the used bytes portion into the specified {@link Datablock}.
	 * For a full read call FseekAndWriteFully. An Fforce flush is performed after all block write operations.
	 * the incore flag is cleared after this operation.
	 * @param toffset
	 * @param tblk
	 * @throws IOException
	 */
	public void FseekAndWrite(long toffset, Datablock tblk) throws IOException;
	
	/**
	 * Seek the specified virtual block and perform a full write of the datablock, ignoring bytesused performing Fforce flush afterwards.
	 * The incore flag is cleared after this operation.
	 * @param toffset
	 * @param tblk
	 * @throws IOException
	 */
	public void FseekAndWriteFully(long toffset, Datablock tblk) throws IOException;
	
	/**
	 * Seek the specified virtual block and and perform a write of the header portion of the block only.
	 * An Fforce flush is performed but NO flags are altered.
	 * @param offset
	 * @param tblk
	 * @throws IOException
	 */
	public void FseekAndWriteHeader(long offset, Datablock tblk) throws IOException;

	/**
	 * Read into the passed {@link Datablock} buffer. The header portion and used bytes alone are read, for a full read use
	 * FseekAndReadFully. The incore flag is cleared after this operation.
	 * @param toffset The virtual block to read
	 * @param tblk The Datablock buffer to read into
	 * @throws IOException
	 */
	public void FseekAndRead(long toffset, Datablock tblk) throws IOException;

	/**
	 * Read into the passed {@link Datablock} block buffer. The header and entire block payload is read regardless of used bytes.
	 * The incore flag is cleared after this operation.
	 * @param toffset The virtual block to read
	 * @param tblk The Datablock buffer to read into
	 * @throws IOException
	 */
	public void FseekAndReadFully(long toffset, Datablock tblk) throws IOException;
	
	/**
	 * Seek the virtual block and perform a read of the header portion only. No flags are altered.
	 * @param toffset
	 * @param tblk
	 * @throws IOException
	 */
	public void FseekAndReadHeader(long toffset, Datablock tblk) throws IOException;

	/**
	 * @see com.neocoretechs.bigsack.io.MultithreadedIOManager#initialize()
	 * @exception IOException if problems opening/creating
	 * @return true if successful
	 */
	boolean initialize() throws IOException;
	
	public BlockAccessIndex getNextFreeBlock() throws IOException;

	/**
	 * Get next free block from given tablespace. The block is translated from real to virtual block.
	 * @return The next free block from the tablespace, translated to a virtual block.
	 * @throws IOExcepion
	 * */
	BlockAccessIndex getNextFreeBlock(int tblsp) throws IOException;

	/**
	 * Used to move a stolen block from freechain onto used list.
	 * @param blk
	 * @return 
	 * @throws IOException 
	 */
	public BlockAccessIndex addBlockAccess(BlockAccessIndex blk) throws IOException;
	
	/**
	* Determine location of new node. {@link MultithreadedIOManager}
	* Attempts to cluster entries in used blocks near insertion point relative to other entries.
	* Choose a random tablespace, then find a key that has that tablespace, then cluster there.
	* @param keys The array of page pointers of existing entries to check for block space
	* @param index The index of the target in array, such that we dont check against that entry
	* @param nkeys The total keys in use to check in array
	* @param bytesneeded The number of bytes to write
	* @return The Optr pointing to the new node position
	* @exception IOException If we cannot get block for new node
	*/
	public Optr getNewInsertPosition(ArrayList<Optr> keys, int index, int nkeys, int length) throws IOException;
	
	public void Fopen() throws IOException;

	public void Fclose() throws IOException;

	public void Fforce() throws IOException;
	
	public long Fsize(int tablespace) throws IOException;

	public boolean isNew();
	
	public RecoveryLogManager getUlog(int tblsp);
	
	public GlobalDBIO getIO();

	public int objseek(Optr loc) throws IOException;

	public int deleten(Optr loc, int size) throws IOException;

	public void writen(int tblsp, byte[] o, int osize) throws IOException;

	public int objseek(long iloc) throws IOException;

	public void deallocOutstandingRollback() throws IOException;
	
	public void deallocOutstandingCommit() throws IOException;
	
	public void deallocOutstanding() throws IOException;
	/**
	 * Perform an Fseek on the block and and write it. Use the write method of Datablock and
	 * the IoWorker for the proper tablespace. Used in final applyChange 
	 * operation of the logAndDo method of the {@link FileLogger} to push the modified block to deep storage.
	 * Used in conjunction with {@link UndoableBlock}.
	 * @param tablespace the tablespace
	 * @param block the physical block
	 * @param blk the data block
	 * @throws IOException
	 */
	public void writeDirect(int tablespace, long block, Datablock blk) throws IOException;

	public void readDirect(int tablespace, long block, Datablock blk) throws IOException;

	public void extend(int ispace, long l) throws IOException;

	public BlockStream getBlockStream(int tablespace);

	public void reInitLogs() throws IOException;

	

}