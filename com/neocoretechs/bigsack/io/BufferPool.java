package com.neocoretechs.bigsack.io;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.cluster.IOWorkerInterface;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.BlockStream;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.pooled.MappedBlockBuffer;
import com.neocoretechs.bigsack.io.pooled.ObjectDBIO;
import com.neocoretechs.bigsack.io.request.CommitRequest;
import com.neocoretechs.bigsack.io.request.IoRequestInterface;
import com.neocoretechs.bigsack.io.request.iomanager.AddBlockAccessNoReadRequest;
import com.neocoretechs.bigsack.io.request.iomanager.DirectBufferWriteRequest;
import com.neocoretechs.bigsack.io.request.iomanager.FindOrAddBlockAccessRequest;
import com.neocoretechs.bigsack.io.request.iomanager.ForceBufferClearRequest;
import com.neocoretechs.bigsack.io.request.iomanager.GetUsedBlockRequest;
/**
 * This class provides encapsulation and management of the database block page pool, the block cursor class,
 * and the recovery manager. Since the MappedBlockBuffers for each tablespace and the cursor class are
 * all being serviced by different threads, and calls back and forth to the standalone or cluster IOManagers occur
 * this class serves as a synchronization barrier for accessing those aggregate buffer pool classes.
 * @see IOManagerInterface
 * @author jg
 *
 */
public class BufferPool {
	private static final boolean DEBUG = false;
	protected final CyclicBarrier forceBarrierSynch = new CyclicBarrier(DBPhysicalConstants.DTABLESPACES);
	protected final CyclicBarrier commitBarrierSynch = new CyclicBarrier(DBPhysicalConstants.DTABLESPACES);
	protected final CyclicBarrier directWriteBarrierSynch = new CyclicBarrier(DBPhysicalConstants.DTABLESPACES);
	private long[] nextFree = new long[DBPhysicalConstants.DTABLESPACES];
	private MappedBlockBuffer[] blockBuffer; // block number to Datablock
	private RecoveryLogManager[] ulog;
	private BlockStream[] lbai = new BlockStream[DBPhysicalConstants.DTABLESPACES];
	
	public BufferPool() {
		blockBuffer = new MappedBlockBuffer[DBPhysicalConstants.DTABLESPACES];
		ulog = new RecoveryLogManager[DBPhysicalConstants.DTABLESPACES];
	}
	
	public synchronized long getNextFree(int tblsp) {
		return nextFree[tblsp];
	}
	public synchronized void setNextFree(long[] nextFree) {
		this.nextFree = nextFree;
	}
	public synchronized MappedBlockBuffer[] getBlockBuffer() {
		return blockBuffer;
	}
	/**
	 * Return the MappedBlockBuffer for the tablespace
	 */
	public synchronized MappedBlockBuffer getBlockBuffer(int tblsp) { return blockBuffer[tblsp]; }
	/**
	 * Set the passed MappedBlockBuffer array as the page pool for each tablespace
	 * @param blockBuffer
	 */
	public synchronized void setBlockBuffer(MappedBlockBuffer[] blockBuffer) {
		this.blockBuffer = blockBuffer;
	}
	/**
	 * Get the array of Recovery log managers for each tablespace
	 * @return
	 */
	protected synchronized RecoveryLogManager[] getUlog() {
		return ulog;
	}
	/**
	 * Get the RecoveryLogManager for a particular tablespace
	 * @param tablespace
	 * @return
	 */
	public synchronized RecoveryLogManager getUlog(int tablespace) {
		return ulog[tablespace];
	}
	/**
	 * Set the array of RecoveryLogManagers for each tablespace
	 * @param ulog
	 */
	protected synchronized void setUlog(RecoveryLogManager[] ulog) {
		this.ulog = ulog;
	}
	/**
	 * Get the page pool cursor for a given tablespace
	 * @param tblsp
	 * @return
	 */
	protected synchronized BlockStream getLbai(int tblsp) {
		return lbai[tblsp];
	}
	/**
	 * Set the array of page pool cursors for each tablespace
	 * @param lbai
	 */
	protected synchronized void setLbai(BlockStream[] lbai) {
		this.lbai = lbai;
	}
	/**
	 * Return the block access index and db buffered stream for this tablespace
	 * @param tblsp
	 * @return
	 */
	public synchronized BlockStream getBlockStream(int tblsp) { return lbai[tblsp]; }
	
	/**
	 * Create the MappedBlockBuffer block pools, block pool cursor, and recovery log managers for each tablespace
	 * @param globalIO the Global IO manager
	 * @param ioManager The IOManagerInterface for cluster or standalone, etc
	 * @param i The target tablespace to create the pool elements for
	 * @throws IOException
	 */
	public synchronized void createPool(ObjectDBIO globalIO, IoManagerInterface ioManager, int i) throws IOException {
		blockBuffer[i] = new MappedBlockBuffer(ioManager, i);
		lbai[i] = new BlockStream(i, blockBuffer[i]);
		ulog[i] = new RecoveryLogManager(globalIO,i);	
	}
	/**
	 * Perform a rollback recovery from the recovery log files
	 * @param i
	 * @throws IOException
	 */
	public synchronized void recover(int i) throws IOException {
		ulog[i].getLogToFile().recover();	
	}
	/**
	 * Find a block in the pool or pull a block from the freechain and bring it in from deep store.
	 * @param tblsp The target table space
	 * @param tbn The logical block number of page
	 * @throws IOException
	 */
	public synchronized void findOrAddBlock(int tblsp, long tbn) throws IOException {
		lbai[tblsp].setLbai(blockBuffer[tblsp].findOrAddBlock(tbn));	
	}

	/**
	* Find or add the block to in-mem list.  First deallocate the currently
	* used block, get the new block, then allocate it
	* @param tbn The virtual block
	* @exception IOException If low-level access fails
	*/
	public synchronized int findOrAddBlock(long tbn) throws IOException {
		int tblsp = GlobalDBIO.getTablespace(tbn);
		if( DEBUG )
			System.out.println("BufferPool.findOrAddBlock tablespace "+tblsp+" pos:"+GlobalDBIO.valueOf(tbn)+" current:"+lbai[tblsp]);
		// If the current entry is the one we are looking for, set byteindex to 0 and return
		// if not, call 'dealloc' and find our target
		lbai[tblsp].setLbai(blockBuffer[tblsp].findOrAddBlock(tbn));
		return tblsp;
	}
	/**
	 * Determin the block position of a new node
	 * @param tblsp The tablespace target
	 * @return The block and offset of the new node position in the block
	 * @throws IOException
	 */
	public synchronized Optr getNewNodePosition(int tblsp) throws IOException {
		return blockBuffer[tblsp].getNewNodePosition(lbai[tblsp].getLbai());	
	}
	/**
	 * Set the position of the next free block in the freechain
	 * @param tblsp Target tablespace
	 * @param longReturn The block virtual number
	 */
	public synchronized void setNextFree(int tblsp, long longReturn) {
		nextFree[tblsp] = longReturn;	
	}
	/**
	 * Set the free blocks for initial bucket creation. Page 0 of tablespace 0 always has root node, so we
	 * allocate first free of that one to be at DBLOCKSIZ
	 */
	public synchronized void setNextFreeBlocks() {
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++)
			if (i == 0)
				nextFree[i] = ((long) DBPhysicalConstants.DBLOCKSIZ);
			else
				nextFree[i] = 0L;
	}
	/**
	 * Find the smallest tablespace. Presumably to do an insert. Attempt to keep
	 * pages somewhat balanced.
	 * @param primarySize The size of primary tablespace 0 to use as a reference point.
	 * @return
	 */
	public synchronized int findSmallestTablespace(long primarySize) {
		int smallestTablespace = 0; // default main
		long smallestSize = primarySize;
		for (int i = 0; i < nextFree.length; i++) {
			if(nextFree[i] != -1 && GlobalDBIO.getBlock(nextFree[i]) < smallestSize) {
				smallestSize = GlobalDBIO.getBlock(nextFree[i]);
				smallestTablespace = i;
			}
		}
		return smallestTablespace;
	}
	/**
	 * Roll back the transactions for the given tablespace to the last checkpoint or commit.
	 * @param i
	 * @return
	 * @throws IOException
	 */
	public synchronized boolean rollback(int i) throws IOException {
		if( lbai[i] != null & lbai[i].getLbai() != null ) {
			lbai[i].getLbai().decrementAccesses();
			forceBufferClear();
			ulog[i].rollBack();
			ulog[i].stop();
			return true;
		}
		return false;
	}

	/**
	 * Force a clearing of the page pool. This amounts to creating a ForceBufferClearRequest and queuing it to the MappedBlockBuffer
	 */
	public synchronized void forceBufferClear() {
		CountDownLatch cdl = new CountDownLatch(DBPhysicalConstants.DTABLESPACES);
		for(int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				ForceBufferClearRequest fbcr = new ForceBufferClearRequest(blockBuffer[i], cdl, forceBarrierSynch);
				blockBuffer[i].queueRequest(fbcr);
				//blockBuffer[i].forceBufferClear();
		}
		try {
				cdl.await();// wait for completion
		} catch (InterruptedException e) {
				// executor requested thread shutdown
				return;
		}
	}
	/**
	 * Deallocate the outstanding block in the buffer pool for each tablespace.
	 * This unlatches the blocks involved in the latest transaction.
	 * @throws IOException
	 */
	public synchronized void deallocOutstanding() throws IOException {
		for(int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			if( lbai[i] != null ) {
				if( lbai[i].getLbai() != null )
					lbai[i].getLbai().decrementAccesses();
			}
		}		
	}
	/**
	 * Deallocate and unlatch the outstanding specific block.
	 * @param pos the virtual block number to deallocate
	 * @throws IOException
	 */
	public synchronized void deallocOutstanding(long pos) throws IOException {
		int tablespace = GlobalDBIO.getTablespace(pos);
		if( lbai[tablespace].getLbai().getBlockNum() == pos ) {
			if( lbai[tablespace].getLbai().getBlk().isIncore() ) {
				//deallocOutstandingWriteLog(tablespace, lbai[tablespace].getLbai());
			} else
				lbai[tablespace].getLbai().decrementAccesses();
			if( DEBUG )
				System.out.println("MultithreadedIOManager.deallocOutstanding current "+GlobalDBIO.valueOf(pos));
		} else {
			BlockAccessIndex bai = blockBuffer[tablespace].get(pos);
			if( bai != null) {
				if( bai.getBlk().isIncore() ) {
					//deallocOutstandingWriteLog(tablespace, bai);
				} else
					bai.decrementAccesses(); 
			} else {
				//System.out.println("MultithreadedIOManager WARNING!! FAILED TO DEALLOCATE "+GlobalDBIO.valueOf(pos));
				//new Throwable().printStackTrace();
			}
		}
		
	}
	/**
	 * Write the block to the log then unlatch it.
	 * @param tablespace The tablespace
	 * @param lbai2 The block, which is presumably outside the pool
	 * @throws IOException
	 */
	public synchronized void deallocOutstandingWriteLog(int tablespace, BlockAccessIndex lbai2) throws IOException {
		if( lbai2.getAccesses() == 1 &&  lbai2.getBlk().isIncore() &&  !lbai2.getBlk().isInlog()) {
			// will set incore, inlog, and push to raw store via applyChange of Loggable
			ulog[tablespace].writeLog(lbai2);
			lbai2.decrementAccesses();
		} else {
			throw new IOException("BufferPool.deallocOutstandingWriteLog failed to dealloc intended target tablespace:"+tablespace+" "+lbai);
		}
	}
	/**
	 * Write the pooled block to the log then unlathc.
	 * @param tblsp The tablespace with the current block as latched. current is that which has been block cursored.
	 * @throws IOException
	 */
	public synchronized void deallocOutstandingWriteLog(int tblsp) throws IOException {
		deallocOutstandingWriteLog(tblsp, lbai[tblsp].getLbai());	
	}
	/**
	 * Add a block, take it from freechain and set its block number, which latches it, then put it in main buffer.
	 * @param Lbn The target block number, which might be a template for a future write.
	 * @return
	 * @throws IOException 
	 */
	public synchronized BlockAccessIndex addBlockAccessNoRead(Long Lbn) throws IOException {
		int tblsp = GlobalDBIO.getTablespace(Lbn);
		return blockBuffer[tblsp].addBlockAccessNoRead(Lbn);
	}
	/**
	 * Formulate a request to the page buffer to bring target page into the pool and latch it.
	 * @param bn The target virtual block number
	 * @return
	 * @throws IOException 
	 */
	public synchronized BlockAccessIndex findOrAddBlockAccess(long bn) throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.findOrAddBlockAccess "+GlobalDBIO.valueOf(bn));
		int tblsp = GlobalDBIO.getTablespace(bn);
		return blockBuffer[tblsp].findOrAddBlock(bn);
	}
	/**
	 * Formulate a request to get a block already in the pool.
	 * @param loc
	 * @return
	 */
	public synchronized BlockAccessIndex getUsedBlock(long loc) {
		int tblsp = GlobalDBIO.getTablespace(loc);
		return blockBuffer[tblsp].getUsedBlock(loc);
	}
	/**
	 * Commit the current buffers and flush the page pools of buffered pages.
	 * @param ioWorker The array of IOWorkers to handle each commmit request.
	 */
	public synchronized void commmitBufferFlush(IOWorkerInterface[] ioWorker) {
		CountDownLatch barrierCount = new CountDownLatch(DBPhysicalConstants.DTABLESPACES);
		// queue to each tablespace
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				IoRequestInterface iori  = new CommitRequest(blockBuffer[i], ulog[i], commitBarrierSynch, barrierCount);
				ioWorker[i].queueRequest(iori);
		}
		try {
				barrierCount.await();
		} catch (InterruptedException e) {}
		
	}
	/**
	 * Create a request to send to each block buffer of each tablepsace to write the outstanding blocks
	 * in each of their buffers. Use a countdownlatch to await each tablespace completion.
	 */
	public synchronized void directBufferWrite() {
		CountDownLatch cdl = new CountDownLatch( DBPhysicalConstants.DTABLESPACES);
		for(int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				//blockBuffer[i].directBufferWrite();
				DirectBufferWriteRequest dbwr = new DirectBufferWriteRequest(blockBuffer[i], cdl, directWriteBarrierSynch);
				blockBuffer[i].queueRequest(dbwr);
		}
		try {
				cdl.await();
		} catch (InterruptedException e) {
				return;
		}
	}
	/**
	 * Seek the block buffer cursor forward by a specific offset.
	 * @param tblsp The tablespace to seek
	 * @param offset The offset to seek it by
	 * @return true if the seek was successful
	 * @throws IOException
	 */
	public synchronized boolean seekFwd(int tblsp, long offset) throws IOException {
		return blockBuffer[tblsp].seek_fwd(lbai[tblsp].getLbai(), offset);
	}
	/**
	 * Write osize bytes current block buffer cursor position.
	 * @param tblsp The tablespace to write to
	 * @param o The byte array to write
	 * @param osize The number of bytes to write
	 * @throws IOException
	 */
	public synchronized void writen(int tblsp, byte[] o, int osize) throws IOException {
		blockBuffer[tblsp].writen(lbai[tblsp].getLbai(), o, osize);
	}
	/**
	 * Delete Osize bytes from the address block and offset
	 * @param adr The virtual block and offset within that block to delete
	 * @param osize The number of bytes to delete
	 * @return The tablespace for fluent interfacing
	 * @throws IOException
	 */
	public synchronized int deleten(Optr adr, int osize) throws IOException {
		int tblsp = objseek(adr);
		blockBuffer[tblsp].deleten(lbai[tblsp].getLbai(), osize);
		return tblsp;
	}
	/**
	* objseek - seek to offset within block
	* @param adr block/offset to seek to
	* @exception IOException If problem seeking block
	* @see Optr
	*/
	public synchronized int objseek(Optr adr) throws IOException {
		if (adr.getBlock() == -1L)
			throw new IOException("Sentinel block seek error");
		int tblsp = findOrAddBlock(adr.getBlock());
		lbai[tblsp].getLbai().setByteindex(adr.getOffset());
		return tblsp;
	}
	/**
	* objseek - seek to offset within block
	* @param adr long block to seek to
	* @return the tablespace extracted from passed pointer
	* @exception IOException If problem seeking block
	* @see Optr
	*/
	public synchronized int objseek(long adr) throws IOException {
		assert(adr != -1L) : "MultithreadedIOManager objseek Sentinel block seek error";
		int tblsp = findOrAddBlock(adr);
		lbai[tblsp].getLbai().setByteindex((short) 0);
		return tblsp;
	}

		
	}
