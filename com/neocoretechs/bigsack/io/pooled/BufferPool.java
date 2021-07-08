package com.neocoretechs.bigsack.io.pooled;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.MultithreadedIOManager;
import com.neocoretechs.bigsack.io.Optr;
import com.neocoretechs.bigsack.io.RecoveryLogManager;
import com.neocoretechs.bigsack.io.ThreadPoolManager;

/**
 * This class provides encapsulation and management of the database block page pool, the block cursor class,
 * and the recovery manager and the primary IO manager {@link MultithreadedIOManager}.<p/> 
 * We maintain an array of {@link BlockStream}, one for each tablespace. The BlockStream encapsulates a {@link MappedBlockBuffer}.
 * To each MappedBlockBuffer is added a BlockStream as a BlockChangeObserver. When a block changes as we iterate through
 * the linked list the observer is notified so that it can keep current and deliver the proper block in the stream
 * without intervention. This allows us to write contiguous blocks across block boundaries.<p/>
 * Much of the initialization occurs in 'createPool', which is called iteratively for each tablespace as supporting classes are constructed.<p/>
 * Since this class sits above the various tablespace-specific managers, it deals with virtual logical blocks vs physical ones and therefore
 * all references to blocks are virtual and the specific tablespaces and physical blocks must be extracted.<p/>
 * Since the {@link MappedBlockBuffer}s for each tablespace and the cursor class {@link BlockStream} are
 * all being serviced by different threads, and calls back and forth to the IOManagers occur,
 * this class serves as a central point for accessing those aggregate buffer pool classes.<p/>
 * The MappedBlockBuffer blockBuffer moves its retrieved blocks into the BlockStream cursor block through
 * the action of this class, as the MappedblockBuffer has no knowledge of the streams or {@link RecoveryLogManager} recovery log.<p/>
 * @see IOManagerInterface
 * @author Jonathan Groff (C) NeoCoreTechs 2021
 */
public class BufferPool {
	private static final boolean DEBUG = true;
	private GlobalDBIO globalDBIO;
	private MappedBlockBuffer[] blockBuffer; // block number to Datablock
	private RecoveryLogManager[] undoLog;
	private BlockStream[] blockStream = new BlockStream[DBPhysicalConstants.DTABLESPACES];
	private String[] ioWorkerNames; // worker thread group names
	/**
	 * Create the {@link MappedBlockBuffer} and {@link RecoverLogManager}
	 * @param ioWorkerNames the thread names used to spin threads via the {@link ThreadPoolManager}
	 */
	public BufferPool(GlobalDBIO globalDBIO, String[] ioWorkerNames) {
		this.globalDBIO = globalDBIO; 
		this.ioWorkerNames = ioWorkerNames;
		blockBuffer = new MappedBlockBuffer[DBPhysicalConstants.DTABLESPACES];
		undoLog = new RecoveryLogManager[DBPhysicalConstants.DTABLESPACES];
	}
	
	public synchronized MappedBlockBuffer[] getBlockBuffer() {
		return blockBuffer;
	}
	
	/**
	 * Return the MappedBlockBuffer for the tablespace
	 */
	public synchronized MappedBlockBuffer getBlockBuffer(int tblsp) { 
		return blockBuffer[tblsp]; 
	}
	
	/**
	 * Set the passed MappedBlockBuffer array as the page pool for each tablespace
	 * @param blockBuffer
	 */
	public synchronized void setBlockBuffer(MappedBlockBuffer[] blockBuffer) {
		this.blockBuffer = blockBuffer;
	}

	/**
	 * Get the RecoveryLogManager for a particular tablespace
	 * @param tablespace
	 * @return
	 */
	public synchronized RecoveryLogManager getUlog(int tablespace) {
		return undoLog[tablespace];
	}

	/**
	 * Return the block access index and db buffered stream for this tablespace
	 * @param tblsp
	 * @return
	 */
	public synchronized BlockStream getBlockStream(int tblsp) { return blockStream[tblsp]; }
	
	/**
	 * Create the MappedBlockBuffer block pools, block pool cursor, and recovery log managers for each tablespace
	 * @param globalIO the Global IO manager
	 * @param ioManager The IOManagerInterface {@code MultithreadedIOManager}
	 * @param tablespace The target tablespace for which to create the pool elements
	 * @throws IOException
	 */
	public synchronized void createPool(IoInterface ioWorker, int tablespace) throws IOException {
		blockBuffer[tablespace] = new MappedBlockBuffer(ioWorker, tablespace);
		blockStream[tablespace] = new BlockStream(tablespace, blockBuffer[tablespace]);
		undoLog[tablespace] = new RecoveryLogManager(globalDBIO,tablespace);
		// Set the BlockStream as an observer of the block change events generated by 'getnextblk' in MappedBlockbuffer
		blockBuffer[tablespace].addBlockChangeObserver(blockStream[tablespace]);
	}
	
	//public synchronized void reInitLog(GlobalDBIO globalIO, int tablespace) throws IOException {
	//	undoLog[tablespace].getLogToFile().initializeLogFileSequence();
	//}
	/**
	 * Perform a rollback recovery from the recovery log files
	 * @param i
	 * @throws IOException
	 */
	public synchronized void recover(int i) throws IOException {
		undoLog[i].getLogToFile().recover();	
	}
	/**
	 * Write the headers for the blocks after allocating the blocks for a newly created database.
	 * @param ioWorker The {@link IOWorker} for the tablespace to initialize
	 * @throws IOException 
	 */
	public void initialize(IoInterface ioWorker) throws IOException {
		long endBl = ioWorker.Fsize() - (long) DBPhysicalConstants.DBLOCKSIZ;
		Datablock d = new Datablock();
		d.resetBlock();
		while (endBl >= 0L) {
			ioWorker.FseekAndWrite(endBl, d);
			endBl -= (long) DBPhysicalConstants.DBLOCKSIZ; // set up next block end
		}
		ioWorker.Fforce();	
	}
	
	/**
	* Find or add the block to in-mem list.  First deallocate the currently
	* used block, get the new block, then allocate it
	* @param tbn The virtual block
	* @return the tablespace extracted from the passed virtual block number
	* @exception IOException If low-level access fails
	*/
	private synchronized int findOrAddBlock(long tbn) throws IOException {
		int tblsp = GlobalDBIO.getTablespace(tbn);
		long tblk = GlobalDBIO.getBlock(tbn);
		// If the current entry is the one we are looking for, set byteindex to 0 and return
		// if not, call 'dealloc' and find our target
		if(blockStream[tblsp].getBlockAccessIndex() != null ) {
			if(blockStream[tblsp].getBlockAccessIndex().getBlockNum() != tbn) {
				blockStream[tblsp].getBlockAccessIndex().decrementAccesses();
			} else {
				blockStream[tblsp].getBlockAccessIndex().byteindex = 0;
				return tblsp;
			}
		}
		blockStream[tblsp].setBlockAccessIndex(blockBuffer[tblsp].findOrAddBlock(tblk).get());
		if( DEBUG )
			System.out.println("BufferPool.findOrAddBlock RETURN tablespace "+tblsp+" pos:"+GlobalDBIO.valueOf(tbn));
		return tblsp;
	}
	
	public Callable<Object> callRollback(MappedBlockBuffer blockBuffer, RecoveryLogManager logManager, BlockStream blockStream) { 
		return () -> {
			if( blockStream != null & blockStream.getBlockAccessIndex() != null ) {
				blockStream.getBlockAccessIndex().decrementAccesses();
			}
			blockBuffer.forceBufferClear();
			//logManager.rollBack();
			return true;
		};
	}
	
	public void rollback() {
		if( DEBUG )
			System.out.printf("%s.rollback()%n",this.getClass().getName());
		Future<?>[] futureArray = new Future<?>[DBPhysicalConstants.DTABLESPACES];
		// queue to each tablespace
		try {
			globalDBIO.getIOManager().Fforce(); // make sure we synch our main file buffers
			for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				futureArray[i] = ThreadPoolManager.getInstance().spin(callRollback(blockBuffer[i], undoLog[i], blockStream[i]), ioWorkerNames[i]);
			}
			for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				futureArray[i].get();
				undoLog[i].rollBack();
			}
		} catch (ExecutionException | InterruptedException e) {	
			
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
	}
	
	public Callable<Object> callForceBufferClear(MappedBlockBuffer blockBuffer) { 
		return () -> {
			blockBuffer.forceBufferClear();
			return true;
		};
	}
	/**
	 * Force a clearing of the page pool.
	 */
	public synchronized void forceBufferClear() {
		//for(int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
		//		blockBuffer[i].forceBufferClear();
		//}
		if( DEBUG )
			System.out.printf("%s.forceBufferClear()%n",this.getClass().getName());
		Future<?>[] futureArray = new Future<?>[DBPhysicalConstants.DTABLESPACES];
		// queue to each tablespace
		try {
			for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				futureArray[i] = ThreadPoolManager.getInstance().spin(callForceBufferClear(blockBuffer[i]), ioWorkerNames[i]);
			}
			for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				futureArray[i].get();
			}
		} catch (ExecutionException | InterruptedException e) {	}
	}
	
	public Callable<Object> callDeallocOutstanding(BlockStream blockStream) { 
		return () -> {
			if( blockStream != null ) {
				if( blockStream.getBlockAccessIndex() != null )
					blockStream.getBlockAccessIndex().decrementAccesses();
			}
			return true;
		};
	}
	/**
	 * Decrement the access count on ALL the outstanding block in the buffer pool for each tablespace.
	 * This unlatches the blocks involved in the latest transaction. This should leave only 'incore' blocks
	 * as the default unlatching prevents accesses to 0 when block under write
	 * @throws IOException
	 */
	public synchronized void deallocOutstanding() throws IOException {
		//for(int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
		//	if( blks[i] != null ) {
		//		if( blks[i].getBlockAccessIndex() != null )
		//			blks[i].getBlockAccessIndex().decrementAccesses();
		//	}
		//}
		if( DEBUG )
			System.out.printf("%s.deallocOutstanding()%n",this.getClass().getName());
		Future<?>[] futureArray = new Future<?>[DBPhysicalConstants.DTABLESPACES];
		// queue to each tablespace
		try {
			for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				futureArray[i] = ThreadPoolManager.getInstance().spin(callDeallocOutstanding(blockStream[i]), ioWorkerNames[i]);
			}
			for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				futureArray[i].get();
			}
		} catch (ExecutionException | InterruptedException e) {	}
	}

	
	/**
	 * Write the block to the log then unlatch it by decrementing accesses.
	 * @param tablespace The tablespace
	 * @param lbai2 The block, which is presumably outside the pool
	 * @throws IOException
	 */
	public synchronized void deallocOutstandingWriteLog(int tablespace, BlockAccessIndex lbai2) throws IOException {
		if( lbai2.getAccesses() == 1 &&  lbai2.getBlk().isIncore() &&  !lbai2.getBlk().isInlog()) {
			// will set incore, inlog, and push to raw store via applyChange of Loggable
			undoLog[tablespace].writeLog(lbai2);
			lbai2.decrementAccesses();
		} else {
			throw new IOException("BufferPool.deallocOutstandingWriteLog failed to dealloc intended target tablespace:"+tablespace);
		}
	}
		
	/**
	 * Add a block, take it from freechain and set its block number, which latches it, then put it in main buffer.
	 * @param Lbn The target block number, as Vblock, which might be a template for a future write.
	 * @return
	 * @throws IOException 
	 */
	public synchronized BlockAccessIndex addBlockAccessNoRead(Long Lbn) throws IOException {
		int tblsp = GlobalDBIO.getTablespace(Lbn);
		return blockBuffer[tblsp].addBlockAccessNoRead(GlobalDBIO.getBlock(Lbn)).get();
	}
	
	public BlockAccessIndex addBlockAccess(BlockAccessIndex blk) throws IOException {
		int tblsp = GlobalDBIO.getTablespace(blk.getBlockNum());
		return blockBuffer[tblsp].addBlockAccess(blk).get();
	}
	/**
	 * Formulate a request to the page buffer to bring target page into the pool and latch it.
	 * @param bn The target virtual block number
	 * @return
	 * @throws IOException 
	 */
	public synchronized BlockAccessIndex findOrAddBlockAccess(long bn) throws IOException {
		if( DEBUG )
			System.out.printf("%s.findOrAddBlockAccess %s%n",this.getClass().getName(),GlobalDBIO.valueOf(bn));
		int tblsp = GlobalDBIO.getTablespace(bn);
		return blockBuffer[tblsp].findOrAddBlock(GlobalDBIO.getBlock(bn)).get();
	}
	
	public Callable<Object> callCommitBufferFlush(MappedBlockBuffer blockBuffer, RecoveryLogManager logManager) { 
		return () -> {
			blockBuffer.commitBufferFlush(logManager);
			return true;
		};
	}
	
	public Callable<Object> callCommit(MappedBlockBuffer blockBuffer, RecoveryLogManager logManager) { 
		return () -> {
			logManager.commit(blockBuffer);
			return true;
		};
	}
	/**
	 * Commit the current buffers and flush the page pools of buffered pages.
	 * @param ioWorker The array of IOWorkers to handle each commit request.
	 * @throws IOException 
	 */
	public synchronized void commitBufferFlush() throws IOException {
		// queue to each tablespace
		//for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
		//		blockBuffer[i].commitBufferFlush(ulog[i]);
		//		ulog[i].commit(blockBuffer[i]);
		//}
			if( DEBUG )
				System.out.printf("%s.commitBufferFlush()%n",this.getClass().getName());
			Future<?>[] futureArray = new Future<?>[DBPhysicalConstants.DTABLESPACES];
			// queue to each tablespace
			try {
				for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
					futureArray[i] = ThreadPoolManager.getInstance().spin(callCommitBufferFlush(blockBuffer[i], undoLog[i]), ioWorkerNames[i]);
				}
				for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
					futureArray[i].get();
					if(DEBUG)
						System.out.printf("%s next commitBufferFlush for tablespace %d%n", this.getClass().getName(),i);
				}
			} catch (InterruptedException | ExecutionException e) {
				throw new IOException(e);
			}
			// now initiate the commit for each tablespace
			//try {
				//for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				//	futureArray[i] = ThreadPoolManager.getInstance().spin(callCommit(blockBuffer[i], undoLog[i]), ioWorkerNames[i]);
				//}
				for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
					//futureArray[i].get();
					undoLog[i].commit(blockBuffer[i]);
					if(DEBUG)
						System.out.printf("%s next commit for tablespace %d%n", this.getClass().getName(),i);
				}
			//} catch (InterruptedException | ExecutionException e) {
			//	throw new IOException(e);
			//}
	}
	
	//public Callable<Object> callCheckpoint(RecoveryLogManager logManager) { 
	//	return () -> {
	//		logManager.checkpoint();
	//		return true;
	//	};
	//}
	/**
	 * Checkpoint the current buffers and flush the page pools of buffered pages.
	 * @param ioWorker The array of IOWorkers to handle each checkpoint request.
	 * @throws IOException 
	 * @throws IllegalAccessException 
	 */
	public synchronized void checkpointBufferFlush(IOWorker[] ioWorker) throws IOException, IllegalAccessException {
		globalDBIO.forceBufferWrite();
		//for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
		//	blockBuffer[i].commitBufferFlush(ulog[i]);
		//	ulog[i].checkpoint();
		//}
		if( DEBUG )
			System.out.printf("%s.checkpointBufferFlush()%n",this.getClass().getName());
		Future<?>[] futureArray = new Future<?>[DBPhysicalConstants.DTABLESPACES];
		// queue to each tablespace
		try {
			for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				futureArray[i] = ThreadPoolManager.getInstance().spin(callCommitBufferFlush(blockBuffer[i], undoLog[i]), ioWorkerNames[i]);
			}
			for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				futureArray[i].get();
				if(DEBUG)
					System.out.printf("%s next checkpointBufferFlush for tablespace %d%n", this.getClass().getName(),i);
			}
		} catch (InterruptedException | ExecutionException e) {
			throw new IOException(e);
		}
		// now initiate the commit for each tablespace
		//try {
			for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				//futureArray[i] = ThreadPoolManager.getInstance().spin(callCheckpoint(undoLog[i]), ioWorkerNames[i]);
				undoLog[i].checkpoint();
			}
			//for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				//futureArray[i].get();
				//if(DEBUG)
					//System.out.printf("%s next checkpoint for tablespace %d%n", this.getClass().getName(),i);
			//}
		//} catch (InterruptedException | ExecutionException e) {
		//	throw new IOException(e);
		//}
	}

	public Callable<Object> callDirectBufferWrite(MappedBlockBuffer blockBuffer) { 
		return () -> {
			blockBuffer.directBufferWrite();
			return true;
		};
	}
	/**
	 * Create a request to send to each block buffer of each tablespace to write the outstanding blocks
	 * in each of their buffers. Use a countdownlatch to await each tablespace completion.
	 * @throws IOException 
	 */
	public synchronized void directBufferWrite() throws IOException {
		//for(int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
		//		blockBuffer[i].directBufferWrite();
		//}
		if( DEBUG )
			System.out.printf("%s.directBufferWrite()%n",this.getClass().getName());
		Future<?>[] futureArray = new Future<?>[DBPhysicalConstants.DTABLESPACES];
		// queue to each tablespace
		try {
			for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				futureArray[i] = ThreadPoolManager.getInstance().spin(callDirectBufferWrite(blockBuffer[i]), ioWorkerNames[i]);
			}
			for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				futureArray[i].get();
				if(DEBUG)
					System.out.printf("%s next directBufferWrite for tablespace %d%n", this.getClass().getName(),i);
			}
		} catch (InterruptedException | ExecutionException e) {
			throw new IOException(e);
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
		return blockBuffer[tblsp].seek_fwd(new SoftReference<BlockAccessIndex>(blockStream[tblsp].getBlockAccessIndex()), offset);
	}
	
	/**
	 * Write osize bytes current block buffer cursor position.
	 * @param tblsp The tablespace to write to
	 * @param o The byte array to write
	 * @param osize The number of bytes to write
	 * @throws IOException
	 */
	public synchronized void writen(int tblsp, byte[] o, int osize) throws IOException {
		blockBuffer[tblsp].writen(new SoftReference<BlockAccessIndex>(blockStream[tblsp].getBlockAccessIndex()), o, osize);
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
		blockBuffer[tblsp].deleten(new SoftReference<BlockAccessIndex>(blockStream[tblsp].getBlockAccessIndex()), osize);
		return tblsp;
	}
	
	/**
	* objseek - seek to offset within block
	* @param adr block/offset to seek to
	* @exception IOException If problem seeking block
	* @see Optr
	*/
	public synchronized int objseek(Optr adr) throws IOException {
		assert(adr.getBlock() != -1L) : "objseek Sentinel block seek error";
		int tblsp = findOrAddBlock(adr.getBlock());
		blockStream[tblsp].getBlockAccessIndex().setByteindex(adr.getOffset());
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
		assert(adr != -1L) : "objseek Sentinel block seek error";
		int tblsp = findOrAddBlock(adr);
		blockStream[tblsp].getBlockAccessIndex().setByteindex((short) 0);
		return tblsp;
	}

		
}
