package com.neocoretechs.bigsack.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.BufferPool;
import com.neocoretechs.bigsack.io.pooled.Datablock;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.pooled.IOWorker;
import com.neocoretechs.bigsack.io.pooled.MappedBlockBuffer;
import com.neocoretechs.bigsack.io.stream.DBInputStream;
import com.neocoretechs.bigsack.io.stream.DBOutputStream;

/**
 * Handles the aggregation of the IO worker threads of which there is one for each tablespace.<p/>
 * Calls to the IO worker assigned to the tablespace desired can operate in parallel
 * with granularity at the tablespace/randomaccessfile level. Asynchronous IO is performed on random access files,
 * either memory mapped or filesystem.<p/>
 * The {@link BufferPool} creates a {@link  MappedBlockBuffer} and an {@link IOWorker} for each tablespace, 
 * fulfilling the {@link IoInterface} contract.<p/>
 * The BufferPool aggregates the MappedBlockBuffer, the Blockstreams, and the undo log for each tablespace.<p/>
 * Operations that work on multiple tablespaces are set up inside Callable lambda expressions and the Futures are
 * aggregated.
 * @author Jonathan Groff  Copyright (C) NeoCoreTechs 2014,2015,2021
 *
 */
public class MultithreadedIOManager implements IoManagerInterface {
	private static final boolean DEBUG = false;
	private static final boolean DEBUGINSERT = false;
	private static final boolean DEBUGWRITE = false; // view blocks written to log and store
	public GlobalDBIO globalIO;
	private IOWorker ioWorker[];
	private BufferPool bufferPool;
	protected int L3cache = 0;
	public String[] ioWorkerNames = new String[DBPhysicalConstants.DTABLESPACES];
	/**
	 * Create the Multi threaded IO manager that manages the IO worker threads and the {@link BufferPool} that maintains the cache of
	 * page level data and the recovery log. 
	 * @param globalIO The global IO module
	 * @param L3cache The primary backing store for the databases, be it filesystem or memory mapped page files, etc.
	 * @throws IOException If we encounter a problem initializing the log subsystem or other related files/stores.
	 */
	public MultithreadedIOManager(GlobalDBIO globalIO, int L3cache) throws IOException {
		this.globalIO = globalIO;
		this.L3cache = L3cache;
		ioWorker = new IOWorker[DBPhysicalConstants.DTABLESPACES];
		// Initialize the thread pool group NAMES to spin new threads in controllable batches
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			ioWorkerNames[i] = String.format("%s%s%d", "IOWORKER",globalIO.getDBName(),i);
		}
		ThreadPoolManager.init(ioWorkerNames, false);
		bufferPool = new BufferPool(globalIO, ioWorkerNames);
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			ioWorker[i] = new IOWorker(globalIO, i, L3cache);
			bufferPool.createPool(ioWorker[i], i);
		}
	}
	
	@Override
	/**
	 * 
	 * This is where the recovery logs are initialized because the logs operate at the block (database page) level.
	 * When this module is instantiated the RecoveryLogManager is assigned to 'ulog' and a roll forward recovery
	 * is started. If there are any records in the log file they will scanned for low water marks and
	 * checkpoints etc and the determination is made based on the type of log record encountered.
	 * Our log granularity is the page level. We store DB blocks and their original mirrors to use in
	 * recovery. At the end of recovery we restore the logs to their initial state, as we do on a commit. 
	 * There is a simple paradigm at work here, we carry a single block access index in another class and use it
	 * to cursor through the blocks as we access them. The BlockStream class has the BlockAccessIndex and DBStream
	 * for each tablespace. The cursor window block and read and written from seep store and buffer pool.
	 */
	public boolean initialize() throws IOException {
		synchronized(bufferPool) {
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			if( !isNew() ) {
				// attempt recovery if needed
				bufferPool.recover(i);
			} else {
				bufferPool.initialize(ioWorker[i]);
			}
		}
		}
		getNextFreeBlocks();
		// Take block 0 from bufferpool
		globalIO.getKeyValueMain().createRootNode();
		// fill in the next free block indicators and set the smallest tablespace
		findEligibleTablespace();
		return true;
	}
	
	@Override
	public synchronized void reInitLogs() throws IOException {
	//	for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
	//		bufferPool.reInitLog(globalIO, i);
	//	}
	}
	
	@Override
	/**
	* Get the new position for clustered entries. If we have values associated with 'keys' we store
	* these 'values' in alternate blocks from the BTree page. This method delivers the block to pack
	* For sets vs maps, we store only the serialized keys clustered on the page.
	* We determine location of new node, store in new_node_pos.
	* Attempts to cluster entries in used blocks near insertion point.
	* @param locs The array of previous entries to check for block space
	* @param bytesNeeded Space required for insert
	* @return The Optr block plus offset in the block pointing to the new node position
	* @exception IOException If we cannot get block for new item
	*/
	public Optr getNewInsertPosition(ArrayList<Long> locs, int bytesNeeded) throws IOException {
		if( DEBUGINSERT )
			System.out.printf("%s.getNewInsertPosition(%s, %d, %d, %d)%n",this.getClass().getName(), locs, bytesNeeded);
		return globalIO.getNewInsertPosition(locs, bytesNeeded);
	}
	
	@Override
	/**
	* Return the reverse scan of the first free block of each tablespace
	* queue the request to the proper {@link ioWorker}.<p/>
	* Accumulates the blocks in the freeBlockList in the {@link MappedBlockBuffer}<p/>.
	* Creates an array of lengths of the freeBlockLists which is sent to {@link FreeBlockAllocator}<p/>.
	* @exception IOException if IO problem
	*/
	public void getNextFreeBlocks() throws IOException {
		if( DEBUG )
			System.out.printf("%s.getNextFreeBlocks()%n",this.getClass().getName());
		Future<?>[] futureArray = new Future<?>[DBPhysicalConstants.DTABLESPACES];
		// queue to each tablespace
		try {
			for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				synchronized(ioWorker[i]) {
					futureArray[i] = ThreadPoolManager.getInstance().spin(ioWorker[i].callGetNextFreeBlock(),ioWorkerNames[i]);
				}
			}
			for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				futureArray[i].get();
				if(DEBUG)
					System.out.printf("%s next free block for tablespace %d%n", this.getClass().getName(),i);
			}
		} catch (InterruptedException | ExecutionException e) {
			throw new IOException(e);
		}
	}
	
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#FseekAndWrite(long, com.neocoretechs.bigsack.io.pooled.Datablock)
	 */
	@Override
	public void FseekAndWrite(long toffset, Datablock tblk) throws IOException {
		if( DEBUG )
			System.out.printf("%s.FseekAndWrite(%s,%s)%n ",this.getClass().getName(),GlobalDBIO.valueOf(toffset),tblk);
		int tblsp = GlobalDBIO.getTablespace(toffset);
		long tblock = GlobalDBIO.getBlock(toffset);
		//try {
			//Future<?> f = ThreadPoolManager.getInstance().spin(ioWorker[tblsp].callFseekAndWrite(offset, tblk),ioWorkerNames[tblsp]);
			//f.get();
		synchronized(ioWorker[tblsp]) {
			ioWorker[tblsp].FseekAndWrite(tblock, tblk);
		}
		//} catch (InterruptedException | ExecutionException e) {
		//	throw new IOException(e);
		//}
	}
	
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#FseekAndWriteFully(long, com.neocoretechs.bigsack.io.pooled.Datablock)
	 */
	@Override
	public void FseekAndWriteFully(long toffset, Datablock tblk) throws IOException {
		if( DEBUG )
			System.out.printf("%s.FseekAndWriteFully(%s,%s)%n ",this.getClass().getName(),GlobalDBIO.valueOf(toffset),tblk);
		int tblsp = GlobalDBIO.getTablespace(toffset);
		long tblock = GlobalDBIO.getBlock(toffset);
		synchronized(ioWorker[tblsp]) {
			ioWorker[tblsp].FseekAndWriteFully(tblock, tblk);
		}
	}
	
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#FseekAndRead(long, com.neocoretechs.bigsack.io.pooled.Datablock)
	 */
	@Override
	public void FseekAndRead(long toffset, Datablock tblk) throws IOException {
		if( DEBUG )
			System.out.printf("%s.FseekAndRead(%s,%s)%n ",this.getClass().getName(),GlobalDBIO.valueOf(toffset),tblk);
		//if( GlobalDBIO.valueOf(toffset).equals("Tablespace_1_114688"))
		//	System.out.println("MultithreadedIOManager.FseekAndRead Tablespace_1_114688");
		int tblsp = GlobalDBIO.getTablespace(toffset);
		long tblock = GlobalDBIO.getBlock(toffset);
		synchronized(ioWorker[tblsp]) {
			ioWorker[tblsp].FseekAndRead(tblock, tblk);
		}
		//if( GlobalDBIO.valueOf(toffset).equals("Tablespace_1_114688"))
		//	System.out.println("MultithreadedIOManager.FseekAndRead EXIT Tablespace_1_114688 "+tblk+" dump:"+tblk.blockdump());
		//assert(tblk.getBytesused() != 0 && tblk.getBytesinuse() != 0) : "MultithreadedIOManager.FseekAndRead returned unusable block from offset "+GlobalDBIO.valueOf(toffset)+" "+tblk.blockdump();
	}
	/**
	 * Formulate a request to seek a block and read all data.
	 * The request architecture allows us to maintain high cohesion and low coupling with all the models
	 * including cluster and standalone. The queue can be 'extended' to remote nodes with the queues servicing
	 * threads also acting as data pipes across the networks once subclassed.
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#FseekAndReadFully(long, com.neocoretechs.bigsack.io.pooled.Datablock)
	 
	@Override
	public void FseekAndReadFully(long toffset, Datablock tblk) throws IOException {
		if( DEBUG )
			System.out.printf("%s.FseekAndReadFully(%s,%s)%n ",this.getClass().getName(),GlobalDBIO.valueOf(toffset),tblk);
		int tblsp = GlobalDBIO.getTablespace(toffset);
		ioWorker[tblsp].FseekAndReadFully(toffset, tblk);
	}
	*/

	/**
	 * Return that which is regarded as the first tablespace, usually 0, location of root node.
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#firstTableSpace()
	 */
	@Override
	public long firstTableSpace() throws IOException {
		return 0L;
	}

	/**
	 * Find the largest freechain, and conversely the smallest tablespace, and leave it in filed smallestTablespace.
	 */
	@Override
	public int findEligibleTablespace() throws IOException {
		int eliglbleTablespace = -1;
		if( DEBUG )
			System.out.printf("%s.findEligibleTablespace invoked.%n",this.getClass().getName());
		Random r = new Random();
		eliglbleTablespace = r.nextInt(DBPhysicalConstants.DTABLESPACES);
		//for (int i = 1; i < DBPhysicalConstants.DTABLESPACES; i++) {
		//	if(bufferPool.getBlockBuffer(i).sizeFreeBlockList() >= bufferPool.getBlockBuffer(eliglbleTablespace).sizeFreeBlockList())
		//		eliglbleTablespace = i;
		//}
		return eliglbleTablespace;
	}
	
	@Override
	/**
	 * Get next free block from given tablespace. The block is translated from real to virtual block.
	 * @return The next free block from the round robin tablespace, translated to a virtual block.
	 * the ioWorker calls back here to addBlockAccess(BlockAccessIndex) from IOWorker.getNextFreeBlock
	 * @throws IOException 
	 */
	public BlockAccessIndex getNextFreeBlock() throws IOException {
		int eliglbleTablespace = findEligibleTablespace();
		if(DEBUG)
			System.out.printf("%s getNextFree smallest Tablespace %d%n", this.getClass().getName(), eliglbleTablespace);
		synchronized(ioWorker[eliglbleTablespace]) {
			BlockAccessIndex bai = ioWorker[eliglbleTablespace].getNextFreeBlock();
			if(bai == null) {
				StringBuilder s = new StringBuilder();
				s.append("size=");
				s.append(bufferPool.getBlockBuffer(eliglbleTablespace).getFreeBlockList().size());
				s.append("|");
				for(Long b : bufferPool.getBlockBuffer(eliglbleTablespace).getFreeBlockList().keySet()) {
					s.append(b);
					s.append("|");
				}
				throw new IOException("Failed to remove valid block from free list:"+GlobalDBIO.valueOf(bai.getBlockNum())+" "+s.toString());
			}
			if(DEBUG)
				System.out.printf("%s getNextFree smallest Tablespace %d for next free block %s%n", this.getClass().getName(), eliglbleTablespace, bai);
			return bai;
		}
	}
	
	@Override
	/**
	 * Get next free block from given tablespace. The block is translated from real to virtual block.
	 * The ioWorker calls back here to addBlockAccess(BlockAccessIndex) from IOWorker.getNextFreeBlock
	 * @return The next free block from the round robin tablespace, translated to a virtual block.
	 * @throws IOException 
	 */
	public BlockAccessIndex getNextFreeBlock(int tblsp) throws IOException {
		synchronized(ioWorker[tblsp]) {
			BlockAccessIndex bai = ioWorker[tblsp].getNextFreeBlock();
			if(DEBUG)
				System.out.printf("%s getNextFree specific Tablespace %d for next free block %s%n", this.getClass().getName(), tblsp, bai);
			return bai;
		}
	}
	
	@Override
	/**
	 * Deallocate the outstanding block and call commit on the recovery log
	 * @throws IOException
	 */
	public void deallocOutstandingCommit() throws IOException {
		if( DEBUG )
			System.out.printf("%s.deallocOutstandingCommit invoking commitBufferFlush and deallocOutstanding...%n",this.getClass().getName());
		commitBufferFlush();
		Fforce();
	}
	
	@Override
	/**
	 * Deallocate the outstanding block and call rollback on the recovery log
	 * @throws IOException
	 */
	public void deallocOutstandingRollback() throws IOException {
		if(DEBUG)
			System.out.printf("%s Rolling back %n",this.getClass().getName());
		synchronized(bufferPool) {
			bufferPool.rollback();
		}
	}
	
	@Override
	/**
	 * dealloc outstanding blocks. if not null, do a dealloc and set null
	 * @throws IOException
	 */
	public void deallocOutstanding(BlockAccessIndex bai) throws IOException {
		synchronized(bufferPool) {
			bufferPool.deallocOutstanding(bai);
		}
	}
	
	@Override
	/**
	 * Deallocate the outstanding block and write it to the log. To be eligible for write it must be incore, have
	 * accesses = 1 (latched) and and NOT yet in log.
	 */
	public void deallocOutstandingWriteLog(BlockAccessIndex lbai) throws IOException {
		synchronized(bufferPool) {
			bufferPool.deallocOutstandingWriteLog(GlobalDBIO.getTablespace(lbai.getBlockNum()), lbai);
		}
	}

 	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#Fopen()
	 */
 	@Override
	public void Fopen() throws IOException {
 			for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
 				synchronized(ioWorker[i]) {
 					if (ioWorker[i] != null && !((IoInterface)ioWorker[i]).isopen()) {
 						((IoInterface)ioWorker[i]).Fopen();
 					}
 				}
 			}
	}
	
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#Fclose()
	 */
	@Override
	public void Fclose() throws IOException {
		Future<?>[] futureArray = new Future<?>[DBPhysicalConstants.DTABLESPACES];
		// queue to each tablespace
		try {
			for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				synchronized(ioWorker[i]) {
					if(((IoInterface)ioWorker[i]).isopen()) {
						futureArray[i] = ThreadPoolManager.getInstance().spin(ioWorker[i].callFclose,ioWorkerNames[i]);
					}
				}
			}
			for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++)
				if (futureArray[i] != null )
					futureArray[i].get();
		} catch (InterruptedException | ExecutionException e) {
			throw new IOException(e);
		}	
	}
	
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#Fforce()
	 */
	@Override
	public void Fforce() throws IOException {
		if( DEBUG )
			System.out.printf("%s.Fforce%n",this.getClass().getName());
		Future<?>[] futureArray = new Future<?>[DBPhysicalConstants.DTABLESPACES];
		// queue to each tablespace
		try {
			for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				synchronized(ioWorker[i]) {
					if(((IoInterface)ioWorker[i]).isopen()) {
						futureArray[i] = ThreadPoolManager.getInstance().spin(ioWorker[i].callFforce,ioWorkerNames[i]);
					}
				}
			}
			for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++)
				if (futureArray[i] != null )
					futureArray[i].get();
		} catch (InterruptedException | ExecutionException e) {
			throw new IOException(e);
		}	
	}
	
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#isNew()
	 */
	@Override
	public boolean isNew() {
		synchronized(ioWorker[0]) {
			return ioWorker[0].isnew();
		}
	}
	
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#getIOWorker(int)
	 */
	public IOWorker getIOWorker(int tblsp) {
		return ioWorker[tblsp];
	}
	
	@Override
	public void forceBufferClear() {
		//for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
		//	blockBuffer[i].forceBufferClear();
		//}
		synchronized(bufferPool) {
			bufferPool.forceBufferClear();
		}
	}
	
	@Override
	public BlockAccessIndex addBlockAccess(BlockAccessIndex blk) throws IOException {
		synchronized(bufferPool) {
			return bufferPool.addBlockAccess(blk);
		}
	}
	
	@Override
	public BlockAccessIndex findOrAddBlockAccess(long bn) throws IOException {
		synchronized(bufferPool) {
			return bufferPool.findOrAddBlockAccess(bn);
		}
	}
	
	/**
	 * Commit the outstanding blocks, wait until the IO requests have finished first
	 */
	@Override
	public void commitBufferFlush() throws IOException {
		if( DEBUG )
			System.out.printf("%s.commitBufferFlush invoked.%n",this.getClass().getName());
		synchronized(bufferPool) {
			bufferPool.commitBufferFlush();
		}
	}
	
	@Override
	public void checkpointBufferFlush() throws IOException, IllegalAccessException {
		if( DEBUG )
			System.out.printf("%s.checkpointBufferFlush invoked.%n",this.getClass().getName());
		synchronized(bufferPool) {
			bufferPool.checkpointBufferFlush();
		}
	}
	/*
	@Override
	public void directBufferWrite() throws IOException {
		if( DEBUG )
			System.out.printf("%s.directBufferWrite invoked.%n",this.getClass().getName());
		//for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
		//	blockBuffer[i].directBufferWrite();
		//}
		bufferPool.directBufferWrite();
	}
	*/
	/**
	* seek_fwd - long seek forward from current spot
	* @param offset offset from current
	* @exception IOException If we cannot acquire next block
	*/
	public boolean seek_fwd(DBInputStream blockStream, int tblsp, long offset) throws IOException {
		synchronized(bufferPool) {
			return bufferPool.seekFwd( blockStream, tblsp, offset);
		}
	}	

	@Override
	public long Fsize(int tblsp) throws IOException {
		synchronized(ioWorker[tblsp]) {
			if( DEBUG )
				System.out.printf("%s.Fsize(%d)%n",this.getClass().getName(),tblsp);
			return ioWorker[tblsp].Fsize();
		}
	}
	
	@Override
	public GlobalDBIO getIO() {
		return globalIO;
	}
		
	@Override
	/**
	 * Perform an Fseek on the block and and write it. Use the write method of Datablock and
	 * the IoWorker for the proper tablespace. Used in final applyChange 
	 * operation of the logAndDo method of the {@link FileLogger} to push the modified block to deep storage.
	 * Used in conjunction with {@link UndoableBlock}.
	 */
	public void writeDirect(int tblsp, long blkn, Datablock blkV2) throws IOException {
		synchronized(ioWorker[tblsp]) {
			((IOWorker) ioWorker[tblsp]).Fseek(GlobalDBIO.getBlock(blkn));
			blkV2.write((IoInterface) ioWorker[tblsp]);
		}
	}
	
	@Override
	/**
	 * Perform an Fseek on the block and read it into the {@code datablock}
	 */
	public void readDirect(int tblsp, long blkn, Datablock blkV2) throws IOException {
		synchronized(ioWorker[tblsp]) {
			((IOWorker) ioWorker[tblsp]).Fseek(GlobalDBIO.getBlock(blkn));
			blkV2.read((IoInterface) ioWorker[tblsp]);
		}
	}
	
	@Override
	/**
	* objseek - seek to offset within block
	* @param adr block/offset to seek to
	* @exception IOException If problem seeking block
	* @see Optr
	*/
	public int objseek(DBInputStream blockStream, Optr adr) throws IOException {
		assert (adr.getBlock() != -1L) : "MultithreadedIOManager objseek Sentinel block seek error";
		synchronized(bufferPool) {
			return bufferPool.objseek(blockStream, adr);
		}
	}
	
	@Override
	/**
	* objseek - seek to offset within block
	* @param adr long block to seek to
	* @return the tablespace extracted from passed pointer
	* @exception IOException If problem seeking block
	* @see Optr
	*/
	public int objseek(DBInputStream blockStream, long adr) throws IOException {
		assert(adr != -1L) : "MultithreadedIOManager objseek Sentinel block seek error";
		synchronized(bufferPool) {
			return bufferPool.objseek(blockStream, adr);
		}
	}
	
	@Override
	public int objseek(DBInputStream blockStream, long tblock, short offset) throws IOException {
		synchronized(bufferPool) {
			return bufferPool.objseek(blockStream, tblock, offset);	
		}
	}
	@Override
	/**
	* objseek - seek to offset within block
	* @param adr block/offset to seek to
	* @exception IOException If problem seeking block
	* @see Optr
	*/
	public int objseek(DBOutputStream blockStream, Optr adr) throws IOException {
		assert (adr.getBlock() != -1L) : "MultithreadedIOManager objseek Sentinel block seek error";
		synchronized(bufferPool) {
			return bufferPool.objseek(blockStream, adr);
		}
	}
	
	@Override
	/**
	* objseek - seek to offset within block
	* @param adr long block to seek to
	* @return the tablespace extracted from passed pointer
	* @exception IOException If problem seeking block
	* @see Optr
	*/
	public int objseek(DBOutputStream blockStream, long adr) throws IOException {
		assert(adr != -1L) : "MultithreadedIOManager objseek Sentinel block seek error";
		synchronized(bufferPool) {
			return bufferPool.objseek(blockStream, adr);
		}
	}
	
	@Override
	public int objseek(DBOutputStream blockStream, long tblock, short offset) throws IOException {
		synchronized(bufferPool) {
			return bufferPool.objseek(blockStream, tblock, offset);	
		}
	}

	@Override
	public RecoveryLogManager getUlog(int tblsp) {
		synchronized(bufferPool) {
			return bufferPool.getUlog(tblsp);
		}
	}
	
	@Override
	public void extend(int ispace, long newLen) throws IOException {
		synchronized(ioWorker[ispace]) {
			ioWorker[ispace].Fset_length(newLen);
		}
	}

	@Override
	public MappedBlockBuffer getBlockBuffer(int tablespace) {
		synchronized(bufferPool) {
			return bufferPool.getBlockBuffer(tablespace);
		}
	}

	/*
	@Override
	public void FseekAndWriteHeader(long offset, Datablock tblk) throws IOException {
		int tblsp = GlobalDBIO.getTablespace(offset);
		ioWorker[tblsp].FseekAndWriteHeader(offset, tblk);
	}

	@Override
	public void FseekAndReadHeader(long offset, Datablock tblk) throws IOException {
		int tblsp = GlobalDBIO.getTablespace(offset);
		ioWorker[tblsp].FseekAndReadHeader(offset, tblk);
	}
	*/
}
