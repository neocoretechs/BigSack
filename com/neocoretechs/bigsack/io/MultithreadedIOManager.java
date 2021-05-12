package com.neocoretechs.bigsack.io;

import java.io.File;
import java.io.IOException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.cluster.IOWorkerInterface;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.BlockStream;
import com.neocoretechs.bigsack.io.pooled.Datablock;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.pooled.ObjectDBIO;

import com.neocoretechs.bigsack.io.request.FSeekAndReadFullyRequest;
import com.neocoretechs.bigsack.io.request.FSeekAndReadRequest;
import com.neocoretechs.bigsack.io.request.FSeekAndWriteFullyRequest;
import com.neocoretechs.bigsack.io.request.FSeekAndWriteRequest;
import com.neocoretechs.bigsack.io.request.FsizeRequest;
import com.neocoretechs.bigsack.io.request.GetNextFreeBlockRequest;
import com.neocoretechs.bigsack.io.request.GetNextFreeBlocksRequest;
import com.neocoretechs.bigsack.io.request.FSyncRequest;
import com.neocoretechs.bigsack.io.request.IoRequestInterface;

/**
 * Handles the aggregation of the IO worker threads of which there is one for each tablespace.
 * Requests are queued to the IO worker assigned to the tablespace desired and can operate in parallel
 * with granularity at the tablespace/randomaccessfile level. This is asynchronous IO on random access files
 * either memory mapped or filesystem.
 * Starts a MappedBlockBuffer and an IOWorker for each tablespace.
 * When we need to cast a global operation which requires all tablespaces to coordinate a response we use
 * the CyclicBarrier class to set up the rendezvous with each IOworker and its particular request to the
 * set of all IO workers. Primarily we are calling into the BufferPool that aggregates the MappedBlockBuffer
 * the Blockstreams and the undo log for each tablespace. We form requests serialized onto queues for many operations.
 * Copyright (C) NeoCoreTechs 2014,2015
 * @author jg
 *
 */
public class MultithreadedIOManager implements IoManagerInterface {
	private static final boolean DEBUG = false;
	private static final boolean DEBUG2 = false;
	private static final boolean DEBUGWRITE = false; // view blocks written to log and store
	public ObjectDBIO globalIO;
	// barrier synch for specific functions, cyclic (reusable)
	protected final CyclicBarrier forceBarrierSynch = new CyclicBarrier(DBPhysicalConstants.DTABLESPACES);
	protected final CyclicBarrier nextBlocksBarrierSynch = new CyclicBarrier(DBPhysicalConstants.DTABLESPACES);
	protected IOWorkerInterface ioWorker[];
	protected BufferPool bufferPool;
	private FreeBlockAllocator alloc;
	protected int L3cache = 0;

	public MultithreadedIOManager(ObjectDBIO globalIO) throws IOException {
		this.globalIO = globalIO;
		ioWorker = new IOWorker[DBPhysicalConstants.DTABLESPACES];
		alloc = new FreeBlockAllocator(this);
		bufferPool = new BufferPool(this);
	}
	/**
	 * Get the allocator of free blocks
	 * @return
	 */
	@Override
	public synchronized FreeBlockAllocator getFreeBlockAllocator() {
		return alloc;
	}
	
	/**
	 * Invoke each tablespace open request by creating buffers and spinning workers.
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#Fopen(java.lang.String, int, boolean)
	 */
	@Override
	public synchronized boolean Fopen(String fname, int L3cache, boolean create) throws IOException {
		this.L3cache = L3cache;
		// Initialize the thread pool group NAMES to spin new threads in controllable batches
		String[] ioWorkerNames = new String[DBPhysicalConstants.DTABLESPACES];
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			ioWorkerNames[i] = "IOWORKER"+translateDb(fname,i);
		}
		ThreadPoolManager.init(ioWorkerNames, false);
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			ioWorker[i] = new IOWorker(translateDb(fname,i), i, L3cache);
			bufferPool.createPool(globalIO, this, i);
			ThreadPoolManager.getInstance().spin((Runnable)ioWorker[i],ioWorkerNames[i]);
		}
		if( create && isNew() ) {
			// init the Datablock arrays, create freepool
			globalIO.createBuckets();
			setNextFreeBlocks();
		} else {
			for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				// attempt recovery if needed
				bufferPool.recover(i);
			}
			// fill in the next free block indicators and set the smallest tablespace
			findSmallestTablespace();
		}
		return true;
	}
	
	/** (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#Fopen(java.lang.String, int, boolean)
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
	@Override
	public synchronized boolean Fopen(String fname, String remote, int L3cache, boolean create) throws IOException {
		this.L3cache = L3cache;
		// Initialize the thread pool group NAMES to spin new threads in controllable batches
		String[] ioWorkerNames = new String[DBPhysicalConstants.DTABLESPACES];
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			ioWorkerNames[i] = "IOWORKER"+translateDb(fname,i);
		}
		ThreadPoolManager.init(ioWorkerNames, false);
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			if( remote == null )
					ioWorker[i] = new IOWorker(translateDb(fname,i), i, L3cache);
			else
					ioWorker[i] = new IOWorker(translateDb(fname,i), translateDb(remote,i), i, L3cache);
			bufferPool.createPool(globalIO, this, i);
			ThreadPoolManager.getInstance().spin((Runnable)ioWorker[i], ioWorkerNames[i]);
		}
		if( create && isNew() ) {
			// init the Datablock arrays, create freepool
			globalIO.createBuckets();
			setNextFreeBlocks();
		} else {
			for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				// attempt recovery if needed
				bufferPool.recover(i);
			}
			// fill in the next free block indicators and set the smallest tablespace
			findSmallestTablespace();
		}
		return true;
	}

	/**
	* Find or add the block to in-mem list.  First deallocate the currently
	* used block, get the new block, then allocate it
	* @param tbn The virtual block
	* @exception IOException If low-level access fails
	*/
	public int findOrAddBlock(long tbn) throws IOException {
		int tblsp = GlobalDBIO.getTablespace(tbn);
		if( DEBUG )
			System.out.println("MultithreadedIoManager.findOrAddBlock tablespace "+tblsp+" pos:"+GlobalDBIO.valueOf(tbn)+" current:"+bufferPool.getBlockStream(tblsp));
		// If the current entry is the one we are looking for, set byteindex to 0 and return
		// if not, call 'dealloc' and find our target
		bufferPool.findOrAddBlock(tblsp, tbn);
		return tblsp;
	}

	/**
	* Get the new position for clustered entries. If we have values associated with 'keys' we store
	* these 'values' in alternate blocks from the BTree page. This method delivers the block to pack
	* For sets vs maps, we store only the serialized keys clustered on the page.
	* We determine location of new node, store in new_node_pos.
	* Attempts to cluster entries in used blocks near insertion point.
	* @param locs The array of previous entries to check for block space
	* @param index The index of the target in array, such that we dont check that
	* @param nkeys The total keys in use to check in array
	* @return The Optr block plus offset in the block pointing to the new node position
	* @exception IOException If we cannot get block for new item
	*/
	public Optr getNewInsertPosition(Optr[] locs, int index, int nkeys, int bytesNeeded) throws IOException {
		if( DEBUG2 )
			System.out.printf("%s.getNewInsertPosition(%s, %d, %d, %d)%n",this.getClass().getName(), locs, index, nkeys, bytesNeeded);
		return MappedBlockBuffer.getNewInsertPosition(this.globalIO, locs, index, nkeys, bytesNeeded);
	}

	/**
	 * Queues a request to the IOWorker to acquire the next free block from the stated tablespace
	 * @param tblsp The target tablespace
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#getNextFreeBlock(int)
	 */
	@Override
	public long getNextFreeBlock(int tblsp) throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.getNextFreeBlock "+tblsp);
		CountDownLatch barrierCount = new CountDownLatch(1);
		IoRequestInterface iori = new GetNextFreeBlockRequest(barrierCount, getFreeBlockAllocator().getNextFree(tblsp));
		iori.setTablespace(tblsp);
		ioWorker[tblsp].queueRequest(iori);
		try {
			barrierCount.await();
		} catch (InterruptedException e) {}
		alloc.setNextFree(tblsp, iori.getLongReturn());
		return iori.getLongReturn();
	}
	/**
	* Return the reverse scan of the first free block of each tablespace
	* queue the request to the proper ioworker, they wait at barrier synch, 
	* then activate countdown latch to signal main.
	* sets the next free blocks in buffer pool
	* @exception IOException if IO problem
	*/
	public void getNextFreeBlocks() throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.getNextFreeBlocks ");
		CountDownLatch barrierCount = new CountDownLatch(DBPhysicalConstants.DTABLESPACES);
		IoRequestInterface[] iori = new IoRequestInterface[DBPhysicalConstants.DTABLESPACES];
		// queue to each tablespace
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			iori[i] = new GetNextFreeBlocksRequest(nextBlocksBarrierSynch, barrierCount);
			iori[i].setTablespace(i);
			ioWorker[i].queueRequest(iori[i]);
		}
		try {
			barrierCount.await();
		} catch (InterruptedException e) {}
		long[] freeArray = new long[DBPhysicalConstants.DTABLESPACES];
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				freeArray[i] = iori[i].getLongReturn();
		}
		alloc.setNextFree(freeArray);
	}
	
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#FseekAndWrite(long, com.neocoretechs.bigsack.io.pooled.Datablock)
	 */
	@Override
	public void FseekAndWrite(long toffset, Datablock tblk) throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.FseekAndWrite "+GlobalDBIO.valueOf(toffset));
		int tblsp = GlobalDBIO.getTablespace(toffset);
		long offset = GlobalDBIO.getBlock(toffset);
		CountDownLatch barrierCount = new CountDownLatch(1);
		IoRequestInterface iori = new FSeekAndWriteRequest(barrierCount, offset, tblk);
		iori.setTablespace(tblsp);
		// no need to wait, let the queue handle serialization
		ioWorker[tblsp].queueRequest(iori);
	}
	
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#FseekAndWriteFully(long, com.neocoretechs.bigsack.io.pooled.Datablock)
	 */
	@Override
	public void FseekAndWriteFully(long toffset, Datablock tblk) throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.FseekAndWriteFully "+GlobalDBIO.valueOf(toffset));
		int tblsp = GlobalDBIO.getTablespace(toffset);
		long offset = GlobalDBIO.getBlock(toffset);
		CountDownLatch barrierCount = new CountDownLatch(1);
		IoRequestInterface iori = new FSeekAndWriteFullyRequest(barrierCount, offset, tblk);
		iori.setTablespace(tblsp);
		ioWorker[tblsp].queueRequest(iori);
	}
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#FseekAndRead(long, com.neocoretechs.bigsack.io.pooled.Datablock)
	 */
	@Override
	public void FseekAndRead(long toffset, Datablock tblk) throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.FseekAndRead "+GlobalDBIO.valueOf(toffset));
		//if( GlobalDBIO.valueOf(toffset).equals("Tablespace_1_114688"))
		//	System.out.println("MultithreadedIOManager.FseekAndRead Tablespace_1_114688");
		int tblsp = GlobalDBIO.getTablespace(toffset);
		long offset = GlobalDBIO.getBlock(toffset);
		CountDownLatch barrierCount = new CountDownLatch(1);
		IoRequestInterface iori = new FSeekAndReadRequest(barrierCount, offset, tblk);
		iori.setTablespace(tblsp);
		ioWorker[tblsp].queueRequest(iori);
		try {
			barrierCount.await();
		} catch (InterruptedException e) {}

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
	 */
	@Override
	public void FseekAndReadFully(long toffset, Datablock tblk) throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.FseekAndReadFully "+GlobalDBIO.valueOf(toffset));
		int tblsp = GlobalDBIO.getTablespace(toffset);
		long offset = GlobalDBIO.getBlock(toffset);
		CountDownLatch barrierCount = new CountDownLatch(1);
		IoRequestInterface iori = new FSeekAndReadFullyRequest(barrierCount, offset, tblk);
		iori.setTablespace(tblsp);
		ioWorker[tblsp].queueRequest(iori);
		try {
			barrierCount.await();
		} catch (InterruptedException e) {}
	}
	
	/**
	 * Set the initial free blocks after a create of the tablespaces.
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#setNextFreeBlocks()
	 */
	@Override
	public void setNextFreeBlocks() {
		alloc.setNextFreeBlocks();
	}

	/**
	 * Return that which is regarded as the first tablespace, usually 0, location of root node.
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#firstTableSpace()
	 */
	@Override
	public long firstTableSpace() throws IOException {
		return 0L;
	}

	/**
	 * Use the parallel getNextFreeBlocks to return the free array, then find
	 * the smallest element in the array. utility method since we use simple random numbers to
	 * tablespaces for new blocks
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#findSmallestTablespace()
	 */
	@Override
	public int findSmallestTablespace() throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.findSmallestTablespace ");
		getNextFreeBlocks(); // sets blocks in feeeblockallocator
		return alloc.nextTablespace();	
	}

	/**
	 * Deallocate the outstanding block and call commit on the recovery log
	 * @throws IOException
	 */
	public synchronized void deallocOutstandingCommit() throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.deallocOutstandingCommit invoked.");
		commitBufferFlush(); // calls iomanager
		deallocOutstanding();
	}
	/**
	 * Deallocate the outstanding block and call rollback on the recovery log
	 * @throws IOException
	 */
	public synchronized void deallocOutstandingRollback() throws IOException {
			for(int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
					//if(
						bufferPool.rollback(i); 
					//)
					// TODO: Do we really wan this Fclose?
					//((IoInterface)ioWorker[i]).Fclose();
			}
	}
	/**
	 * dealloc outstanding blocks. if not null, do a dealloc and set null
	 * @throws IOException
	 */
	public void deallocOutstanding() throws IOException {
			bufferPool.deallocOutstanding();
	}
	/**
	 * Deallocate the outstanding BlockAccesIndex from the virtual block number. First
	 * check the outstanding buffers per tablespace, then consult the block buffer failing that.
	 * If we find nothing in the buffers, there is obviously nothing to unlatch.
	 * @param pos The virtual block number to deallocate.
	 * @throws IOException
	 */
	public void deallocOutstanding(long pos) throws IOException {
		bufferPool.deallocOutstanding(pos);
	}
	
	/**
	 * Deallocate the outstanding block and write it to the log. To be eligible for write it must be incore, have
	 * accesses = 1 (latched) and and NOT yet in log.
	 */
	public void deallocOutstandingWriteLog(int tablespace, BlockAccessIndex lbai) throws IOException {
		bufferPool.deallocOutstandingWriteLog(tablespace, lbai);
	}
		
	protected String translateDb(String dbname, int tablespace) {
		String db;
        // replace any marker of $ with tablespace number
        if( dbname.indexOf('$') != -1) {
        	db = dbname.replace('$', String.valueOf(tablespace).charAt(0));
        } else
        	db = dbname;
        db = (new File(db)).toPath().getParent().toString() + File.separator +
        		"tablespace"+String.valueOf(tablespace) + File.separator +
        		(new File(dbname).getName());
        return db;
	}
	
 	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#Fopen()
	 */
 	@Override
	public synchronized void Fopen() throws IOException {
 			for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++)
 				if (ioWorker[i] != null && !((IoInterface)ioWorker[i]).isopen())
 					((IoInterface)ioWorker[i]).Fopen();
	}
	
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#Fclose()
	 */
	@Override
	public synchronized void Fclose() throws IOException {
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++)
			if (ioWorker[i] != null && ((IoInterface)ioWorker[i]).isopen()) {
				if( ioWorker[i].getRequestQueueLength() == 0 )
					((IoInterface)ioWorker[i]).Fclose();
				else
					throw new IOException("Attempt to close tablespace with outstanding requests");
			}
	}
	
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#Fforce()
	 */
	@Override
	public synchronized void Fforce() throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.Fforce ");
		CountDownLatch barrierCount = new CountDownLatch(DBPhysicalConstants.DTABLESPACES);
		IoRequestInterface[] iori = new IoRequestInterface[DBPhysicalConstants.DTABLESPACES];
		// queue to each tablespace
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				iori[i] = new FSyncRequest(forceBarrierSynch, barrierCount);
				ioWorker[i].queueRequest(iori[i]);
		}
		try {
				barrierCount.await();
		} catch (InterruptedException e) {}
	}
	
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#isNew()
	 */
	@Override
	public boolean isNew() {
			return ((IoInterface)ioWorker[0]).isnew();
	}
	
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#getIOWorker(int)
	 */
	@Override
	public IOWorkerInterface getIOWorker(int tblsp) {
		return ioWorker[tblsp];
	}
	@Override
	public void forceBufferClear() {
		//for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
		//	blockBuffer[i].forceBufferClear();
		//}
		bufferPool.forceBufferClear();
	}
	/**
	 * Load up a block from the freelist with the assumption that it will be filled in later. Do not 
	 * check for whether it should be logged,etc. As part of the 'acquireblock' process, this takes place. Latch it
	 * as soon as possible though. Queue a request to the MappedBlockBuffer IoManager to do this. Await the countdownlatch to continue.
	 */
	@Override
	public BlockAccessIndex addBlockAccessNoRead(Long Lbn) throws IOException {
		return bufferPool.addBlockAccessNoRead(Lbn);
	}
	
	@Override
	public BlockAccessIndex findOrAddBlockAccess(long bn) throws IOException {
		return bufferPool.findOrAddBlockAccess(bn);
	}
	
	@Override
	public BlockAccessIndex getUsedBlock(long loc) {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.getUsedBlock "+GlobalDBIO.valueOf(loc));
		//int tblsp = GlobalDBIO.getTablespace(loc);
		//return blockBuffer[tblsp].getUsedBlock(loc);
		return bufferPool.getUsedBlock(loc);
	}

	/**
	 * Commit the outstanding blocks, wait until the IO requests have finished first
	 */
	@Override
	public void commitBufferFlush() throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.commitBufferFlush invoked.");
		bufferPool.commmitBufferFlush(ioWorker);
	}
	
	@Override
	public void directBufferWrite() throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.directBufferWrite invoked.");
		//for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
		//	blockBuffer[i].directBufferWrite();
		//}
		bufferPool.directBufferWrite();
	}
	
	/**
	* seek_fwd - long seek forward from current spot
	* @param offset offset from current
	* @exception IOException If we cannot acquire next block
	*/
	public boolean seek_fwd(int tblsp, long offset) throws IOException {
		return bufferPool.seekFwd(tblsp, offset);
	}
	

	@Override
	public long Fsize(int tblsp) throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.fsize for tablespace "+tblsp);
		CountDownLatch cdl = new CountDownLatch(1);
		FsizeRequest abanrr = new FsizeRequest(cdl);
		abanrr.setTablespace(tblsp);
		ioWorker[tblsp].queueRequest(abanrr);
		try {
			cdl.await();
			return abanrr.getLongReturn();
		} catch (InterruptedException e) {
			// shutdown waiting for return
			return -1L;
		}
	}
	@Override
	public ObjectDBIO getIO() {
		return globalIO;
	}
	
	@Override
	public void writen(int tblsp, byte[] o, int osize) throws IOException {
		bufferPool.writen(tblsp, o, osize);
	}
	
	@Override
	public int deleten(Optr adr, int osize) throws IOException {
		return bufferPool.deleten(adr, osize);
	}
	
	@Override
	public void deallocOutstandingWriteLog(int tblsp) throws IOException {
		bufferPool.deallocOutstandingWriteLog(tblsp);
		if( DEBUGWRITE ) {
			System.out.println("MultithreadedIOManager.deallocOutstandingWriteLog:"+bufferPool.getBlockStream(tblsp).getBlockAccessIndex());
		}
	}
	
	public void writeDirect(int tblsp, long blkn, Datablock blkV2) throws IOException {
		synchronized(ioWorker[tblsp]) {
			((IOWorker) ioWorker[tblsp]).Fseek(blkn);
			blkV2.write((IoInterface) ioWorker[tblsp]);
		}
	}
	
	public void readDirect(int tblsp, long blkn, Datablock blkV2) throws IOException {
		synchronized(ioWorker[tblsp]) {
			((IOWorker) ioWorker[tblsp]).Fseek(blkn);
			blkV2.read((IoInterface) ioWorker[tblsp]);
		}
	}
	/**
	* objseek - seek to offset within block
	* @param adr block/offset to seek to
	* @exception IOException If problem seeking block
	* @see Optr
	*/
	public int objseek(Optr adr) throws IOException {
		assert (adr.getBlock() != -1L) : "MultithreadedIOManager objseek Sentinel block seek error";
		return bufferPool.objseek(adr);
	}
	/**
	* objseek - seek to offset within block
	* @param adr long block to seek to
	* @return the tablespace extracted from passed pointer
	* @exception IOException If problem seeking block
	* @see Optr
	*/
	public int objseek(long adr) throws IOException {
		assert(adr != -1L) : "MultithreadedIOManager objseek Sentinel block seek error";
		return bufferPool.objseek(adr);
	}
	@Override
	public RecoveryLogManager getUlog(int tblsp) {
		return bufferPool.getUlog(tblsp);
	}
	@Override
	public MappedBlockBuffer getBlockBuffer(int tblsp) {
		return bufferPool.getBlockBuffer(tblsp);
	}
	@Override
	public BlockStream getBlockStream(int tblsp) {
		return bufferPool.getBlockStream(tblsp);
	}
	@Override
	public BufferPool getBufferPool() {
		return bufferPool;
	}

}
