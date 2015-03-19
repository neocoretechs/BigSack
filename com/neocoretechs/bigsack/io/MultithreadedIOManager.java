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
import com.neocoretechs.bigsack.io.pooled.MappedBlockBuffer;
import com.neocoretechs.bigsack.io.pooled.ObjectDBIO;
import com.neocoretechs.bigsack.io.request.CommitRequest;
import com.neocoretechs.bigsack.io.request.FSeekAndReadFullyRequest;
import com.neocoretechs.bigsack.io.request.FSeekAndReadRequest;
import com.neocoretechs.bigsack.io.request.FSeekAndWriteFullyRequest;
import com.neocoretechs.bigsack.io.request.FSeekAndWriteRequest;
import com.neocoretechs.bigsack.io.request.FsizeRequest;
import com.neocoretechs.bigsack.io.request.GetNextFreeBlockRequest;
import com.neocoretechs.bigsack.io.request.GetNextFreeBlocksRequest;
import com.neocoretechs.bigsack.io.request.FSyncRequest;
import com.neocoretechs.bigsack.io.request.IoRequestInterface;
import com.neocoretechs.bigsack.io.request.iomanager.AddBlockAccessNoReadRequest;
import com.neocoretechs.bigsack.io.request.iomanager.DirectBufferWriteRequest;
import com.neocoretechs.bigsack.io.request.iomanager.FindOrAddBlockAccessRequest;
import com.neocoretechs.bigsack.io.request.iomanager.ForceBufferClearRequest;
import com.neocoretechs.bigsack.io.request.iomanager.GetUsedBlockRequest;

/**
 * Handles the aggregation of the IO worker threads of which there is one for each tablespace.
 * Requests are queued to the IO worker assigned to the tablespace desired and can operate in parallel
 * with granularity at the tablespace/randomaccessfile level. This is asynchronous IO on random access files
 * either memory mapped or filesystem.
 * When we need to cast a global operation which requires all tablespaces to coordinate a response we use
 * the CyclicBarrier class to set up the rendezvous with each IOworker and its particular request to the
 * set of all IO workers.
 * TODO: REFACTOR IoManagerInterface common methods between mutlithreaded and cluster to abstract class they inherit
 * Copyright (C) NeoCoreTechs 2014
 * @author jg
 *
 */
public class MultithreadedIOManager implements IoManagerInterface {
	private static final boolean DEBUG = false;
	public ObjectDBIO globalIO;
	// barrier synch for specific functions, cyclic (reusable)
	final CyclicBarrier forceBarrierSynch = new CyclicBarrier(DBPhysicalConstants.DTABLESPACES);
	final CyclicBarrier nextBlocksBarrierSynch = new CyclicBarrier(DBPhysicalConstants.DTABLESPACES);
	final CyclicBarrier commitBarrierSynch = new CyclicBarrier(DBPhysicalConstants.DTABLESPACES);
	final CyclicBarrier directWriteBarrierSynch = new CyclicBarrier(DBPhysicalConstants.DTABLESPACES);
	protected IOWorkerInterface ioWorker[];
	protected int L3cache = 0;
	protected long[] nextFree = new long[DBPhysicalConstants.DTABLESPACES];
	private MappedBlockBuffer[] blockBuffer; // block number to Datablock
	private RecoveryLogManager[] ulog;
	protected BlockStream[] lbai = new BlockStream[DBPhysicalConstants.DTABLESPACES];
	/*
	protected Datablock getBlk(int i) { return lbai[i].getLbai().getBlk(); }
	public short getByteindex(int i) { return lbai[i].getLbai().getByteindex(); }
	protected void moveByteindex(int i, short runcount) { 
		lbai[i].getLbai().setByteindex((short) (lbai[i].getLbai().getByteindex() + runcount)); 
	}
	protected void incrementByteindex(int i) { 
		lbai[i].getLbai().setByteindex((short) (lbai[i].getLbai().getByteindex() + 1)); 
	}
	
	protected BlockAccessIndex getBlockIndex(int i) { return lbai[i].getLbai();}
	*/
	public MultithreadedIOManager(ObjectDBIO globalIO) throws IOException {
		this.globalIO = globalIO;
		ioWorker = new IOWorker[DBPhysicalConstants.DTABLESPACES];
		blockBuffer = new MappedBlockBuffer[DBPhysicalConstants.DTABLESPACES];
		ulog = new RecoveryLogManager[DBPhysicalConstants.DTABLESPACES];
		// Initialize the thread pool group NAMES to spin new threads in controllable batches
		ThreadPoolManager.init(new String[]{"BLOCKPOOL","IOWORKER"});
		setNextFreeBlocks();
	}
	/**
	 * Return the MappedBlockBuffer for the tablespace
	 */
	public MappedBlockBuffer getBlockBuffer(int tblsp) { return blockBuffer[tblsp]; }
	/**
	 * Return the block access index and db buffered stream for this tablespace
	 * @param tblsp
	 * @return
	 */
	public BlockStream getBlockStream(int tblsp) { return lbai[tblsp]; }
	
	public RecoveryLogManager getUlog(int tablespace) {
		return ulog[tablespace];
	}
	
	public void setUlog(int tablespace, RecoveryLogManager ulog) {
		this.ulog[tablespace] = ulog;
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
			System.out.println("MultithreadedIoManager.findOrAddBlock tablespace "+tblsp+" pos:"+GlobalDBIO.valueOf(tbn)+" current:"+lbai[tblsp]);
		// If the current entry is the one we are looking for, set byteindex to 0 and return
		// if not, call 'dealloc' and find our target
		lbai[tblsp].setLbai(blockBuffer[tblsp].findOrAddBlock(tbn));
		return tblsp;
	}

	public int deleten(Optr adr, int osize) throws IOException {
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
	public int objseek(Optr adr) throws IOException {
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
	public int objseek(long adr) throws IOException {
		assert(adr != -1L) : "MultithreadedIOManager objseek Sentinel block seek error";
		int tblsp = findOrAddBlock(adr);
		lbai[tblsp].getLbai().setByteindex((short) 0);
		return tblsp;
	}

	/**
	* Get the new node position for clustered entries. If we have values associated with keys we store
	* the values in alternate blocks from the BTree page. This method delivers the block to pack
	* For sets vs maps, we store only the serialized keys clustered on the page
	* determine location of new node, store in new_node_pos.
	* Attempts to cluster entries in used blocks near insertion point
	* @return The Optr pointing to the new node position
	* @exception IOException If we cannot get block for new node
	*/
	public synchronized Optr getNewNodePosition(int tblsp) throws IOException {
		return blockBuffer[tblsp].getNewNodePosition(lbai[tblsp].getLbai());
	}
	public synchronized void setNewNodePosition(long newPos) {
	}
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#getNextFreeBlock(int)
	 */
	@Override
	public synchronized long getNextFreeBlock(int tblsp) throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.getNextFreeBlock "+tblsp);
		CountDownLatch barrierCount = new CountDownLatch(1);
		IoRequestInterface iori = new GetNextFreeBlockRequest(barrierCount, nextFree[tblsp]);
		ioWorker[tblsp].queueRequest(iori);
		try {
			barrierCount.await();
		} catch (InterruptedException e) {}
		nextFree[tblsp] = iori.getLongReturn();
		return nextFree[tblsp];
	}
	/**
	* Return the reverse scan of the first free block of each tablespace
	* queue the request to the proper ioworker, they wait at barrier synch, 
	* then activate countdown latch to signal main. Result in placed in class level nextFree
	* @exception IOException if IO problem
	*/
	private void getNextFreeBlocks() throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.getNextFreeBlocks ");
		CountDownLatch barrierCount = new CountDownLatch(DBPhysicalConstants.DTABLESPACES);
		IoRequestInterface[] iori = new IoRequestInterface[DBPhysicalConstants.DTABLESPACES];
		// queue to each tablespace
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			iori[i] = new GetNextFreeBlocksRequest(nextBlocksBarrierSynch, barrierCount);
			ioWorker[i].queueRequest(iori[i]);
		}
		try {
			barrierCount.await();
		} catch (InterruptedException e) {}
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			nextFree[i] = iori[i].getLongReturn();
		}

	}
	
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#FseekAndWrite(long, com.neocoretechs.bigsack.io.pooled.Datablock)
	 */
	@Override
	public synchronized void FseekAndWrite(long toffset, Datablock tblk) throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.FseekAndWrite "+toffset);
		int tblsp = GlobalDBIO.getTablespace(toffset);
		long offset = GlobalDBIO.getBlock(toffset);
		CountDownLatch barrierCount = new CountDownLatch(1);
		IoRequestInterface iori = new FSeekAndWriteRequest(barrierCount, offset, tblk);
		// no need to wait, let the queue handle serialization
		ioWorker[tblsp].queueRequest(iori);
	}
	
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#FseekAndWriteFully(long, com.neocoretechs.bigsack.io.pooled.Datablock)
	 */
	@Override
	public synchronized void FseekAndWriteFully(long toffset, Datablock tblk) throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.FseekAndWriteFully "+toffset);
		int tblsp = GlobalDBIO.getTablespace(toffset);
		long offset = GlobalDBIO.getBlock(toffset);
		CountDownLatch barrierCount = new CountDownLatch(1);
		IoRequestInterface iori = new FSeekAndWriteFullyRequest(barrierCount, offset, tblk);
		ioWorker[tblsp].queueRequest(iori);
	}
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#FseekAndRead(long, com.neocoretechs.bigsack.io.pooled.Datablock)
	 */
	@Override
	public synchronized void FseekAndRead(long toffset, Datablock tblk) throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.FseekAndRead "+GlobalDBIO.valueOf(toffset));
		//if( GlobalDBIO.valueOf(toffset).equals("Tablespace_1_114688"))
		//	System.out.println("MultithreadedIOManager.FseekAndRead Tablespace_1_114688");
		int tblsp = GlobalDBIO.getTablespace(toffset);
		long offset = GlobalDBIO.getBlock(toffset);
		CountDownLatch barrierCount = new CountDownLatch(1);
		IoRequestInterface iori = new FSeekAndReadRequest(barrierCount, offset, tblk);
		ioWorker[tblsp].queueRequest(iori);
		try {
			barrierCount.await();
		} catch (InterruptedException e) {}

		//if( GlobalDBIO.valueOf(toffset).equals("Tablespace_1_114688"))
		//	System.out.println("MultithreadedIOManager.FseekAndRead EXIT Tablespace_1_114688 "+tblk+" dump:"+tblk.blockdump());
		//assert(tblk.getBytesused() != 0 && tblk.getBytesinuse() != 0) : "MultithreadedIOManager.FseekAndRead returned unusable block from offset "+GlobalDBIO.valueOf(toffset)+" "+tblk.blockdump();
	}
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#FseekAndReadFully(long, com.neocoretechs.bigsack.io.pooled.Datablock)
	 */
	@Override
	public synchronized void FseekAndReadFully(long toffset, Datablock tblk) throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.FseekAndReadFully "+toffset);
		int tblsp = GlobalDBIO.getTablespace(toffset);
		long offset = GlobalDBIO.getBlock(toffset);
		CountDownLatch barrierCount = new CountDownLatch(1);
		IoRequestInterface iori = new FSeekAndReadFullyRequest(barrierCount, offset, tblk);
		ioWorker[tblsp].queueRequest(iori);
		try {
			barrierCount.await();
		} catch (InterruptedException e) {}
	}
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#setNextFreeBlocks()
	 */
	@Override
	public synchronized void setNextFreeBlocks() {
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++)
			if (i == 0)
				nextFree[i] = ((long) DBPhysicalConstants.DBLOCKSIZ);
			else
				nextFree[i] = 0L;
	}

	/* (non-Javadoc)
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
	public synchronized int findSmallestTablespace() throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.findSmallestTablespace ");
		// always make sure we have primary
		long primarySize = Fsize(0);
		int smallestTablespace = 0; // default main
		long smallestSize = primarySize;
		getNextFreeBlocks();
		for (int i = 0; i < nextFree.length; i++) {
			if(nextFree[i] != -1 && GlobalDBIO.getBlock(nextFree[i]) < smallestSize) {
				smallestSize = GlobalDBIO.getBlock(nextFree[i]);
				smallestTablespace = i;
			}
		}
		return smallestTablespace;
	}
	
	/**
	 * Invoke each tablespace open request by creating buffers and spinning workers.
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#Fopen(java.lang.String, int, boolean)
	 */
	@Override
	public synchronized boolean Fopen(String fname, int L3cache, boolean create) throws IOException {
		this.L3cache = L3cache;
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			ioWorker[i] = new IOWorker(translateDb(fname,i), i, L3cache);
			blockBuffer[i] = new MappedBlockBuffer(this, i);
			lbai[i] = new BlockStream(i, blockBuffer[i]);
			ulog[i] = new RecoveryLogManager(globalIO,i);
			ThreadPoolManager.getInstance().spin((Runnable)ioWorker[i],"IOWORKER");
			ThreadPoolManager.getInstance().spin(blockBuffer[i], "BLOCKPOOL");
			// attempt recovery if needed
			ulog[i].getLogToFile().recover();
		}
		// fill in the next free block indicators and set the smallest tablespace
		findSmallestTablespace();
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
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			if( remote == null )
					ioWorker[i] = new IOWorker(translateDb(fname,i), i, L3cache);
			else
					ioWorker[i] = new IOWorker(translateDb(fname,i), translateDb(remote,i), i, L3cache);
			blockBuffer[i] = new MappedBlockBuffer(this, i);
			lbai[i] = new BlockStream(i, blockBuffer[i]);
			ulog[i] = new RecoveryLogManager(globalIO,i);
			ThreadPoolManager.getInstance().spin((Runnable)ioWorker[i], "IOWORKER");
			ThreadPoolManager.getInstance().spin(blockBuffer[i], "BLOCKPOOL");
			// attempt recovery if needed
			ulog[i].getLogToFile().recover();
		}
		// fill in the next free block indicators and set the smallest tablespace
		findSmallestTablespace();
		return true;
	}
	/**
	 * Deallocate the outstanding block and call commit on the recovery log
	 * @throws IOException
	 */
	public synchronized void deallocOutstandingCommit() throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.deallocOutstandingCommit invoked.");
		deallocOutstanding();
		commitBufferFlush(); // calls iomanager
	}
	/**
	 * Deallocate the outstanding block and call rollback on the recovery log
	 * @throws IOException
	 */
	public void deallocOutstandingRollback() throws IOException {
		synchronized( lbai ) {
			for(int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				lbai[i].getLbai().decrementAccesses();
				forceBufferClear();
				ulog[i].rollBack();
				ulog[i].stop();
				((IoInterface)ioWorker[i]).Fclose();
			}
		}
	}
	/**
	 * dealloc outstanding blocks. if not null, do a dealloc and set null
	 * @throws IOException
	 */
	public void deallocOutstanding() throws IOException {
		synchronized(lbai) {
			for(int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				if( lbai[i] != null )
					lbai[i].getLbai().decrementAccesses();
			}	
		}
	}
	
	public void deallocOutstandingWriteLog(int tablespace, BlockAccessIndex lbai) throws IOException {
		if( lbai.getAccesses() == 1 &&  lbai.getBlk().isIncore() &&  !lbai.getBlk().isInlog()) {
				// will set incore, inlog, and push to raw store via applyChange of Loggable
				ulog[tablespace].writeLog(lbai); 
		}
	}
		
	private String translateDb(String dbname, int tablespace) {
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
	public void Fopen() throws IOException {
 		synchronized(ioWorker) {
 			for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++)
 				if (ioWorker[i] != null && !((IoInterface)ioWorker[i]).isopen())
 					((IoInterface)ioWorker[i]).Fopen();
 		}
	}
	
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#Fclose()
	 */
	@Override
	public void Fclose() throws IOException {
		synchronized(ioWorker) {
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++)
			if (ioWorker[i] != null && ((IoInterface)ioWorker[i]).isopen()) {
				if( ioWorker[i].getRequestQueueLength() == 0 )
					((IoInterface)ioWorker[i]).Fclose();
				else
					throw new IOException("Attempt to close tablespace with outstanding requests");
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#Fforce()
	 */
	@Override
	public void Fforce() throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.Fforce ");
		synchronized(ioWorker) {
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
	}
	
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#isNew()
	 */
	@Override
	public boolean isNew() {
		synchronized(ioWorker) {
			return ((IoInterface)ioWorker[0]).isnew();
		}
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
		synchronized(blockBuffer) {
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
	}
	/**
	 * Load up a block from the freelist with the assumption that it will be filled in later. Do not 
	 * check for whether it should be logged,etc. As part of the 'acquireblock' process, this takes place. Latch it
	 * as soon as possible though. Queue a request to the MappedBlockBuffer IoManager to do this. Await the countdownlatch to continue.
	 */
	@Override
	public BlockAccessIndex addBlockAccessNoRead(Long Lbn) throws IOException {
		int tblsp = GlobalDBIO.getTablespace(Lbn);
		//return blockBuffer[tblsp].addBlockAccessNoRead(Lbn);
		CountDownLatch cdl = new CountDownLatch(1);
		AddBlockAccessNoReadRequest abanrr = new AddBlockAccessNoReadRequest(blockBuffer[tblsp], cdl, Lbn);
		blockBuffer[tblsp].queueRequest(abanrr);
		try {
			cdl.await();
			return (BlockAccessIndex) abanrr.getObjectReturn();
		} catch (InterruptedException e) {
			// shutdown waiting for return
			return null;
		}
	}
	@Override
	public BlockAccessIndex findOrAddBlockAccess(long bn) throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.findOrAddBlockAccess "+GlobalDBIO.valueOf(bn));
		
		//int tblsp = GlobalDBIO.getTablespace(bn);
		//return blockBuffer[tblsp].findOrAddBlockAccess(bn);
		int tblsp = GlobalDBIO.getTablespace(bn);
		CountDownLatch cdl = new CountDownLatch(1);
		FindOrAddBlockAccessRequest abanrr = new FindOrAddBlockAccessRequest(blockBuffer[tblsp], cdl, bn);
		blockBuffer[tblsp].queueRequest(abanrr);
		try {
			cdl.await();
			if( DEBUG )
				System.out.println("MultithreadedIOManager.findOrAddBlockAccess "+(BlockAccessIndex) abanrr.getObjectReturn());
			lbai[tblsp].setLbai(((BlockAccessIndex) abanrr.getObjectReturn()));
			return (BlockAccessIndex) abanrr.getObjectReturn();
		} catch (InterruptedException e) {
			// shutdown waiting for return
			return null;
		}
	}
	@Override
	public BlockAccessIndex getUsedBlock(long loc) {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.getUsedBlock "+GlobalDBIO.valueOf(loc));
		//int tblsp = GlobalDBIO.getTablespace(loc);
		//return blockBuffer[tblsp].getUsedBlock(loc);
		int tblsp = GlobalDBIO.getTablespace(loc);
		CountDownLatch cdl = new CountDownLatch(1);
		GetUsedBlockRequest abanrr = new GetUsedBlockRequest(blockBuffer[tblsp], cdl, loc);
		blockBuffer[tblsp].queueRequest(abanrr);
		try {
			cdl.await();
			return (BlockAccessIndex) abanrr.getObjectReturn();
		} catch (InterruptedException e) {
			// shutdown waiting for return
			return null;
		}
	}

	/**
	 * Commit the outstanding blocks, wait until the IO requests have finished first
	 */
	@Override
	public void commitBufferFlush() throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.commitBufferFlush invoked.");
		CountDownLatch barrierCount = new CountDownLatch(DBPhysicalConstants.DTABLESPACES);
		// queue to each tablespace
		synchronized(ioWorker) {
			for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				IoRequestInterface iori  = new CommitRequest(blockBuffer[i], ulog[i], commitBarrierSynch, barrierCount);
				ioWorker[i].queueRequest(iori);
			}
			try {
				barrierCount.await();
			} catch (InterruptedException e) {}
		}
	}
	
	@Override
	public void directBufferWrite() throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.directBufferWrite invoked.");
		//for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
		//	blockBuffer[i].directBufferWrite();
		//}
		CountDownLatch cdl = new CountDownLatch( DBPhysicalConstants.DTABLESPACES);
		synchronized(blockBuffer) {
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
	}
	
	/**
	* seek_fwd - long seek forward from current spot
	* @param offset offset from current
	* @exception IOException If we cannot acquire next block
	*/
	public boolean seek_fwd(int tblsp, long offset) throws IOException {
		return blockBuffer[tblsp].seek_fwd(lbai[tblsp].getLbai(), offset);
	}
	

	@Override
	public long Fsize(int tblsp) throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.fsize "+tblsp);
		CountDownLatch cdl = new CountDownLatch(1);
		FsizeRequest abanrr = new FsizeRequest(cdl);
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
		blockBuffer[tblsp].writen(lbai[tblsp].getLbai(), o, osize);
	}
	@Override
	public IoInterface getDirectIO(int tblsp) {
		return (IoInterface)ioWorker[tblsp];
	}
	@Override
	public void deallocOutstandingWriteLog(int tblsp) throws IOException {
		deallocOutstandingWriteLog(tblsp, lbai[tblsp].getLbai());
		
	}

}
