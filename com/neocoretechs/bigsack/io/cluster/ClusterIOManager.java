package com.neocoretechs.bigsack.io.cluster;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.btree.BTreeMain;
import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.IoManagerInterface;
import com.neocoretechs.bigsack.io.ThreadPoolManager;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.Datablock;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.pooled.MappedBlockBuffer;
import com.neocoretechs.bigsack.io.request.cluster.AbstractClusterWork;
import com.neocoretechs.bigsack.io.request.cluster.CommitRequest;
import com.neocoretechs.bigsack.io.request.cluster.CompletionLatchInterface;
import com.neocoretechs.bigsack.io.request.cluster.FSeekAndReadFullyRequest;
import com.neocoretechs.bigsack.io.request.cluster.FSeekAndReadRequest;
import com.neocoretechs.bigsack.io.request.cluster.FSeekAndWriteFullyRequest;
import com.neocoretechs.bigsack.io.request.cluster.FSeekAndWriteRequest;
import com.neocoretechs.bigsack.io.request.cluster.FSizeRequest;
import com.neocoretechs.bigsack.io.request.cluster.GetNextFreeBlockRequest;
import com.neocoretechs.bigsack.io.request.cluster.GetNextFreeBlocksRequest;
import com.neocoretechs.bigsack.io.request.cluster.FSyncRequest;
import com.neocoretechs.bigsack.io.request.cluster.IsNewRequest;
import com.neocoretechs.bigsack.io.request.cluster.RemoteCommitRequest;
import com.neocoretechs.bigsack.io.request.iomanager.AddBlockAccessNoReadRequest;

import com.neocoretechs.bigsack.io.request.iomanager.DirectBufferWriteRequest;
import com.neocoretechs.bigsack.io.request.iomanager.FindOrAddBlockAccessRequest;
import com.neocoretechs.bigsack.io.request.iomanager.ForceBufferClearRequest;
import com.neocoretechs.bigsack.io.request.iomanager.FreeupBlockRequest;
import com.neocoretechs.bigsack.io.request.iomanager.GetUsedBlockRequest;
import com.neocoretechs.bigsack.io.request.IoRequestInterface;


/**
 * Handles the aggregation of the IO worker threads of which there is one for each tablespace.
 * In this incarnation the IOWorkers are subclassed to UDPMaster which handles traffic down to 
 * UDPWorkers through the command to start via the WorkBoot node
 * When we need to cast a global operation which requires all tablespaces to coordinate a response we use
 * the CyclicBarrier class to set up the rendezvous with each IOworker and its particular request to the
 * set of all IO workers
 * Copyright (C) NeoCoreTechs 2014
 * @author jg
 *
 */
public final class ClusterIOManager implements IoManagerInterface {
	private static final boolean DEBUG = false;
	private GlobalDBIO globalIO;
	protected DistributedIOWorker ioWorker[];
	protected int L3cache = 0;
	protected long[] nextFree = new long[DBPhysicalConstants.DTABLESPACES];
	private static int currentPort = 10000; // starting UDP port, increments as assigned
	private static int messageSeq = 0; // monotonically increasing request id
	// barrier synch for specific functions, cyclic (reusable)
	final CyclicBarrier forceBarrierSynch = new CyclicBarrier(DBPhysicalConstants.DTABLESPACES);
	final CyclicBarrier nextBlocksBarrierSynch = new CyclicBarrier(DBPhysicalConstants.DTABLESPACES);
	final CyclicBarrier commitBarrierSynch = new CyclicBarrier(DBPhysicalConstants.DTABLESPACES);
	final CyclicBarrier directWriteBarrierSynch = new CyclicBarrier(DBPhysicalConstants.DTABLESPACES);
	private MappedBlockBuffer[] blockBuffer; // block number to Datablock
	/**
	 * Instantiate our master node array per database that communicate with our worker nodes
	 * @throws IOException 
	 */
	public ClusterIOManager(GlobalDBIO globalIO) throws IOException {
		this.globalIO = globalIO;
		//ioWorker = new UDPMaster[DBPhysicalConstants.DTABLESPACES];
		ioWorker = new DistributedIOWorker[DBPhysicalConstants.DTABLESPACES];
		blockBuffer = new MappedBlockBuffer[DBPhysicalConstants.DTABLESPACES];
		// create master buffers for each tablespace
		ThreadPoolManager.init(new String[]{"BLOCKPOOL"});
		for(int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			blockBuffer[i] = new MappedBlockBuffer(globalIO, i);
			ThreadPoolManager.getInstance().spin(blockBuffer[i], "BLOCKPOOL");
		}
	}
	
	/**
	* Return the first available block that can be acquired for write
	* queue the request to the proper ioworker
	* @param tblsp The tablespace
	* @return The block available as a real, not virtual block
	* @exception IOException if IO problem
	*/
	public long getNextFreeBlock(int tblsp) throws IOException {
		if( DEBUG )
			System.out.println("ClusterIOManager.getNextFreeBlock "+tblsp);
		CountDownLatch barrierCount = new CountDownLatch(1);
		IoRequestInterface iori = new GetNextFreeBlockRequest(barrierCount, nextFree[tblsp]);
		ioWorker[tblsp].queueRequest(iori);
		try {
			barrierCount.await();
		} catch (InterruptedException e) {}
		// remove old request
		ioWorker[tblsp].removeRequest((AbstractClusterWork) iori);
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
			System.out.println("ClusterIOManager.getNextFreeBlocks ");
		CountDownLatch barrierCount = new CountDownLatch(DBPhysicalConstants.DTABLESPACES);
		IoRequestInterface[] iori = new IoRequestInterface[DBPhysicalConstants.DTABLESPACES];
		// queue to each tablespace
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			iori[i] = new GetNextFreeBlocksRequest(barrierCount);
			ioWorker[i].queueRequest(iori[i]);
		}
		// Wait for barrier synchronization from UDP master nodes if request demands it
		try {
			barrierCount.await();
		} catch (InterruptedException e) {}
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			nextFree[i] = iori[i].getLongReturn();
			// remove old requests
			ioWorker[i].removeRequest((AbstractClusterWork) iori[i]);
		}
	}
	/**
	 * Send the request to write the given block at the given location, with
	 * the number of bytes used written
	 */
	public void FseekAndWrite(long toffset, Datablock tblk) throws IOException {
		if( DEBUG )
			System.out.println("ClusterIOManager.FseekAndWrite "+toffset);
		int tblsp = GlobalDBIO.getTablespace(toffset);
		long offset = GlobalDBIO.getBlock(toffset);
		CountDownLatch barrierCount = new CountDownLatch(1);
		IoRequestInterface iori = new FSeekAndWriteRequest(barrierCount, offset, tblk);
		ioWorker[tblsp].queueRequest(iori);
		try {
			barrierCount.await();
		} catch (InterruptedException e) {}
		ioWorker[tblsp].removeRequest((AbstractClusterWork) iori);
	}
	/**
	 * Send the request to write the entire contents of the given block at the location specified
	 * Presents a guaranteed write of full block for file extension or other spacing operations
	 */
	public void FseekAndWriteFully(long toffset, Datablock tblk) throws IOException {
		if( DEBUG )
			System.out.println("ClusterIOManager.FseekAndWriteFully "+toffset);
		int tblsp = GlobalDBIO.getTablespace(toffset);
		long offset = GlobalDBIO.getBlock(toffset);
		CountDownLatch barrierCount = new CountDownLatch(1);
		IoRequestInterface iori = new FSeekAndWriteFullyRequest(barrierCount, offset, tblk);
		ioWorker[tblsp].queueRequest(iori);
		try {
			barrierCount.await();
		} catch (InterruptedException e) {}
		ioWorker[tblsp].removeRequest((AbstractClusterWork) iori);
	}
	/**
	 * Queue a request to read int the passed block buffer 
	 * @param toffset The virtual block to read
	 * @param tblk The Datablock buffer to read into
	 * @throws IOException
	 */
	public void FseekAndRead(long toffset, Datablock tblk) throws IOException {
		if( DEBUG )
			System.out.println("ClusterIOManager.FseekAndRead "+toffset);
		int tblsp = GlobalDBIO.getTablespace(toffset);
		long offset = GlobalDBIO.getBlock(toffset);
		CountDownLatch barrierCount = new CountDownLatch(1);
		IoRequestInterface iori = new FSeekAndReadRequest(barrierCount, offset, tblk);
		ioWorker[tblsp].queueRequest(iori);
		try {
			barrierCount.await();
		} catch (InterruptedException e) {}
		// original request should contain object from response from remote worker
		Datablock rblock = (Datablock) iori.getObjectReturn();
		rblock.doClone(tblk);
		// remove old requests
		ioWorker[tblsp].removeRequest((AbstractClusterWork) iori);
	}
	/**
	 * Queue a request to read int the passed block buffer 
	 * @param toffset The virtual block to read
	 * @param tblk The Datablock buffer to read into
	 * @throws IOException
	 */
	public void FseekAndReadFully(long toffset, Datablock tblk) throws IOException {
		if( DEBUG )
			System.out.println("ClusterIOManager.FseekAndReadFully "+toffset);
		int tblsp = GlobalDBIO.getTablespace(toffset);
		long offset = GlobalDBIO.getBlock(toffset);
		CountDownLatch barrierCount = new CountDownLatch(1);
		CompletionLatchInterface iori = new FSeekAndReadFullyRequest(barrierCount, offset, tblk);
		ioWorker[tblsp].queueRequest(iori);
		try {
			barrierCount.await();
		} catch (InterruptedException e) {}
		// original request should contain object from response from remote worker
		Datablock rblock = (Datablock) iori.getObjectReturn();
		rblock.doClone(tblk);
		// remove old requests, this signals we are done
		ioWorker[tblsp].removeRequest((AbstractClusterWork) iori);
	}
	/**
	* Set the initial free blocks after buckets created or bucket initial state
	* Since our directory head gets created in block 0 tablespace 0, the next one is actually the start
	*/
	public void setNextFreeBlocks() {
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++)
			if (i == 0)
				nextFree[i] = ((long) DBPhysicalConstants.DBLOCKSIZ);
			else
				nextFree[i] = 0L;
	}

	/**
	* Get first tablespace
	* @return the position of the first byte of first tablespace
	*/
	public long firstTableSpace() throws IOException {
		return 0L;
	}

	/**
	* Find the smallest tablespace for storage balance, we will always favor creating one
	* over extending an old one
	* @return tablespace
	* @exception IOException if seeking new tablespace or creating fails
	*/
	public int findSmallestTablespace() throws IOException {
		if( DEBUG )
			System.out.println("ClusterIOManager.findSmallestTablespace ");
		synchronized(nextFree) {
			// always make sure we have primary
			long primarySize = Fsize(0);
			int smallestTablespace = 0; // default main
			long smallestSize = primarySize;
			getNextFreeBlocks();
			for (int i = 0; i < nextFree.length; i++) {
				if(nextFree[i] != -1 && nextFree[i] < smallestSize) {
					smallestSize = nextFree[i];
					smallestTablespace = i;
				}
			}
			return smallestTablespace;
		}
	}
	
	public long Fsize(int tblsp) throws IOException {
		if( DEBUG )
			System.out.println("ClusterIOManager.Fsize ");
		CountDownLatch barrierCount = new CountDownLatch(1);
		CompletionLatchInterface iori = new FSizeRequest(barrierCount);
		ioWorker[tblsp].queueRequest(iori);
		try {
			barrierCount.await();
		} catch (InterruptedException e) {}
		long retVal = iori.getLongReturn();
		// remove old requests
		ioWorker[tblsp].removeRequest((AbstractClusterWork) iori);
		return retVal;
	}
	/**
	* If create is true, create only primary tablespace
	* else try to open all existing. For cluster operations we are spinning UDPMaster
	* instances instead of IOWorkers, although UDPMaster subclasses IOWorker to add the cluster transport.
	* Every time we open a new DB, we spin workers for each tablespace which can be partitioned as necessary.
	* @param fname String file name
	* @param create true to create if not existing
	* @exception IOException if problems opening/creating
	* @return true if successful
	* @see IoInterface
	*/
	public boolean Fopen(String fname, int L3cache, boolean create) throws IOException {
		if( DEBUG )
			System.out.println("ClusterIOManager.Fopen with "+fname);
		this.L3cache = L3cache;
		for (int i = 0; i < ioWorker.length; i++) {
			if (ioWorker[i] == null) {
				ioWorker[i] = new DistributedIOWorker(fname, i, ++currentPort, ++currentPort);
				ThreadPoolManager.getInstance().spin(ioWorker[i]);
			}
		}
		// allow the workers to come up
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {}
		return true;
	}
	
	public boolean Fopen(String fname, String remote, int L3cache, boolean create) throws IOException {
		if( DEBUG )
			System.out.println("ClusterIOManager.Fopen with "+fname+" having remote "+remote);
		this.L3cache = L3cache;
		for (int i = 0; i < ioWorker.length; i++) {
			if (ioWorker[i] == null) {
				ioWorker[i] = new DistributedIOWorker(fname, remote, i, ++currentPort, ++currentPort);
				ThreadPoolManager.getInstance().spin(ioWorker[i]);
			}
		}
		// allow the workers to come up
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {}
		return true;
	}
 	public void Fopen() throws IOException {
	}
	
	public void Fclose() throws IOException {
		for (int i = 0; i < ioWorker.length; i++)
			if (ioWorker[i] != null ) {
				if( ioWorker[i].getRequestQueueLength() != 0 )
					System.out.println("WARNING: closing tablespace "+i+" with "+
							ioWorker[i].getRequestQueueLength()+" outstanding requests");
			}
		// just sync in cluster mode
		Fforce();
	}
	
	public void Fforce() throws IOException {
		if( DEBUG ) {
			System.out.println("ClusterIOManager.Fforce ");
		}
		CountDownLatch barrierCount = new CountDownLatch(DBPhysicalConstants.DTABLESPACES);
		IoRequestInterface[] iori = new IoRequestInterface[DBPhysicalConstants.DTABLESPACES];
		// queue to each tablespace
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				iori[i] = new FSyncRequest(barrierCount);
				ioWorker[i].queueRequest(iori[i]);
		}
		try {
				barrierCount.await();
		} catch (InterruptedException e) {}
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				// remove old requests
				ioWorker[i].removeRequest((AbstractClusterWork) iori[i]);
		}
	}
	
	public boolean isNew() {
		try {
			return FisNew(0);
		} catch (IOException e) {
		}
		return false;
	}
	
	private boolean FisNew(int tblsp) throws IOException {
		if( DEBUG )
			System.out.println("ClusterIOManager.FisNew for tablespace "+tblsp);
		CountDownLatch barrierCount = new CountDownLatch(1);
		CompletionLatchInterface iori = new IsNewRequest(barrierCount);
		ioWorker[tblsp].queueRequest(iori);
		try {
			barrierCount.await();
		} catch (InterruptedException e) {}
		boolean retVal = (Boolean) iori.getObjectReturn();
		// remove old requests
		ioWorker[tblsp].removeRequest((AbstractClusterWork) iori);
		return retVal;
	}
	
	public IOWorkerInterface getIOWorker(int tblsp) {
		return ioWorker[tblsp];
	}
	
	public static int getNextUUID() { return ++messageSeq; }

	@Override
	public void forceBufferClear() {
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
	 * Load up a block from the freelist with the assumption that it will be filled in later. Do not 
	 * check for whether it should be logged,etc. As part of the 'acquireblock' process, this takes place. Latch it
	 * as soon as possible though
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
		int tblsp = GlobalDBIO.getTablespace(bn);
		//return blockBuffer[tblsp].findOrAddBlockAccess(bn);
		CountDownLatch cdl = new CountDownLatch(1);
		FindOrAddBlockAccessRequest abanrr = new FindOrAddBlockAccessRequest(blockBuffer[tblsp], cdl, bn);
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
	public BlockAccessIndex getUsedBlock(long loc) {
		int tblsp = GlobalDBIO.getTablespace(loc);
		//return blockBuffer[tblsp].getUsedBlock(loc);
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
	 * Check the free block list for 0 elements. This is a demand response method guaranteed to give us a free block
	 * if 0, begin a search for an element that has 0 accesses
	 * if its in core and not yet in log, write through
	 * @throws IOException
	 */
	@Override
	public void freeupBlock() throws IOException {
		CountDownLatch cdl = new CountDownLatch( DBPhysicalConstants.DTABLESPACES);
		for(int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			//blockBuffer[i].freeupBlock();
			FreeupBlockRequest fbr = new FreeupBlockRequest(blockBuffer[i], cdl);
			blockBuffer[i].queueRequest(fbr);
		}
		try {
			cdl.await(); // barrier synchronization
		} catch (InterruptedException e) {
			return;
		}
	}
	/**
	 * When something comes through the TCPWorker or UDPWorker the ioInterface is set to the TCPWorker
	 * or UDPWorker, which also implement NodeBlockBufferInterface, so we have access to the node block buffer
	 * through the ioInterface if the request traverses those classes. If we are in standalone the MultiThreadedIoManager
	 * uses an alternate request method.
	 * We queue a request to the local block buffer to commit after the preceding requests finish.
	 * We queue a request to the master to forward to the workers to commit their blocks.
	 */
	@Override
	public void commitBufferFlush() throws IOException {
		if( DEBUG ) {
			System.out.println("ClusterIOManager.commitBufferFlush");
		}
		CountDownLatch cdl = new CountDownLatch( DBPhysicalConstants.DTABLESPACES);
		for(int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			//blockBuffer[i].commitBufferFlush();
			CommitRequest cbfr = new CommitRequest(blockBuffer[i], globalIO.getUlog(), commitBarrierSynch, cdl);
			ioWorker[i].queueRequest(cbfr);
		}
		try {
			cdl.await();
		} catch (InterruptedException e) {
			return; // executor shutdown
		}
		if( DEBUG ) {
			System.out.println("ClusterIOManager.commitBufferFlush local buffers synched, messaging remote workers");
		}
		// local buffers are flushed, queue request outbound to flush remote buffers, possibly updated by
		// our commit of local buffers pushing blocks out.
		cdl = new CountDownLatch( DBPhysicalConstants.DTABLESPACES);
		IoRequestInterface[] iori = new IoRequestInterface[DBPhysicalConstants.DTABLESPACES];
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			iori[i] = new RemoteCommitRequest(commitBarrierSynch, cdl);
			ioWorker[i].queueRequest(iori[i]);
		}
		try {
			cdl.await();
		} catch (InterruptedException e) {}
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			// remove old requests
			ioWorker[i].removeRequest((AbstractClusterWork) iori[i]);
		}
		if( DEBUG ) {
			System.out.println("ClusterIOManager.commitBufferFlush exiting.");
		}
	}

	@Override
	public void directBufferWrite() throws IOException {
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
	
	
}
