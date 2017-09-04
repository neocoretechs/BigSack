package com.neocoretechs.bigsack.io.cluster;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.MultithreadedIOManager;
import com.neocoretechs.bigsack.io.ThreadPoolManager;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.Datablock;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.pooled.ObjectDBIO;
import com.neocoretechs.bigsack.io.request.cluster.AbstractClusterWork;
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
import com.neocoretechs.bigsack.io.request.iomanager.ForceBufferClearRequest;
import com.neocoretechs.bigsack.io.request.IoRequestInterface;

/**
 * Handles the aggregation of the IO worker threads of which there is one for each tablespace.
 * In this incarnation the IOWorkers are subclassed to UDPMaster or some other IO subsystem like MPI,
 * which handles traffic down to specific networkers, such as
 * UDPWorkers, through the command to start, via the WorkBoot node.
 * When we need to cast a global operation which requires all tablespaces to coordinate a response we use
 * the CyclicBarrier class to set up the rendezvous with each IOworker and its particular request to the
 * set of all IO workers
 * Copyright (C) NeoCoreTechs 2014
 * @author jg
 *
 */
public final class ClusterIOManager extends MultithreadedIOManager {
	private static final boolean DEBUG = false;
	protected int L3cache = 0;
	private static int currentPort = 10000; // starting UDP port, increments as assigned
	private static int messageSeq = 0; // monotonically increasing request id

	/**
	 * Instantiate our master node array per database that communicate with our worker nodes
	 * @throws IOException 
	 */
	public ClusterIOManager(ObjectDBIO globalIO) throws IOException {
		super(globalIO);
	}
	
	protected void assignIoWorker() {
		ioWorker = new DistributedIOWorker[DBPhysicalConstants.DTABLESPACES];
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
		IoRequestInterface iori = new GetNextFreeBlockRequest(barrierCount, bufferPool.getFreeBlockAllocator().getNextFree(tblsp));
		ioWorker[tblsp].queueRequest(iori);
		try {
			barrierCount.await();
		} catch (InterruptedException e) {}
		// remove old request
		((DistributedIOWorker)ioWorker[tblsp]).removeRequest((AbstractClusterWork) iori);
		bufferPool.getFreeBlockAllocator().setNextFree(tblsp, iori.getLongReturn());
		return bufferPool.getFreeBlockAllocator().getNextFree(tblsp);
		
	}
	/**
	* Return the reverse scan of the first free block of each tablespace
	* queue the request to the proper ioworker, they wait at barrier synch, 
	* then activate countdown latch to signal main. Result in placed in class level nextFree
	* @exception IOException if IO problem
	*/
	public void getNextFreeBlocks() throws IOException {
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
	
		long[] freeArray = new long[DBPhysicalConstants.DTABLESPACES];
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				freeArray[i] = iori[i].getLongReturn();
				// remove old requests
				((DistributedIOWorker)ioWorker[i]).removeRequest((AbstractClusterWork) iori[i]);
		}
		bufferPool.getFreeBlockAllocator().setNextFree(freeArray);
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
		((DistributedIOWorker)ioWorker[tblsp]).removeRequest((AbstractClusterWork) iori);
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
		((DistributedIOWorker)ioWorker[tblsp]).removeRequest((AbstractClusterWork) iori);
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
		((DistributedIOWorker)ioWorker[tblsp]).removeRequest((AbstractClusterWork) iori);
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
		((DistributedIOWorker)ioWorker[tblsp]).removeRequest((AbstractClusterWork) iori);
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
		((DistributedIOWorker)ioWorker[tblsp]).removeRequest((AbstractClusterWork) iori);
		return retVal;
	}
	/**
	 * Invoke each tablespace open request by creating buffers and spinning workers.
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#Fopen(java.lang.String, int, boolean)
	 */
	@Override
	public synchronized boolean Fopen(String fname, int L3cache, boolean create) throws IOException {
		this.L3cache = L3cache;
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			if( globalIO.getWorkerNodes() != null )
				ioWorker[i] = new DistributedIOWorker(fname, i, ++currentPort, ++currentPort, 
						globalIO.getWorkerNodes()[i][0], Integer.valueOf(globalIO.getWorkerNodes()[i][1]) );
			else
				ioWorker[i] = new DistributedIOWorker(fname, i, ++currentPort, ++currentPort, null, 0);
			bufferPool.createPool(globalIO, this, i);
			ThreadPoolManager.getInstance().spin((Runnable)ioWorker[i],"IOWORKER");
			ThreadPoolManager.getInstance().spin(getBlockBuffer(i), "BLOCKPOOL");
			// allow the workers to come up
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {}
			// attempt recovery if needed
			bufferPool.recover(i);
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
		String bootNode;
		int bootPort;
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			if( globalIO.getWorkerNodes() != null ) {
				bootNode = globalIO.getWorkerNodes()[i][0]; 
				bootPort = Integer.valueOf(globalIO.getWorkerNodes()[i][1]);
			} else {
				bootNode = null;
				bootPort = 0;
			}
			if( remote == null )
					ioWorker[i] = new DistributedIOWorker(fname, i, ++currentPort, ++currentPort, bootNode, bootPort);
			else
					ioWorker[i] = new DistributedIOWorker(fname, remote, i, ++currentPort, ++currentPort, bootNode, bootPort);
			bufferPool.createPool(globalIO, this, i);
			ThreadPoolManager.getInstance().spin((Runnable)ioWorker[i], "IOWORKER");
			ThreadPoolManager.getInstance().spin(bufferPool.getBlockBuffer(i), "BLOCKPOOL");
			// allow the workers to come up
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {}
			// attempt recovery if needed
			bufferPool.recover(i);
		}
		// fill in the next free block indicators and set the smallest tablespace
		findSmallestTablespace();
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
			((DistributedIOWorker)ioWorker[i]).removeRequest((AbstractClusterWork) iori[i]);
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
		((DistributedIOWorker)ioWorker[tblsp]).removeRequest((AbstractClusterWork) iori);
		return retVal;
	}
	
	public static int getNextUUID() { return ++messageSeq; }

	@Override
	public void forceBufferClear() {
		CountDownLatch cdl = new CountDownLatch(DBPhysicalConstants.DTABLESPACES);
		for(int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				ForceBufferClearRequest fbcr = new ForceBufferClearRequest(bufferPool.getBlockBuffer(i), cdl, forceBarrierSynch);
				bufferPool.getBlockBuffer(i).queueRequest(fbcr);
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
	 * as soon as possible though.
	 */
	@Override
	public BlockAccessIndex addBlockAccessNoRead(Long Lbn) throws IOException {
		int tblsp = GlobalDBIO.getTablespace(Lbn);
		//return blockBuffer[tblsp].addBlockAccessNoRead(Lbn);
		CountDownLatch cdl = new CountDownLatch(1);
		AddBlockAccessNoReadRequest abanrr = new AddBlockAccessNoReadRequest(bufferPool.getBlockBuffer(tblsp), cdl, Lbn);
		bufferPool.getBlockBuffer(tblsp).queueRequest(abanrr);
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
		return bufferPool.findOrAddBlockAccess(bn);
	}

	@Override
	public BlockAccessIndex getUsedBlock(long loc) {
		if( DEBUG )
			System.out.println("ClusterIOManager.getUsedBlock "+GlobalDBIO.valueOf(loc));
		//int tblsp = GlobalDBIO.getTablespace(loc);
		//return blockBuffer[tblsp].getUsedBlock(loc);
		return bufferPool.getUsedBlock(loc);
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
		
		bufferPool.commmitBufferFlush(ioWorker);

		if( DEBUG ) {
			System.out.println("ClusterIOManager.commitBufferFlush local buffers synched, messaging remote workers");
		}
		// local buffers are flushed, queue request outbound to flush remote buffers, possibly updated by
		// our commit of local buffers pushing blocks out.
		CountDownLatch cdl = new CountDownLatch( DBPhysicalConstants.DTABLESPACES);
		IoRequestInterface[] iori = new IoRequestInterface[DBPhysicalConstants.DTABLESPACES];
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			iori[i] = new RemoteCommitRequest(cdl);
			ioWorker[i].queueRequest(iori[i]);
		}
		try {
			cdl.await();
		} catch (InterruptedException e) {}
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			// remove old requests
			((DistributedIOWorker)ioWorker[i]).removeRequest((AbstractClusterWork) iori[i]);
		}
		if( DEBUG ) {
			System.out.println("ClusterIOManager.commitBufferFlush exiting.");
		}
	}

	@Override
	public void directBufferWrite() throws IOException {
		if( DEBUG )
			System.out.println("ClusterIOManager.directBufferWrite invoked.");
		bufferPool.directBufferWrite();
	}
	
	@Override
	public void writeDirect(int tblsp, long blkn, Datablock blkV2) throws IOException {
		synchronized(ioWorker[tblsp]) {
			CountDownLatch barrierCount = new CountDownLatch(1);
			IoRequestInterface iori = new FSeekAndWriteRequest(barrierCount, blkn, blkV2);
			ioWorker[tblsp].queueRequest(iori);
			try {
				barrierCount.await();
			} catch (InterruptedException e) {}
			((DistributedIOWorker)ioWorker[tblsp]).removeRequest((AbstractClusterWork) iori);
		}
	}
	
	@Override
	public void readDirect(int tblsp, long blkn, Datablock blkV2) throws IOException {
		synchronized(ioWorker[tblsp]) {
			CountDownLatch barrierCount = new CountDownLatch(1);
			IoRequestInterface iori = new FSeekAndReadRequest(barrierCount, blkn, blkV2);
			ioWorker[tblsp].queueRequest(iori);
			try {
				barrierCount.await();
			} catch (InterruptedException e) {}
			// original request should contain object from response from remote worker
			Datablock rblock = (Datablock) iori.getObjectReturn();
			rblock.doClone(blkV2);
			// remove old requests
			((DistributedIOWorker)ioWorker[tblsp]).removeRequest((AbstractClusterWork) iori);
		}
	}
	
}
