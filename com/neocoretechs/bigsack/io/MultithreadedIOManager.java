package com.neocoretechs.bigsack.io;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.cluster.IOWorkerInterface;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.Datablock;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.pooled.MappedBlockBuffer;
import com.neocoretechs.bigsack.io.request.CommitRequest;
import com.neocoretechs.bigsack.io.request.FSeekAndReadFullyRequest;
import com.neocoretechs.bigsack.io.request.FSeekAndReadRequest;
import com.neocoretechs.bigsack.io.request.FSeekAndWriteFullyRequest;
import com.neocoretechs.bigsack.io.request.FSeekAndWriteRequest;
import com.neocoretechs.bigsack.io.request.GetNextFreeBlockRequest;
import com.neocoretechs.bigsack.io.request.GetNextFreeBlocksRequest;
import com.neocoretechs.bigsack.io.request.FSyncRequest;
import com.neocoretechs.bigsack.io.request.IoRequestInterface;
import com.neocoretechs.bigsack.io.request.iomanager.AddBlockAccessNoReadRequest;
import com.neocoretechs.bigsack.io.request.iomanager.DirectBufferWriteRequest;
import com.neocoretechs.bigsack.io.request.iomanager.FindOrAddBlockAccessRequest;
import com.neocoretechs.bigsack.io.request.iomanager.ForceBufferClearRequest;
import com.neocoretechs.bigsack.io.request.iomanager.FreeupBlockRequest;
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
	private GlobalDBIO globalIO;
	// barrier synch for specific functions, cyclic (reusable)
	final CyclicBarrier forceBarrierSynch = new CyclicBarrier(DBPhysicalConstants.DTABLESPACES);
	final CyclicBarrier nextBlocksBarrierSynch = new CyclicBarrier(DBPhysicalConstants.DTABLESPACES);
	final CyclicBarrier commitBarrierSynch = new CyclicBarrier(DBPhysicalConstants.DTABLESPACES);
	final CyclicBarrier directWriteBarrierSynch = new CyclicBarrier(DBPhysicalConstants.DTABLESPACES);
	protected IOWorkerInterface ioWorker[];
	protected int L3cache = 0;
	protected long[] nextFree = new long[DBPhysicalConstants.DTABLESPACES];
	private MappedBlockBuffer[] blockBuffer; // block number to Datablock
	
	public MultithreadedIOManager(GlobalDBIO globalIO) {
		this.globalIO = globalIO;
		ioWorker = new IOWorker[DBPhysicalConstants.DTABLESPACES];
		blockBuffer = new MappedBlockBuffer[DBPhysicalConstants.DTABLESPACES];
		// create master buffers for each tablespace
		ThreadPoolManager.init(new String[]{"BLOCKPOOL"});
		for(int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			blockBuffer[i] = new MappedBlockBuffer(globalIO, i);
			ThreadPoolManager.getInstance().spin(blockBuffer[i], "BLOCKPOOL");
		}
	}
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#getNextFreeBlock(int)
	 */
	@Override
	public long getNextFreeBlock(int tblsp) throws IOException {
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
	public void FseekAndWrite(long toffset, Datablock tblk) throws IOException {
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
	public void FseekAndWriteFully(long toffset, Datablock tblk) throws IOException {
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
			System.out.println("MultithreadedIOManager.FseekAndRead "+toffset);
		int tblsp = GlobalDBIO.getTablespace(toffset);
		long offset = GlobalDBIO.getBlock(toffset);
		CountDownLatch barrierCount = new CountDownLatch(1);
		IoRequestInterface iori = new FSeekAndReadRequest(barrierCount, offset, tblk);
		ioWorker[tblsp].queueRequest(iori);
		try {
			barrierCount.await();
		} catch (InterruptedException e) {}
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
	public void setNextFreeBlocks() {
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

	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#findSmallestTablespace()
	 */
	@Override
	public int findSmallestTablespace() throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.findSmallestTablespace ");
		// always make sure we have primary
		long primarySize = ((IoInterface)ioWorker[0]).Fsize();
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
	
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#Fopen(java.lang.String, int, boolean)
	 */
	@Override
	public boolean Fopen(String fname, int L3cache, boolean create) throws IOException {
		this.L3cache = L3cache;
		for (int i = 0; i < ioWorker.length; i++) {
			if (ioWorker[i] == null) {
				ioWorker[i] = new IOWorker(translateDb(fname,i), i, L3cache);
			}
			ThreadPoolManager.getInstance().spin((Runnable)ioWorker[i]);
		}
		return true;
	}
	
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#Fopen(java.lang.String, int, boolean)
	 */
	@Override
	public boolean Fopen(String fname, String remote, int L3cache, boolean create) throws IOException {
		this.L3cache = L3cache;
		for (int i = 0; i < ioWorker.length; i++) {
			if (ioWorker[i] == null) {
					if( remote == null )
						ioWorker[i] = new IOWorker(translateDb(fname,i), i, L3cache);
					else
						ioWorker[i] = new IOWorker(translateDb(fname,i), translateDb(remote,i), i, L3cache);
			}
			ThreadPoolManager.getInstance().spin((Runnable)ioWorker[i]);
		}
		return true;
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
		for (int i = 0; i < ioWorker.length; i++)
			if (ioWorker[i] != null && !((IoInterface)ioWorker[i]).isopen())
				((IoInterface)ioWorker[i]).Fopen();
	}
	
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.IoManagerInterface#Fclose()
	 */
	@Override
	public void Fclose() throws IOException {
		for (int i = 0; i < ioWorker.length; i++)
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
	public void Fforce() throws IOException {
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
		return 	((IoInterface)ioWorker[0]).isnew();
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
			CountDownLatch cdl = new CountDownLatch(DBPhysicalConstants.DTABLESPACES);
			synchronized(blockBuffer) {
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
		//int tblsp = GlobalDBIO.getTablespace(bn);
		//return blockBuffer[tblsp].findOrAddBlockAccess(bn);
		int tblsp = GlobalDBIO.getTablespace(bn);
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
	 * A little different variation of parallel where we dont barrier synch in the request because
	 * we can return staggered results
	 */
	@Override
	public void freeupBlock() throws IOException {
			//for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			//	blockBuffer[i].freeupBlock();
			//}
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
	 * Commit the outstanding blocks, wait until the IO requests have finished first
	 */
	@Override
	public void commitBufferFlush() throws IOException {
		CountDownLatch barrierCount = new CountDownLatch(DBPhysicalConstants.DTABLESPACES);
		IoRequestInterface[] iori = new IoRequestInterface[DBPhysicalConstants.DTABLESPACES];
		// queue to each tablespace
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			iori[i] = new CommitRequest(blockBuffer[i], commitBarrierSynch, barrierCount);
			ioWorker[i].queueRequest(iori[i]);
		}
		try {
			barrierCount.await();
		} catch (InterruptedException e) {}
	}
	
	@Override
	public void directBufferWrite() throws IOException {
		//for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
		//	blockBuffer[i].directBufferWrite();
		//}
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
