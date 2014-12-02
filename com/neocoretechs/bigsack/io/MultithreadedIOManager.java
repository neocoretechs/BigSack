package com.neocoretechs.bigsack.io;

import java.io.IOException;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.Props;
import com.neocoretechs.bigsack.io.pooled.BlockDBIOInterface;
import com.neocoretechs.bigsack.io.pooled.Datablock;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.request.FSeekAndReadFullyRequest;
import com.neocoretechs.bigsack.io.request.FSeekAndReadRequest;
import com.neocoretechs.bigsack.io.request.FSeekAndWriteFullyRequest;
import com.neocoretechs.bigsack.io.request.FSeekAndWriteRequest;
import com.neocoretechs.bigsack.io.request.GetNextFreeBlockRequest;
import com.neocoretechs.bigsack.io.request.GetNextFreeBlocksRequest;
import com.neocoretechs.bigsack.io.request.FSyncRequest;
import com.neocoretechs.bigsack.io.request.IoRequestInterface;
/**
 * Handles the aggregation of the IO worker threads of which there is one for each tablespace.
 * Requests are queued to the IO worker assigned to the tablespace desired and can operate in parallel
 * with granularity at the tablespace/randomaccessfile level. This is asynchronous IO on random access files
 * either memory mapped or filesystem.
 * When we need to cast a global operation which requires all tablespaces to coordinate a response we use
 * the CyclicBarrier class to set up the rendezvous with each IOworker and its particular request to the
 * set of all IO workers
 * Copyright (C) NeoCoreTechs 2014
 * @author jg
 *
 */
public final class MultithreadedIOManager {
	private static final boolean DEBUG = true;
	final CyclicBarrier barrierSynch = new CyclicBarrier(DBPhysicalConstants.DTABLESPACES);
	public MultithreadedIOManager() {}
	private IOWorker ioWorker[] = new IOWorker[DBPhysicalConstants.DTABLESPACES];
	private int L3cache = 0;
	private long[] nextFree = new long[DBPhysicalConstants.DTABLESPACES];
	/**
	* Return the first available block that can be acquired for write
	* queue the request to the proper ioworker
	* @param tblsp The tablespace
	* @return The block available as a real, not virtual block
	* @exception IOException if IO problem
	*/
	public long getNextFreeBlock(int tblsp) throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.getNextFreeBlock "+tblsp);
		CountDownLatch barrierCount = new CountDownLatch(1);
		IoRequestInterface iori = new GetNextFreeBlockRequest(barrierCount);
		ioWorker[tblsp].queueRequest(iori);
		try {
			barrierCount.await();
		} catch (InterruptedException e) {}
		return iori.getLongReturn();
	}
	/**
	* Return the reverse scan of the first free block of each tablespace
	* queue the request to the proper ioworker, they wait at barrier synch, then activate countdown latch to signal main
	* @param tblsp The tablespace
	* @return The block available
	* @exception IOException if IO problem
	*/
	public long[] getNextFreeBlocks() throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.getNextFreeBlocks ");
		CountDownLatch barrierCount = new CountDownLatch(DBPhysicalConstants.DTABLESPACES);
		IoRequestInterface[] iori = new IoRequestInterface[DBPhysicalConstants.DTABLESPACES];
		long[] tableBlks = new long[DBPhysicalConstants.DTABLESPACES];
		// queue to each tablespace
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			iori[i] = new GetNextFreeBlocksRequest(barrierSynch, barrierCount);
			ioWorker[i].queueRequest(iori[i]);
		}
		try {
			barrierCount.await();
		} catch (InterruptedException e) {}
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			tableBlks[i] = iori[i].getLongReturn();
		}
		return tableBlks;
	}
	
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
	
	public void FseekAndWriteFully(long toffset, Datablock tblk) throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.FseekAndWriteFully "+toffset);
		int tblsp = GlobalDBIO.getTablespace(toffset);
		long offset = GlobalDBIO.getBlock(toffset);
		CountDownLatch barrierCount = new CountDownLatch(1);
		IoRequestInterface iori = new FSeekAndWriteFullyRequest(barrierCount, offset, tblk);
		ioWorker[tblsp].queueRequest(iori);
	}
	/**
	 * Queue a request to read int the passed block buffer 
	 * @param toffset The virtual block to read
	 * @param tblk The Datablock buffer to read into
	 * @throws IOException
	 */
	public synchronized void FseekAndRead(long toffset, Datablock tblk) throws IOException {
		//if( DEBUG )
		//	System.out.println("MultithreadedIOManager.FseekAndRead "+toffset);
		int tblsp = GlobalDBIO.getTablespace(toffset);
		long offset = GlobalDBIO.getBlock(toffset);
		CountDownLatch barrierCount = new CountDownLatch(1);
		IoRequestInterface iori = new FSeekAndReadRequest(barrierCount, offset, tblk);
		ioWorker[tblsp].queueRequest(iori);
		try {
			barrierCount.await();
		} catch (InterruptedException e) {}
	}
	/**
	 * Queue a request to read int the passed block buffer 
	 * @param toffset The virtual block to read
	 * @param tblk The Datablock buffer to read into
	 * @throws IOException
	 */
	public synchronized void FseekAndReaFully(long toffset, Datablock tblk) throws IOException {
		//if( DEBUG )
		//	System.out.println("MultithreadedIOManager.FseekAndReadFully "+toffset);
		int tblsp = GlobalDBIO.getTablespace(toffset);
		long offset = GlobalDBIO.getBlock(toffset);
		CountDownLatch barrierCount = new CountDownLatch(1);
		IoRequestInterface iori = new FSeekAndReadFullyRequest(barrierCount, offset, tblk);
		ioWorker[tblsp].queueRequest(iori);
		try {
			barrierCount.await();
		} catch (InterruptedException e) {}
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
			System.out.println("MultithreadedIOManager.findSmallestTablespace ");
		// always make sure we have primary
		long primarySize = ioWorker[0].Fsize();
		int smallestTablespace = 0; // default main
		long smallestSize = primarySize;
		nextFree = getNextFreeBlocks();
		for (int i = 0; i < nextFree.length; i++) {
			if (nextFree[i] < smallestSize) {
				smallestSize = nextFree[i];
				smallestTablespace = i;
			}
		}
		return smallestTablespace;
	}
	

	/**
	* If create is true, create only primary tablespace
	* else try to open all existing
	* @param fname String file name
	* @param create true to create if not existing
	* @exception IOException if problems opening/creating
	* @return true if successful
	* @see IoInterface
	*/
	public boolean Fopen(String fname, int L3cache, boolean create) throws IOException {
		this.L3cache = L3cache;
		for (int i = 0; i < ioWorker.length; i++) {
			if (ioWorker[i] == null)
						ioWorker[i] = new IOWorker(fname, i, L3cache);
			ThreadPoolManager.getInstance().spin(ioWorker[i]);
		}
		return true;
	}
	
	
 	public void Fopen() throws IOException {
		for (int i = 0; i < ioWorker.length; i++)
			if (ioWorker[i] != null && !ioWorker[i].isopen())
				ioWorker[i].Fopen();
	}
	
	public void Fclose() throws IOException {
		for (int i = 0; i < ioWorker.length; i++)
			if (ioWorker[i] != null && ioWorker[i].isopen()) {
				if( ioWorker[i].getRequestQueueLength() == 0 )
					ioWorker[i].Fclose();
				else
					throw new IOException("Attempt to close tablespace with outstanding requests");
			}
	}
	
	public void Fforce() throws IOException {
		if( DEBUG )
			System.out.println("MultithreadedIOManager.Fforce ");
			CountDownLatch barrierCount = new CountDownLatch(DBPhysicalConstants.DTABLESPACES);
			IoRequestInterface[] iori = new IoRequestInterface[DBPhysicalConstants.DTABLESPACES];
			// queue to each tablespace
			for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				iori[i] = new FSyncRequest(barrierSynch, barrierCount);
				ioWorker[i].queueRequest(iori[i]);
			}
			try {
				barrierCount.await();
			} catch (InterruptedException e) {}
	}
	
	public boolean isNew() {
		return ioWorker[0].isnew();
	}
	

}
