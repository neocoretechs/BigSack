package com.neocoretechs.bigsack.io.request;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.pooled.Datablock;
/**
 * This is an intent parallel computation component of a tablespace wide request.
 * We are using a CyclicBarrier set up with the number of tablespaces and after each thread
 * does an fsync it will await the barrier synch. 
 * Once released from barrier synch a countdown latch is decreased which activates the multi
 * threading IO manager countdown latch waiter when count reaches 0, thereby releasing the thread
 * to proceed.
 * Copyright (C) NeoCoreTechs 2014
 * @author jg
 *
 */
public final class FSyncRequest implements IoRequestInterface {
	private IoInterface ioUnit;
	private CyclicBarrier barrierSynch;
	private int tablespace;
	private CountDownLatch barrierCount;
	public FSyncRequest(CyclicBarrier barrierSynch, CountDownLatch barrierCount) {
		this.barrierSynch = barrierSynch;
		this.barrierCount = barrierCount;
	}
	@Override
	public void process() throws IOException {
		Fsync();
		barrierCount.countDown();
	}
	private void Fsync() throws IOException {
		synchronized(ioUnit) {
			if (ioUnit != null && ioUnit.isopen())
				ioUnit.Fforce();
			else
				throw new IOException("IO thread offline or not open");
		}
		// wait at the barrier until all other tablespaces arrive at their result
		try {
			barrierSynch.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			throw new IOException(e);
		}
	}
	@Override
	public long getLongReturn() {
		return -1L;
	}

	@Override
	public Object getObjectReturn() {
		return new Long(-1L);
	}
	/**
	 * This method is called by queueRequest to set the proper tablespace from IOManager 
	 * It is the default way to set the active IO unit
	 */
	@Override
	public void setIoInterface(IoInterface ioi) {
		this.ioUnit = ioi;	
	}
	
	@Override
	public void setTablespace(int tablespace) {
		this.tablespace = tablespace;
	}
	public String toString() {
		return "FSyncRequest for tablespace "+tablespace;
	}

}
