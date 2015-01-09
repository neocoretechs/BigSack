package com.neocoretechs.bigsack.io.request.cluster;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.CountDownLatch;

import com.neocoretechs.bigsack.io.IoInterface;

/**
 * This is an intent parallel computation component of a tablespace wide request.
 * We are using a CountDownLatch to set up with the number of tablepsaces and after each thread
 * does an fsync it will await the barrier. 
 * Once UDPMaster receives a return a countdown latch is decreased which activates the cluster
 * IO manager countdown latch waiter when count reaches 0, thereby releasing the thread to proceed.
 * Copyright (C) NeoCoreTechs 2014
 * @author jg
 *
 */
public final class FSyncRequest extends AbstractClusterWork implements CompletionLatchInterface, Serializable {
	private static final long serialVersionUID = 3100814623119741116L;
	private transient IoInterface ioUnit;
	private int tablespace;
	private transient CountDownLatch barrierCount;
	public FSyncRequest() {}
	public FSyncRequest(CountDownLatch barrierCount) {
		this.barrierCount = barrierCount;
	}
	@Override
	public synchronized void process() throws IOException {
		Fsync();
		barrierCount.countDown();
	}
	private void Fsync() throws IOException {
	if (ioUnit != null && ioUnit.isopen())
		ioUnit.Fforce();
	}
	@Override
	public synchronized long getLongReturn() {
		return -1L;
	}

	@Override
	public synchronized Object getObjectReturn() {
		return new Long(-1L);
	}
	/**
	 * This method is called by queueRequest to set the proper tablespace from IOManager 
	 * It is the default way to set the active IO unit
	 */
	@Override
	public synchronized void setIoInterface(IoInterface ioi) {
		this.ioUnit = ioi;	
	}
	
	@Override
	public synchronized void setTablespace(int tablespace) {
		this.tablespace = tablespace;
	}
	public synchronized String toString() {
		return getUUID()+",tablespace:"+tablespace+":FSyncRequest";
	}
	/**
	 * The latch will be extracted by the UDPMaster and when a response comes back it will be tripped
	 */
	@Override
	public CountDownLatch getCountDownLatch() {
		return barrierCount;
	}

	@Override
	public void setCountDownLatch(CountDownLatch cdl) {
		barrierCount = cdl;
	}
	@Override
	public void setLongReturn(long val) {	
	}

	@Override
	public void setObjectReturn(Object o) {	
	}

}
