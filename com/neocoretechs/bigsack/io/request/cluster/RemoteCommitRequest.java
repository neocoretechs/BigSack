package com.neocoretechs.bigsack.io.request.cluster;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.cluster.NodeBlockBuffer;
import com.neocoretechs.bigsack.io.cluster.NodeBlockBufferInterface;

/**
 * Request is passed to workers and commits blocks on nodes. Really just telling remote buffers to flush.
 * the Iointerface is implemented by cluster workers which further extend interfaces
 * to include the NodeBlockBufferInterface (and distributeWorkerResponseInterface) 
 * which allows access to the NodeBlockBuffer.
 * The WorkerRequestProcessors are responsible for setting the fields for the ioUnit and countdownlatch.
 * Essentially, the transient fields are filled in by methods invoked by the processor before 'process' is called.
 * Copyright (C) NeoCoreTechs 2014
 * @author jg
 *
 */
public final class RemoteCommitRequest extends AbstractClusterWork implements CompletionLatchInterface, Serializable  {
	private static final boolean DEBUG = true;
	private static final long serialVersionUID = 1L;
	private int tablespace;
	private transient CountDownLatch barrierCount;
	private transient IoInterface ioUnit;
	private transient NodeBlockBuffer blockBuffer;
	/**
	* Send out a remote commit to flush buffer pools at remote nodes 
	*/
	public RemoteCommitRequest() {}
	
	public RemoteCommitRequest(CountDownLatch cdl) {
		this.barrierCount = cdl;
	}

	@Override
	public void process() throws IOException {
		if( DEBUG )
			System.out.println(this);
		blockBuffer.force();
		barrierCount.countDown();
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
	 * It is the default way to set the active IO unit. A number of these methods are called by WorkerRequestProcessor
	 */
	@Override
	public void setIoInterface(IoInterface ioi) {
		this.ioUnit = ioi;	
		blockBuffer = ((NodeBlockBufferInterface)ioUnit).getBlockBuffer();
	}
	
	@Override
	public void setTablespace(int tablespace) {
		this.tablespace = tablespace;
	}
	public String toString() {
		return "Remote commit Request for tablespace "+tablespace+" io:"+this.ioUnit+" buffer pool:"+blockBuffer+" latch:"+barrierCount;
	}
	@Override
	public CountDownLatch getCountDownLatch() {
		return barrierCount;
	}
	@Override
	public void setCountDownLatch(CountDownLatch cdl) {
		this.barrierCount = cdl;
	}
	@Override
	public void setLongReturn(long val) {
	}
	@Override
	public void setObjectReturn(Object o) {
	}
	@Override
	public CyclicBarrier getCyclicBarrier() {
		return null;
	}
	@Override
	public void setCyclicBarrier(CyclicBarrier cb) {	
	}

}
