package com.neocoretechs.bigsack.io.cluster;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import com.neocoretechs.bigsack.io.ThreadPoolManager;
import com.neocoretechs.bigsack.io.pooled.Datablock;
import com.neocoretechs.bigsack.io.request.IoRequestInterface;
import com.neocoretechs.bigsack.io.request.cluster.AbstractClusterWork;

public class DistributedIOWorker implements IOWorkerInterface, Runnable {
	private static final boolean DEBUG = false;
	protected UDPMaster ioUnit;
	private long nextFreeBlock = 0L;
	private BlockingQueue<IoRequestInterface> requestQueue;
	public boolean shouldRun = true;
	protected int tablespace; // 0-7
	protected String DBName;
	private ConcurrentHashMap<Integer, IoRequestInterface> requestContext;
	public DistributedIOWorker(String dbName, int tablespace, int port) throws IOException {
		requestContext = new ConcurrentHashMap<Integer,IoRequestInterface>(1024);
		requestQueue = new ArrayBlockingQueue<IoRequestInterface>(1024);
		ioUnit =  new UDPMaster(dbName, tablespace, port, requestQueue, requestContext);
		ThreadPoolManager.getInstance().spin(ioUnit);
	}
	/**
	 * Remove request from queue when finished with it
	 */
	public synchronized void removeRequest(AbstractClusterWork ior) {
		 IoRequestInterface iori = requestContext.get(ior.getUUID());
		 requestContext.remove(ior.getUUID(), iori);
	}
	
	public synchronized int getRequestQueueLength() { return requestContext.size(); }
	/**
	 * Queue a request down to the UDPWorker node
	 * We assume node names on remote nodes corresponds to the remoteWorker prefix + tablespace
	 */
	@Override
	public synchronized void queueRequest(IoRequestInterface iori) {
		iori.setTablespace(tablespace);
		if( DEBUG ) {
			System.out.println("Adding request "+iori+" size:"+requestQueue.size());
		}
		requestQueue.add(iori);
        if( !((AbstractClusterWork)iori).isResponse() ) {
        	requestContext.put(((AbstractClusterWork)iori).newUUID(), iori); // equals of AbstractClusterWork compares UUIDs
        }
    }
	public long getNextFreeBlock() {
		return nextFreeBlock;
	}
	public void setNextFreeBlock(long nextFreeBlock) {
		this.nextFreeBlock = nextFreeBlock;
	}
	@Override
	public void run() {
		while(shouldRun) {
			try {
				IoRequestInterface iori = requestQueue.take();
				ioUnit.send(iori);
			} catch (InterruptedException e) {
				System.out.println("Request queue interrupt ");
				e.printStackTrace();
			}
		}

	}

	
}
