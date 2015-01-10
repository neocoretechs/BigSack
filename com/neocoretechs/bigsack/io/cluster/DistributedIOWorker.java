package com.neocoretechs.bigsack.io.cluster;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import com.neocoretechs.bigsack.io.ThreadPoolManager;
import com.neocoretechs.bigsack.io.request.IoRequestInterface;
import com.neocoretechs.bigsack.io.request.cluster.AbstractClusterWork;
/**
 * This Worker node serves as the queue processing thread for the UDPMaster
 * instances. It takes requests from the queue and calls 'send' to ship them to the UDPMaster.
 * Another difference is that a 'context' map of monotonic by worker id's to original request is maintained
 * to be able to respond to latches waiting on the requests. In general, a wy to map responses to original request
 * as the 'uuid' is passed around the cluster.
 * @author jg
 *
 */
public class DistributedIOWorker implements IOWorkerInterface, Runnable {
	private static final boolean DEBUG = true;
	protected UDPMaster ioUnit;
	private long nextFreeBlock = 0L;
	private BlockingQueue<IoRequestInterface> requestQueue;
	public boolean shouldRun = true;
	protected int tablespace; // 0-7
	protected String DBName;
	private ConcurrentHashMap<Integer, IoRequestInterface> requestContext;
	public DistributedIOWorker(String dbName, int tablespace, int masterPort, int slavePort) throws IOException {
		this.DBName = dbName;
		this.tablespace = tablespace;
		requestContext = new ConcurrentHashMap<Integer,IoRequestInterface>(1024);
		requestQueue = new ArrayBlockingQueue<IoRequestInterface>(1024);
		ioUnit =  new UDPMaster(dbName, tablespace, masterPort, slavePort, requestContext);
		ThreadPoolManager.getInstance().spin(ioUnit);
		//ioUnit.Fopen(dbName, true);
		ioUnit.Fopen(dbName, false);
		
	}
	/**
	 * Remove request from queue when finished with it
	 */
	public synchronized void removeRequest(AbstractClusterWork ior) {
		 IoRequestInterface iori = requestContext.get(ior.getUUID());
		 requestContext.remove(ior.getUUID(), iori);
		 if( DEBUG ) {
			 System.out.println("Request removed: "+iori+" origin:"+ior);
		 }
	}
	
	public synchronized int getRequestQueueLength() { return requestContext.size(); }
	/**
	 * Queue a request down to the UDPWorker node
	 * We assume node names on remote nodes corresponds to the remoteWorker prefix + tablespace
	 */
	@Override
	public synchronized void queueRequest(IoRequestInterface iori) {
		iori.setTablespace(tablespace);
        requestContext.put(((AbstractClusterWork)iori).newUUID(), iori); // equals of AbstractClusterWork compares UUIDs
		if( DEBUG ) {
			System.out.println("Adding request "+iori+" size:"+requestQueue.size());
		}
		requestQueue.add(iori);
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
