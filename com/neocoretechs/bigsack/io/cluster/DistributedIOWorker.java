package com.neocoretechs.bigsack.io.cluster;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import com.neocoretechs.bigsack.Props;
import com.neocoretechs.bigsack.io.ThreadPoolManager;
import com.neocoretechs.bigsack.io.request.IoRequestInterface;
import com.neocoretechs.bigsack.io.request.cluster.AbstractClusterWork;
/**
 * This Worker node serves as the queue processing thread for the MasterInterface implementors
 * contained in this class.
 * UDPMaster and TCPMaster are two examples of the implementors of this interface.
 * It takes requests from the queue and calls 'send' to ship them to the MasterInterface.
 * Another difference from the standalone IOWorker is that a 'context' map of monotonic by 
 * worker id's to original request is maintained. These Ids are unique to each master/worker and so
 * can be simple integers monotonically increased for each message.
 * The ids are used mainly to be able to respond to latches waiting on the requests. 
 * In general, this class is a way to map responses to original request as the 'uuid' is passed around the cluster.
 * @author jg
 *
 */
public class DistributedIOWorker implements IOWorkerInterface, Runnable {
	private static final boolean DEBUG = false;
	protected MasterInterface ioUnit;
	private long nextFreeBlock = 0L;
	private BlockingQueue<IoRequestInterface> requestQueue;
	public boolean shouldRun = true;
	protected int tablespace; // 0-7
	protected String DBName;
	protected String remoteDBName = null;
	private ConcurrentHashMap<Integer, IoRequestInterface> requestContext;
	
	public DistributedIOWorker(String dbName, int tablespace, int masterPort, int slavePort) throws IOException {
		this.DBName = dbName;
		this.tablespace = tablespace;
		requestContext = new ConcurrentHashMap<Integer,IoRequestInterface>(1024);
		requestQueue = new ArrayBlockingQueue<IoRequestInterface>(1024, true);
		if (Props.toString("Model").endsWith("UDP")) {
			if( DEBUG )
				System.out.println("Cluster Transport UDP...");
			ioUnit =  new UDPMaster(dbName, tablespace, masterPort, slavePort, requestContext);
		} else {
			if( DEBUG )
				System.out.println("Cluster Transport TCP...");
			ioUnit =  new TCPMaster(dbName, tablespace, masterPort, slavePort, requestContext);
		}
	
		ThreadPoolManager.getInstance().spin((Runnable) ioUnit);
		ioUnit.Fopen(dbName, true);
	}
	/**
	 * Alternate constructor with the option to specify a different remote worker directory
	 * Spin up the masters with the remote dir
	 * @param dbName
	 * @param remoteDBName
	 * @param tablespace
	 * @param masterPort
	 * @param slavePort
	 * @throws IOException
	 */
	public DistributedIOWorker(String dbName, String remoteDBName, int tablespace, int masterPort, int slavePort) throws IOException {
		this.DBName = dbName;
		this.remoteDBName = remoteDBName;
		this.tablespace = tablespace;
		requestContext = new ConcurrentHashMap<Integer,IoRequestInterface>(1024);
		requestQueue = new ArrayBlockingQueue<IoRequestInterface>(1024, true);
		if (Props.toString("Model").endsWith("UDP")) {
			if( DEBUG )
				System.out.println("Cluster Transport UDP...");
			ioUnit =  new UDPMaster(dbName, remoteDBName, tablespace, masterPort, slavePort, requestContext);
		} else {
			if( DEBUG )
				System.out.println("Cluster Transport TCP...");
			ioUnit =  new TCPMaster(dbName, remoteDBName, tablespace, masterPort, slavePort, requestContext);
		}
		ThreadPoolManager.getInstance().spin((Runnable) ioUnit);
		ioUnit.Fopen(dbName, true);	
	}
	/**
	 * Remove request from queue when finished with it
	 */
	public void removeRequest(AbstractClusterWork ior) {
			IoRequestInterface iori = requestContext.get(ior.getUUID());
			requestContext.remove(ior.getUUID(), iori);
			if( DEBUG ) {
			 System.out.println("Request removed: "+iori+" origin:"+ior);
			}
	}
	
	public int getRequestQueueLength() { return requestContext.size(); }
	/**
	 * Queue a request down to the UDPWorker node
	 * We assume node names on remote nodes corresponds to the remoteWorker prefix + tablespace
	 */
	@Override
	public void queueRequest(IoRequestInterface iori) {
		iori.setTablespace(tablespace);
        requestContext.put(((AbstractClusterWork)iori).newUUID(), iori); // equals of AbstractClusterWork compares UUIDs
		if( DEBUG ) {
			System.out.println("Adding request "+iori+" size:"+requestQueue.size());
		}
		try {
			requestQueue.put(iori);
		} catch (InterruptedException e) {
			// shutdown requested while awaiting 'take' to free up queue space
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
				return; // requested shutdown from executor, leave thread
			}
		}

	}
	
}
