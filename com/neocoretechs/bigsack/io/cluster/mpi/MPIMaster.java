package com.neocoretechs.bigsack.io.cluster.mpi;
import mpi.*;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import com.neocoretechs.arieslogger.core.impl.LogToFile;
import com.neocoretechs.bigsack.io.cluster.MasterInterface;
import com.neocoretechs.bigsack.io.cluster.WorkBootCommand;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.request.IoRequestInterface;
import com.neocoretechs.bigsack.io.request.IoResponseInterface;
import com.neocoretechs.bigsack.io.request.cluster.CompletionLatchInterface;
/**
 * MPI implementation
 * This node functions as the master, in effect, a layer between MultiThreadedIOManager in its incarnation
 * as ClusterIOManager and each IOWorker thread located on a remote node.
 * There will be one of these for each tablespace of each database, so 8 per DB each with its own port
 * The naming convention for the remote nodes is the constant 'remoteWorker' with the tablespace number appended.
 * The 'WorkBoot' process on the remote node is responsible for spinning the workers that communicate with the master.
 * A back-channel MPI server to the workboot initiates the process.
 * To test in local cluster mode set the boolean 'TEST' value true. This replaces the references to remote workers
 * 'AMI' + tablespace Ip address with a localhost IP address. Also TCPworker uses 'AMIMASTER' as its remote master and
 * those references are likewise replaced with localhost. In general the remote directory is 
 * 'Database path + 'tablespace'+ tablespace# + tablename' where tablename is 'DBname+class+'.'+tablespace#'
 * so if your remote db path is /home/relatrix/AMI as passed to workboot then its translation is:
 *  /home/relatrix/tablespace0/AMIcom.yourpack.yourclass.0
 * for the remote node 'AMI0', for others replace all '0' with '1' etc for other tablespaces.
 * So to test cluster locally use 1 workboot and different directories on localhost called tablespace0-7 under the same
 * directory as 'log', the recovery log location. this directory also needs the .properties file
 * On the true cluster a workboot would be running on each node and /home/relatrix/tablespace0,1,2 and properties
 * etc must be present on each node. The master contains the recovery logs and distributes IO requests to each worker node
 * based on tablespace.
 * @author jg
 * Copyright (C) NeoCoreTechs 2014,2015
 */
public class MPIMaster implements Runnable, MasterInterface {
	private static final boolean DEBUG = true;
	public static final boolean TEST = false; // true to run in local cluster test mode
	
	private String masterPort;
	private int WORKBOOTPORT = 8000;
	private static String remoteWorker = "AMI";
	private Intercomm intercomm;

	ByteBuffer bout = MPI.newByteBuffer(LogToFile.DEFAULT_LOG_BUFFER_SIZE);
	ByteBuffer bin = MPI.newByteBuffer(LogToFile.DEFAULT_LOG_BUFFER_SIZE);
	//ByteBuffer b = ByteBuffer.allocate(LogToFile.DEFAULT_LOG_BUFFER_SIZE);

	private String DBName;
	private int tablespace;
	private String remoteDBName = null; // if not null, alternate database name for remote worker nodes with specific directory
	
	private volatile boolean shouldRun = true;
	
	private ConcurrentHashMap<Integer, IoRequestInterface> requestContext;
	private int MAXLEN = 10000;
	private InetAddress IPAddress;
	
	/**
	 * Start a master cluster node. The database, tablespace, and listener port are assigned
	 * by the respective IO manager. The request queue and mapping from request id to original request hashmap
	 * are passed again by the respective IO manager. These masters are one-to-one tablespace and database and worker
	 * on the remote node. The masters all run on the main cluster node.
	 * @param dbName
	 * @param tablespace
	 * @param port
	 * @param requestQueue
	 * @param requestContext
	 * @throws IOException
	 */
	public MPIMaster(String dbName, int tablespace, ConcurrentHashMap<Integer, IoRequestInterface> requestContext)  throws IOException {
		this.DBName = dbName;
		this.tablespace = tablespace;
	
		this.requestContext = requestContext;
		
		try {
			masterPort = Intracomm.openPort();
			if( DEBUG ) {
				System.out.println("MPIMaster constructed with "+DBName+" tablespace:"+tablespace+" MPI master port:"+masterPort);
			}
			// We have to contact the remote TCP backchannel associated with this WORKBOOT to spin remote threads
			// if TEST we are all local, this flag appears in all the network options
			if( TEST ) {
				IPAddress = InetAddress.getLocalHost();
			} else {
				IPAddress = InetAddress.getByName(remoteWorker+String.valueOf(tablespace));
			}
		} catch (MPIException e) {
			throw new IOException(e);
		}
	}

	/**
	 * Specify an alternate remote DB name and directory for the current database.
	 * Primary usage is for nodes with OSs different from the master
	 * @param dbName
	 * @param remoteDBName
	 * @param tablespace
	 * @param masterPort
	 * @param slavePort
	 * @param requestContext
	 * @throws IOException
	 */
	public MPIMaster(String dbName, String remoteDBName, int tablespace, ConcurrentHashMap<Integer, IoRequestInterface> requestContext)  throws IOException {
		this(dbName, tablespace, requestContext);
		this.remoteDBName = remoteDBName;
		if( DEBUG )
			System.out.println("MPIMaster constructed with "+dbName+" using remote DB:"+remoteDBName+" tablespace:"+tablespace);
	}
	

	/**
	 * Set the prefix name of the remote worker node that this master communicates with
	 * This name plus the tablespace identifies each individual worker node
	 * In test mode, the local host is used for workers and master
	 * @param rname
	 */
	public void setRemoteWorkerName(String rname) {
		remoteWorker = rname;
	}
	
	/**
	 * Look for messages coming back from the workers. Extract the UUID of the returned packet
	 * and get the real request from the ConcurrentHashTable buffer
	 */
	@Override
	public void run() {
  	     if( DEBUG ) {
  	    	 System.out.println("MPIMaster connection ");
 			try {
				intercomm = MPI.COMM_SELF.accept(masterPort, 0);
			} catch (MPIException e) {
				System.out.println("MPI Master connection fault "+e+", returning from MPIMaster thread");
				return;
			}
 			if( DEBUG ) {
 				System.out.println("MPIMaster got connection:"+intercomm+" db:"+DBName+" tablespace:"+tablespace+" MPI master port:"+masterPort);
 			}
  	     }
		try {
	
			while(shouldRun ) {
				bin.clear();
				intercomm.recv(bin, bin.capacity(), MPI.BYTE, 0, 99);		
				IoResponseInterface iori = (IoResponseInterface) GlobalDBIO.deserializeObject(bin);                                            
				// get the original request from the stored table
				IoRequestInterface ior = requestContext.get(iori.getUUID());
				if( DEBUG )
					 System.out.println("MPIMaster FROM Remote, response:"+iori);
				 //
				 // If we detect a request that has not correspondence in the table of requests issued
				 // then the request is a duplicate of some sort of corruption has occurred. If in debug, log, dump
				 // table of current requests, and ignore
				 //
				 if( DEBUG ) {
					 System.out.println("MPIMaster Extracting latch from original request:"+ior);
					 if( ior == null ) {
						 Set<Entry<Integer, IoRequestInterface>> e = requestContext.entrySet();
						 System.out.println("MPIMaster ******* INBOUND REQUEST DOES NOT VERIFY *******\r\nDump context table, size:"+requestContext.size());
						 Iterator<Entry<Integer, IoRequestInterface>> ei = e.iterator();
						 while(ei.hasNext()) {
							 Entry<Integer, IoRequestInterface> ein = ei.next();
							 System.out.println("Request #: "+ein.getKey()+" val:"+ein.getValue());
						 }
						 break;
					 }   	 
				 }
				 // set the return values in the original request to our values from remote workers
				 ((CompletionLatchInterface)ior).setLongReturn(iori.getLongReturn());
				 Object o = iori.getObjectReturn();
				 if( o instanceof Exception ) {
					 System.out.println("MPIMaster: ******** REMOTE EXCEPTION ******** "+o);
				 }
				 ((CompletionLatchInterface)ior).setObjectReturn(o);
				 if( DEBUG ) {
					 System.out.println("MPIMaster ready to count down latch with "+ior);
				 }
				 // now add to any latches awaiting
				 CountDownLatch cdl = ((CompletionLatchInterface)ior).getCountDownLatch();
				 cdl.countDown();

			}
		} catch (IOException | MPIException e) {
			// we lost the remote, try to close worker and wait for reconnect
			System.out.println("MPIMaster receive IO error "+e);
			try {
				Intracomm.closePort(masterPort);
				MPI.Finalize();
				MPI.Init(null);
			} catch (MPIException e3) {}
	   }	
	}
	/**
	 * Send request to remote worker
	 * @param iori
	 */
	public void send(IoRequestInterface iori) {
	    byte[] sendData;
	    int rank = -1;
		try {
			sendData = GlobalDBIO.getObjectAsBytes(iori);
			bout = MPI.newByteBuffer(sendData.length);
			bout.put(sendData);
			bout.flip();
			intercomm.send(bout, sendData.length, MPI.BYTE, 1, 99);
				
		} catch (IOException | MPIException e) {
				System.out.println("MPI send error "+e+" rank:"+rank);
		}
	}
	
	/**
	 * Open a socket to the remote worker located at 'remoteWorker' with the tablespace appended
	 * so each node is named [remoteWorker]0 [remoteWorker]1 etc
	 * @param fname
	 * @param create
	 * @return
	 * @throws IOException
	 */
	public boolean Fopen(String fname, boolean create) throws IOException {
		// send a remote Fopen request to the node
		// this consists of sending the running WorkBoot a message to start the worker for a particular
		// database and tablespace and the node we hand down
		Socket s = new Socket(IPAddress, WORKBOOTPORT);
		OutputStream os = s.getOutputStream();
		WorkBootCommand cpi = new WorkBootCommand();
		if( remoteDBName != null )
			cpi.setDatabase(remoteDBName);
		else
			cpi.setDatabase(DBName);
		cpi.setTablespace(tablespace);
		cpi.setTransport("MPI");
		cpi.setMasterPort(masterPort);
		cpi.setSlavePort(masterPort);
		os.write(GlobalDBIO.getObjectAsBytes(cpi));
		os.flush();
		os.close();
		s.close();
		return true;
	}
	
	@Override
	public void setMasterPort(String port) {

		
	}
	@Override
	public void setSlavePort(String port) {
		
	}
	
}
