package com.neocoretechs.bigsack.io.cluster;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import com.neocoretechs.arieslogger.core.impl.LogToFile;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.request.IoRequestInterface;
import com.neocoretechs.bigsack.io.request.IoResponseInterface;
import com.neocoretechs.bigsack.io.request.cluster.CompletionLatchInterface;
/**
 * This node functions as the master, in effect, a layer between MultiThreadedIOManager in its incarnation
 * as ClusterIOManager and each IOWorker thread located on a remote node.
 * There will be one of these for each tablespace of each database, so 8 per DB each with its own port
 * The naming convention for the remote nodes is the constant 'remoteWorker' with the tablespace number appended.
 * The 'WorkBoot' process on the remote node is responsible for spinning the workers that communicate with the master.
 * A back-channel TCP server to the workboot initiates the process.
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
public class TCPMaster implements Runnable, MasterInterface {
	private static final boolean DEBUG = false;
	public static final boolean TEST = false; // true to run in local cluster test mode
	private int MASTERPORT = 9876;
	private int SLAVEPORT = 9876;
	private int WORKBOOTPORT = 8000;
	private static String remoteWorker = "AMI";
	private InetAddress IPAddress = null;

	private SocketChannel workerSocketChannel = null;
	private SocketAddress workerSocketAddress;
	//private Socket masterSocket;
	private ServerSocketChannel masterSocketChannel;
	private SocketAddress masterSocketAddress;
	ByteBuffer b = ByteBuffer.allocate(LogToFile.DEFAULT_LOG_BUFFER_SIZE);

	private String DBName;
	private int tablespace;
	private boolean shouldRun = true;
	private ConcurrentHashMap<Integer, IoRequestInterface> requestContext;
	
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
	public TCPMaster(String dbName, int tablespace, int masterPort, int slavePort, ConcurrentHashMap<Integer, IoRequestInterface> requestContext)  throws IOException {
		this.DBName = dbName;
		this.tablespace = tablespace;
		this.MASTERPORT = masterPort;
		this.SLAVEPORT = slavePort;
		this.requestContext = requestContext;
		if( TEST ) {
			IPAddress = InetAddress.getLocalHost();
		} else {
			IPAddress = InetAddress.getByName(remoteWorker+String.valueOf(tablespace));
		}
		if( DEBUG ) {
			System.out.println("TCPMaster constructed with "+DBName+" "+tablespace+" master port:"+masterPort+" slave:"+slavePort);
		}
		masterSocketAddress = new InetSocketAddress(MASTERPORT);
		masterSocketChannel = ServerSocketChannel.open();
		masterSocketChannel.bind(masterSocketAddress);
		
	}
	
	public void setMasterPort(int port) {
		MASTERPORT = port;
	}
	public void setSlavePort(int port) {
		SLAVEPORT = port;
	}
	/**
	 * Set the prefix name of the remote worker node that this master communicates with
	 * This name plus the tablespace identifies each individual worker node
	 * In test mode, the local host is used for workers and master
	 * @param rname
	 */
	public synchronized void setRemoteWorkerName(String rname) {
		remoteWorker = rname;
	}
	
	/**
	 * Look for messages coming back from the workers. Extract the UUID of the returned packet
	 * and get the real request from the ConcurrentHashTable buffer
	 */
	@Override
	public void run() {
  	    SocketChannel sock;
		try {
			sock = masterSocketChannel.accept();
		} catch (IOException e1) {
			System.out.println("TCPMaster server socket accept failed with "+e1);
			return;
		}
  	     if( DEBUG ) {
  	    	 System.out.println("TCPMaster got connection "+sock);
  	     }
		while(shouldRun ) {
			try {
			 sock.read(b);
			 IoResponseInterface iori = (IoResponseInterface) GlobalDBIO.deserializeObject(b);
	   	     // get the original request from the stored table
	   	     IoRequestInterface ior = requestContext.get(iori.getUUID());
	   	     if( DEBUG )
	   	    	 System.out.println("FROM Remote, response:"+iori+" master port:"+MASTERPORT+" slave:"+SLAVEPORT);
	   	     //
	   	     if( DEBUG ) {
	   	    	 System.out.println("Extracting latch from original request:"+ior);
	   	    	 if( ior == null ) {
	   	    		 Enumeration<Integer> e = requestContext.keys();
	   	    		 System.out.println("Dump context table "+requestContext.size());
	   	    		 while(e.hasMoreElements())System.out.println(e.nextElement());
	   	    	 }
	   	    	 
	   	     }
	   	     // set the return values in the original request to our values from remote workers
	   	     ((CompletionLatchInterface)ior).setLongReturn(iori.getLongReturn());
	   	     ((CompletionLatchInterface)ior).setObjectReturn(iori.getObjectReturn());
	   	     if( DEBUG ) {
	   	    	 System.out.println("TCPMaster ready to count down latch with "+ior);
	   	     }
	   	     // now add to any latches awaiting
	   	     CountDownLatch cdl = ((CompletionLatchInterface)ior).getCountDownLatch();
	   	     cdl.countDown();
	   	     b.clear();
			} catch (SocketException e) {
					System.out.println("TCPMaster receive socket error "+e+" Address:"+IPAddress+" master port:"+MASTERPORT+" slave:"+SLAVEPORT);
					break;
			} catch (IOException e) {
				// we lost the remote, try to close worker and wait for reconnect
				System.out.println("TCPMaster receive IO error "+e+" Address:"+IPAddress+" master port:"+MASTERPORT+" slave:"+SLAVEPORT);
				if(workerSocketChannel != null ) {
						try {
							workerSocketChannel.close();
						} catch (IOException e1) {}
						workerSocketChannel = null;
				}
				// re-establish master slave connect
				if(masterSocketChannel.isOpen())
					try {
						masterSocketChannel.close();
					} catch (IOException e2) {}
				try {
					masterSocketChannel = ServerSocketChannel.open();
					masterSocketChannel.bind(masterSocketAddress);
				} catch (IOException e3) {
					System.out.println("TCPMaster server socket RETRY channel open failed with "+e3+" THIS NODE IST KAPUT!");
					return;
				}
				// We have done everything we can to close all open channels, now try to re-open them
				// Wait in loop contacting WorkBoot until it somehow re-animates, most likely 
				// through human intervention
				while(true) {
					try {
						Fopen(null, false);
						break;
					} catch (IOException e2) {
						try {
							Thread.sleep(3000);
						} catch (InterruptedException e1) {} // every 3 seconds
					}
				}
				// reached the WorkBoot to restart, set up accept
				try {
						sock = masterSocketChannel.accept();
				} catch (IOException e1) {
						System.out.println("TCPMaster server socket RETRY accept failed with "+e1+" THIS NODE IST KAPUT!");
						return;
				}
			  	if( DEBUG ) {
			  	    	 System.out.println("TCPMaster got RE-connection "+sock);
			  	}
			}
	      }	
	}
	/**
	 * Send request to remote worker
	 * @param iori
	 */
	public synchronized void send(IoRequestInterface iori) {
	    byte[] sendData;
		try {
			if(workerSocketChannel == null ) {
				workerSocketAddress = new InetSocketAddress(IPAddress, SLAVEPORT);
				workerSocketChannel = SocketChannel.open(workerSocketAddress);
			}
			sendData = GlobalDBIO.getObjectAsBytes(iori);
			ByteBuffer srcs = ByteBuffer.wrap(sendData);
			workerSocketChannel.write(srcs);
		} catch (SocketException e) {
				System.out.println("Exception setting up socket to remote worker port "+SLAVEPORT+" "+e);
		} catch (IOException e) {
				System.out.println("Socket send error "+e+" to address "+IPAddress+" on port "+SLAVEPORT);
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
	public synchronized boolean Fopen(String fname, boolean create) throws IOException {
		// send a remote Fopen request to the node
		// this consists of sending the running WorkBoot a message to start the worker for a particular
		// database and tablespace and the node we hand down
		Socket s = new Socket(IPAddress, WORKBOOTPORT);
		OutputStream os = s.getOutputStream();
		WorkBootCommand cpi = new WorkBootCommand();
		cpi.setDatabase(DBName);
		cpi.setTablespace(tablespace);
		cpi.setMasterPort(MASTERPORT);
		cpi.setSlavePort(SLAVEPORT);
		cpi.setTransport("TCP");
		os.write(GlobalDBIO.getObjectAsBytes(cpi));
		os.flush();
		os.close();
		s.close();
		return true;
	}
	
}
