package com.neocoretechs.bigsack.io.cluster;

import java.io.IOException;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.request.IoRequestInterface;
import com.neocoretechs.bigsack.io.request.IoResponseInterface;
import com.neocoretechs.bigsack.io.request.cluster.CompletionLatchInterface;
/**
 * This node functions as the master, in effect, a layer between MultiThreadedIOManager in its incarnation
 * as ClusterIOManager and each IOWorker thread located on a remote node.
 * There will be one of these for each tablespace of each database, so 8 per DB each with its own port
 * The naming convention for the remote nodes is the constant 'remoteWorker' with the tablespace number appended
 * @author jg
 *
 */
public class UDPMaster implements Runnable, MasterInterface {
	private static final boolean DEBUG = true;
	public static final boolean TEST = true;
	private int MASTERPORT = 9876;
	private int SLAVEPORT = 9876;
	private int WORKBOOTPORT = 8000;
	private static String remoteWorker = "AMI";
	private InetAddress IPAddress = null;
	private DatagramSocket clientSocket;

	private String DBName;
	private int tablespace;
	private boolean shouldRun = true;
	private ConcurrentHashMap<Integer, IoRequestInterface> requestContext;
	
    private byte[] receiveData = new byte[10000];
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
	public UDPMaster(String dbName, int tablespace, int masterPort, int slavePort, ConcurrentHashMap<Integer, IoRequestInterface> requestContext)  throws IOException {
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
			System.out.println("UDPMaster constructed with "+DBName+" "+tablespace+" master port:"+masterPort+" slave:"+slavePort);
		}
		 clientSocket = new DatagramSocket(MASTERPORT);
	
	}
	
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.cluster.MasterInterface#setMasterPort(int)
	 */
	@Override
	public void setMasterPort(int port) {
		MASTERPORT = port;
	}
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.cluster.MasterInterface#setSlavePort(int)
	 */
	@Override
	public void setSlavePort(int port) {
		SLAVEPORT = port;
	}
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.cluster.MasterInterface#setRemoteWorkerName(java.lang.String)
	 */
	@Override
	public synchronized void setRemoteWorkerName(String rname) {
		remoteWorker = rname;
	}
	
	/**
	 * Look for messages coming back from the workers. Extract the UUID of the returned packet
	 * and get the real request from the COncurrentHashTable buffer
	 */
	@Override
	public void run() {
		while(shouldRun ) {
	      //DatagramSocket clientSocket;
			try {
			 //clientSocket = new DatagramSocket(MASTERPORT);
	   	     DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
	   	     clientSocket.receive(receivePacket);
	   	     byte[] b = receivePacket.getData();
	   	     IoResponseInterface iori = (IoResponseInterface) GlobalDBIO.deserializeObject(b);
	   	     synchronized(requestContext) {
	   	    	 // get the original request from the stored table
	   	    	 IoRequestInterface ior = requestContext.get(iori.getUUID());
	   	    	 if( DEBUG )
	   	    		 System.out.println("FROM Remote, size:" + b.length+" response:"+iori+" master port:"+MASTERPORT+" slave:"+SLAVEPORT);
	   	    	 //clientSocket.close();
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
	   	    		 System.out.println("UDPMaster ready to count down latch with "+ior);
	   	    	 }
	   	    	 // now add to any latches awaiting
	   	    	 CountDownLatch cdl = ((CompletionLatchInterface)ior).getCountDownLatch();
	   	    	 cdl.countDown();
	   	     }
			} catch (SocketException e) {
					System.out.println("UDPMaster receive socket error "+e+" Address:"+IPAddress+" master port:"+MASTERPORT+" slave:"+SLAVEPORT);
			} catch (IOException e) {
					System.out.println("UDPMaster receive IO error "+e+" Address:"+IPAddress+" master port:"+MASTERPORT+" slave:"+SLAVEPORT);
			}
	      }	
	}
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.cluster.MasterInterface#send(com.neocoretechs.bigsack.io.request.IoRequestInterface)
	 */
	@Override
	public void send(IoRequestInterface iori) {
	try {	
	    byte[] sendData;
	    DatagramSocket clientSocket;
		sendData = GlobalDBIO.getObjectAsBytes(iori);
		clientSocket = new DatagramSocket();
	    //clientSocket.connect(IPAddress,  SLAVEPORT);
	    if( DEBUG ) 
	     		System.out.println("Sending datagram to "+IPAddress+" port "+SLAVEPORT+" of length "+sendData.length);
	    DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, SLAVEPORT);
	    if( DEBUG ) 
     		System.out.println("Datagram sent to "+IPAddress+" port "+SLAVEPORT+" of length "+sendData.length);
	    clientSocket.send(sendPacket);
	    clientSocket.close();
	} catch (SocketException e) {
		System.out.println("UDPMaster send socket error "+e+" Address:"+IPAddress+" master port:"+MASTERPORT+" slave:"+SLAVEPORT);		
	}  catch (IOException e) {
		System.out.println("UDPMaster send IO error "+e+" Address:"+IPAddress+" master port:"+MASTERPORT+" slave:"+SLAVEPORT);	
	}
	}
	
	/* (non-Javadoc)
	 * @see com.neocoretechs.bigsack.io.cluster.MasterInterface#Fopen(java.lang.String, boolean)
	 */
	@Override
	public boolean Fopen(String fname, boolean create) throws IOException {
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
		cpi.setTransport("UDP");
		os.write(GlobalDBIO.getObjectAsBytes(cpi));
		os.flush();
		os.close();
		s.close();
		return true;
	}
	
}
