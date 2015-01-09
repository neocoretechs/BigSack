package com.neocoretechs.bigsack.io.cluster;

import java.io.IOException;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
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
public class UDPMaster implements Runnable {
	private static final boolean DEBUG = true;
	public static final boolean TEST = true;
	private int UDPPORT = 9876;
	private int WORKBOOTPORT = 8000;
	private static String remoteWorker = "AMI";
	private InetAddress IPAddress = null;

	private String DBName;
	private int tablespace;
	private boolean shouldRun = true;
	private BlockingQueue<IoRequestInterface> requestQueue;
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
	public UDPMaster(String dbName, int tablespace, int port, BlockingQueue<IoRequestInterface> requestQueue, ConcurrentHashMap<Integer, IoRequestInterface> requestContext)  throws IOException {
		this.DBName = dbName;
		this.tablespace = tablespace;
		this.UDPPORT = port;
		this.requestQueue = requestQueue;
		this.requestContext = requestContext;
		if( TEST ) {
			IPAddress = InetAddress.getLocalHost();
		} else {
			IPAddress = InetAddress.getByName(remoteWorker+String.valueOf(tablespace));
		}
	
	}
	
	public void setPort(int port) {
		UDPPORT = port;
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
	 * and get the real request from the COncurrentHashTable buffer
	 */
	@Override
	public void run() {
		while(shouldRun ) {
	      byte[] receiveData = new byte[1024];
	      DatagramSocket clientSocket;
			try {
			 clientSocket = new DatagramSocket(UDPPORT);
	   	     DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
	   	     clientSocket.receive(receivePacket);
	   	     byte[] b = receivePacket.getData();
	   	     IoResponseInterface iori = (IoResponseInterface) GlobalDBIO.deserializeObject(b);
	   	     // get the original request from the stored table
	   	     IoRequestInterface ior = requestContext.get(iori.getUUID());
	   	     if( DEBUG )
	   	    	 System.out.println("FROM SERVER:" + b.length+" user:"+iori.getUUID()+" port:"+UDPPORT);
	   	     clientSocket.close();
	   	     // set the return values in the original request to our values from remote workers
	   	     ((CompletionLatchInterface)ior).setLongReturn(iori.getLongReturn());
	   	     ((CompletionLatchInterface)ior).setObjectReturn(iori.getObjectReturn());
	   	     // now add to any latches awaiting
	   	     CountDownLatch cdl = ((CompletionLatchInterface)ior).getCountDownLatch();
	   	     cdl.countDown();
			} catch (SocketException e) {
					System.out.println("UDPMaster receive socket error "+e+" Address:"+IPAddress+" port "+UDPPORT);
			} catch (IOException e) {
					System.out.println("UDPMaster receive IO error "+e+" Address:"+IPAddress+" port "+UDPPORT);
			}
	      }	
	}
	
	public void send(IoRequestInterface iori) {
	try {	
	    byte[] sendData;
	    DatagramSocket clientSocket;
		sendData = GlobalDBIO.getObjectAsBytes(iori);
		clientSocket = new DatagramSocket();
	    clientSocket.connect(IPAddress,  UDPPORT);
	    if( DEBUG ) 
	     		System.out.println("Sending datagram to "+IPAddress+" port "+UDPPORT+" of length "+sendData.length);
	    DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, UDPPORT);
	    clientSocket.send(sendPacket);
	    clientSocket.close();
	} catch (SocketException e) {
		System.out.println("UDPMaster queueRequest socket error "+e);		
	}  catch (IOException e) {
		System.out.println("UDPMaster queueRequest IO error "+e);	
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
		CommandPacketInterface cpi = new CommandPacketInterface() {
			private static final long serialVersionUID = 893462083293613273L;
			String database;
			int tablespace;
			int port;
			@Override
			public String getDatabase() {
				return database;
			}
			@Override
			public void setDatabase(String database) {
				this.database = database;
			}
			@Override
			public int getTablespace() {
				return tablespace;
			}
			@Override
			public void setTablespace(int tablespace) {
				this.tablespace = tablespace;
			}
			@Override
			public int getPort() {
				return port;
			}
			@Override
			public void setPort(int port) {
				this.port = port;	
			}
		};
		cpi.setDatabase(DBName);
		cpi.setTablespace(tablespace);
		cpi.setPort(UDPPORT);
		os.write(GlobalDBIO.getObjectAsBytes(cpi));
		os.flush();
		os.close();
		s.close();
		return true;
	}
	
	public void setRequestQueue(BlockingQueue<IoRequestInterface> requestQueue) {
		this.requestQueue = requestQueue;	
	}
}
