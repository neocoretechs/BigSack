package com.neocoretechs.bigsack.io.cluster;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;

import com.neocoretechs.bigsack.io.IOWorker;
import com.neocoretechs.bigsack.io.ThreadPoolManager;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.request.IoResponseInterface;
import com.neocoretechs.bigsack.io.request.cluster.AbstractClusterWork;
import com.neocoretechs.bigsack.io.request.cluster.CompletionLatchInterface;
import com.neocoretechs.bigsack.io.request.cluster.IoResponse;

/**
 * This class functions as the remote IOWorker 
 * Multiple threads on each node, one for each database, an Fopen spawns
 * additional instances of these so it acts as its own master in a sense.
 * Presumably, there is an instance of this present on each of the 8
 * tablespace worker nodes.
 * When a block comes down it gets written, if a block comes up it gets read.
 * the request comes down as a serialized object.
 * Instances of these are started by the WorkBoot controller node
 * @author jg
 *
 */
public class UDPWorker extends IOWorker implements DistributedWorkerResponseInterface, NodeBlockBufferInterface {
	private static final boolean DEBUG = true;
	boolean shouldRun = true;
	public int MASTERPORT = 9876;
	public int SLAVEPORT = 9876;
	public static String remoteMaster = "AMIMASTER";
    private byte[] receiveData = new byte[10000];
    private byte[] sendData;
	private InetAddress IPAddress = null;
	DatagramSocket responseSocket = null;
	
	private NodeBlockBuffer blockBuffer = new NodeBlockBuffer();
	
    public UDPWorker(String dbname, int tablespace, int masterPort, int slavePort, int L3Cache) throws IOException {
    	super(dbname, tablespace, L3Cache);
    	MASTERPORT= masterPort;
    	SLAVEPORT = slavePort;
		try {
			if(UDPMaster.TEST) {
				IPAddress = InetAddress.getLocalHost();
			} else {
				IPAddress = InetAddress.getByName(remoteMaster);
			}
		} catch (UnknownHostException e) {
			throw new RuntimeException("Bad remote master address:"+remoteMaster);
		}
		try {
				responseSocket = new DatagramSocket();
				responseSocket.connect(IPAddress, MASTERPORT);
		} catch (SocketException e) {
				System.out.println("Exception setting up socket "+IPAddress+" to remote master port "+MASTERPORT+" on local port "+SLAVEPORT+" "+e);
				responseSocket = null;
				return;
		}
		
		// spin the request processor thread for the worker
		ThreadPoolManager.getInstance().spin(new WorkerRequestProcessor(this));
		if( DEBUG ) {
			System.out.println("Worker on port "+SLAVEPORT+" with master "+MASTERPORT+" database:"+dbname+
					" tablespace "+tablespace+" address:"+IPAddress);
		}
	}
    
	public NodeBlockBuffer getBlockBuffer() { return blockBuffer; }
	
	/**
	 * Queue a request on this worker, the request is assumed to be on this tablespace
	 * Instead of queuing to a running thread request queue, queue this for outbound message
	 * The type is IOResponseInterface and contains the Id and the payload
	 * back to master
	 * @param irf
	 */
	public synchronized void queueResponse(IoResponseInterface irf) {
		if( DEBUG ) {
			System.out.println("UDPWorker Adding response "+irf+" to outbound from worker to "+IPAddress+" port:"+MASTERPORT);
		}
		// set up send

		try {
			sendData = GlobalDBIO.getObjectAsBytes(irf);
		} catch (IOException e) {
			System.out.println("UDPWorker queueResponse "+irf+" cant get serialized form due to "+e);
			if( responseSocket != null ) responseSocket.close();
			responseSocket = null;
		}
		DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, MASTERPORT);
		try {
			responseSocket.send(sendPacket);
		} catch (IOException e) {
			if( DEBUG )
				System.out.println("Socket send error "+e+" to address "+IPAddress+" on port "+MASTERPORT);
			if( responseSocket != null ) responseSocket.close();
			responseSocket = null;
		}
		
	}
	/**
     * Spin the worker, get the tablespace from the cmdl param
     * @param args
     * @throws Exception
     */
	public static void main(String args[]) throws Exception {
		if( args.length < 4 ) {
			System.out.println("Usage: java com.neocoretechs.bigsack.io.cluster.UDPWorker [database] [tablespace] [master port] [slave port]");
		}
		// Use mmap mode 0
		ThreadPoolManager.getInstance().spin(new UDPWorker(args[0], Integer.valueOf(args[1]), Integer.valueOf(args[2]), Integer.valueOf(args[3]), 0));
	}
	
	@Override
	public void run() {
		DatagramSocket serverSocket = null;
		try {
			serverSocket = new DatagramSocket(SLAVEPORT);
		} catch (SocketException e) {
			System.out.println("UDPWorker Cannot construct DatagramSocket to worker port "+SLAVEPORT);
			return;
		}
		DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
		while(shouldRun) {
			try {
				serverSocket.receive(receivePacket);
				byte[] databytes = receivePacket.getData();
				if( DEBUG ) {
					System.out.println("UDPWorker FROM REMOTE on port:"+SLAVEPORT+" size:"+databytes.length);
				}
				// extract the serialized request
				final CompletionLatchInterface iori = (CompletionLatchInterface) GlobalDBIO.deserializeObject(databytes);
				iori.setIoInterface(this);
				// put the received request on the processing stack
				getRequestQueue().put(iori);
			} catch(IOException ioe) {
				// most likely a broken pipe due to master break
				System.out.println("UDPWorker receive exception "+ioe+" on port "+SLAVEPORT);
				if( serverSocket != null ) serverSocket.close();
				if( responseSocket != null ) responseSocket.close();
				responseSocket = null;
				return;
			} catch (InterruptedException e) {
				// Executor shutdown while waiting for request queue to obtain a free slot
				if( serverSocket != null ) serverSocket.close();
				if( responseSocket != null ) responseSocket.close();
				responseSocket = null;
				return;
			}
		}
	}

	@Override
	public int getMasterPort() {
		return MASTERPORT;
	}

	@Override
	public int getSlavePort() {
		return SLAVEPORT;
	}

	
}
