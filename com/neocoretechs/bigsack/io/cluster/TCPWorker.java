package com.neocoretechs.bigsack.io.cluster;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.StandardSocketOptions;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import com.neocoretechs.arieslogger.core.impl.LogToFile;
import com.neocoretechs.bigsack.io.IOWorker;
import com.neocoretechs.bigsack.io.ThreadPoolManager;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.request.IoResponseInterface;
import com.neocoretechs.bigsack.io.request.cluster.CompletionLatchInterface;


/**
 * This class functions as the remote IOWorker. Two unidirectional channels provide full duplex
 * communication. Each 'master' and 'worker' connect via server socket and client from each. 
 * Multiple threads on each node, one set of master/worker/worker processor threads is spun
 * for each database. Each node maintains a specific tablespace for all databases.
 * An Fopen spawns additional instances of these threads.
 * Presumably, there is an instance of this present on each of the 8 tablespace worker nodes, but
 * any combination of nodes can be used as long as the target directory has the proper 'tablespace#'
 * subdirectories. The design has the target database path concatenated with the tablespace in cluster mode.
 * Actual operation is simple: When a block comes down it gets written, if a block comes up it gets read.
 * The request comes down as a serialized object similar to standalone requests, but with additional network garnish.
 * The network requests are interpreted as standard requests when they reach the IOWorker.
 * Instances of these TCPWorkers are started by the WorkBoot controller node in response to
 * the backchannel TCPServer requests. Existing threads are shut down and sockets closed, and a new batch of threads
 * are spun up if necessary.
 * @author jg
 * Copyright (C) NeoCoreTechs 2014,2015
 *
 */
public class TCPWorker extends IOWorker implements DistributedWorkerResponseInterface, NodeBlockBufferInterface {
	private static final boolean DEBUG = false;
	boolean shouldRun = true;
	public int MASTERPORT = 9876;
	public int SLAVEPORT = 9876;
	private String remoteMaster = "AMIMASTER";
    //private byte[] sendData;
	private InetAddress IPAddress = null;
	//private ServerSocketChannel workerSocketChannel;
	private SocketAddress workerSocketAddress;
	//private SocketChannel masterSocketChannel;
	private SocketAddress masterSocketAddress;
	
	private ServerSocket workerSocket;
	private Socket masterSocket;
	
	private WorkerRequestProcessor workerRequestProcessor;
	// ByteBuffer for NIO socket read/write, currently broken under arm
	//private ByteBuffer b = ByteBuffer.allocate(LogToFile.DEFAULT_LOG_BUFFER_SIZE);
	
	private NodeBlockBuffer blockBuffer;
	
    public TCPWorker(String dbname, int tablespace, String remoteMaster, int masterPort, int slavePort, int L3Cache) throws IOException {
    	super(dbname, tablespace, L3Cache);
    	if( remoteMaster != null )
    		this.remoteMaster = remoteMaster;
    	MASTERPORT= masterPort;
    	SLAVEPORT = slavePort;
		try {
			if(TCPMaster.TEST) {
				IPAddress = InetAddress.getLocalHost();
			} else {
				IPAddress = InetAddress.getByName(remoteMaster);
			}
		} catch (UnknownHostException e) {
			throw new RuntimeException("Bad remote master address:"+remoteMaster);
		}
		/*
		masterSocketAddress = new InetSocketAddress(IPAddress, MASTERPORT);
		masterSocketChannel = SocketChannel.open(masterSocketAddress);
		masterSocketChannel.configureBlocking(true);
		masterSocketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
		masterSocketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
		masterSocketChannel.setOption(StandardSocketOptions.SO_SNDBUF, 32767);
		masterSocketChannel.setOption(StandardSocketOptions.SO_RCVBUF, 32767);
		// start listening on the required worker port
		workerSocketAddress = new InetSocketAddress(SLAVEPORT);
		workerSocketChannel = ServerSocketChannel.open();
		workerSocketChannel.configureBlocking(true);
		workerSocketChannel.bind(workerSocketAddress);
		*/
		masterSocketAddress = new InetSocketAddress(IPAddress, MASTERPORT);
		masterSocket = new Socket();
		masterSocket.connect(masterSocketAddress);
		masterSocket.setKeepAlive(true);
		//masterSocket.setTcpNoDelay(true);
		masterSocket.setReceiveBufferSize(32767);
		masterSocket.setSendBufferSize(32767);
		// start listening on the required worker port
		workerSocketAddress = new InetSocketAddress(SLAVEPORT);
		workerSocket = new ServerSocket();
		workerSocket.bind(workerSocketAddress);
		// spin the request processor thread for the worker
		workerRequestProcessor = new WorkerRequestProcessor(this);
		ThreadPoolManager.getInstance().spin(workerRequestProcessor);
		blockBuffer = new NodeBlockBuffer(this);
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
	public void queueResponse(IoResponseInterface irf) {
	
		if( DEBUG ) {
			System.out.println("Adding response "+irf+" to outbound from worker to "+IPAddress+" port:"+MASTERPORT);
		}
		try {
			// connect to the master and establish persistent connect
			//sendData = GlobalDBIO.getObjectAsBytes(irf);
			//ByteBuffer srcs = ByteBuffer.wrap(sendData);
			//masterSocketChannel.write(srcs);
			OutputStream os = masterSocket.getOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(os);
			oos.writeObject(irf);
			oos.flush();
		} catch (SocketException e) {
				System.out.println("Exception setting up socket to remote master port "+MASTERPORT+" on local port "+SLAVEPORT+" "+e);
				throw new RuntimeException(e);
		} catch (IOException e) {
				System.out.println("Socket send error "+e+" to address "+IPAddress+" on port "+MASTERPORT);
				throw new RuntimeException(e);
		}
	}
	/**
     * Spin the worker, get the tablespace from the cmdl param
     * @param args
     * @throws Exception
     */
	public static void main(String args[]) throws Exception {
		if( args.length < 4 ) {
			System.out.println("Usage: java com.neocoretechs.bigsack.io.cluster.TCPWorker [database] [tablespace] [remote master] [master port] [slave port]");
		}
		// Use mmap mode 0
		ThreadPoolManager.getInstance().spin(new TCPWorker(
				args[0], // database
				Integer.valueOf(args[1]), // tablespace
				args[2], // remote master node
				Integer.valueOf(args[4]) , // master port
				Integer.valueOf(args[5]), //worker port
				0)); // L3 cache, mmap hardwired for now
	}
	
	@Override
	public void run() {
		//SocketChannel s = null;
		Socket s = null;
		try {
			/*
			s = workerSocketChannel.accept();
			s.configureBlocking(true);
			s.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
			s.setOption(StandardSocketOptions.TCP_NODELAY, true);
			s.setOption(StandardSocketOptions.SO_SNDBUF, 32767);
			s.setOption(StandardSocketOptions.SO_RCVBUF, 32767);
			*/
			s = workerSocket.accept();
			s.setKeepAlive(true);
			//s.setTcpNoDelay(true);
			s.setSendBufferSize(32767);
			s.setReceiveBufferSize(32767);
		} catch (IOException e) {
			System.out.println("TCPWorker socket accept exception "+e+" on port "+SLAVEPORT);
			return;
		}
		while(shouldRun) {
			try {
				//s.read(b);
				// extract the serialized request
				//final CompletionLatchInterface iori = (CompletionLatchInterface)GlobalDBIO.deserializeObject(b);
				//b.clear();
				InputStream ins = s.getInputStream();
				ObjectInputStream ois = new ObjectInputStream(ins);
				CompletionLatchInterface iori = (CompletionLatchInterface)ois.readObject();
				if( DEBUG ) {
					System.out.println("TCPWorker Queuing request "+iori+" on port "+SLAVEPORT);
				}
				// Hook the request up to a real IoWorker
				iori.setIoInterface(this);
				// put the received request on the processing stack
				getRequestQueue().put(iori);
			} catch(IOException ioe) {
				System.out.println("TCPWorker receive exception "+ioe+" on port "+SLAVEPORT);
				break;
			} catch (InterruptedException e) {
				// the condition here is that the blocking request queue was waiting on a 'put' since the
				// queue was at maximum capacity, and a the ExecutorService requested a shutdown during that
				// time, we should bail form the thread and exit
			    // quit the processing thread
			    break;
			} catch (ClassNotFoundException e) {
				System.out.println("TCPWorker class not found on deserialization"+e+" on port "+SLAVEPORT);
				break;
			} 
		}
		// thread has been stopped by WorkBoot or by error
		try {
			s.close();
			/*
			if( masterSocketChannel.isOpen() ) masterSocketChannel.close();
			if( workerSocketChannel.isOpen() ) workerSocketChannel.close();
			*/
			if(!masterSocket.isClosed()) masterSocket.close();
			if(!workerSocket.isClosed()) workerSocket.close();
			workerRequestProcessor.stop();
		} catch (IOException e) {}
	}

	@Override
	public String getMasterPort() {
		return String.valueOf(MASTERPORT);
	}

	@Override
	public String getSlavePort() {
		return String.valueOf(SLAVEPORT);
	}

	public void stopWorker() {
		// thread has been stopped by WorkBoot
		shouldRun = false;
	}
}
