package com.neocoretechs.bigsack.io.cluster.mpi;

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

import mpi.Intercomm;
import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;

import com.neocoretechs.arieslogger.core.impl.LogToFile;
import com.neocoretechs.bigsack.io.IOWorker;
import com.neocoretechs.bigsack.io.ThreadPoolManager;
import com.neocoretechs.bigsack.io.cluster.DistributedWorkerResponseInterface;
import com.neocoretechs.bigsack.io.cluster.NodeBlockBuffer;
import com.neocoretechs.bigsack.io.cluster.NodeBlockBufferInterface;
import com.neocoretechs.bigsack.io.cluster.WorkerRequestProcessor;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.request.IoRequestInterface;
import com.neocoretechs.bigsack.io.request.IoResponseInterface;
import com.neocoretechs.bigsack.io.request.cluster.CompletionLatchInterface;


/**
 * This class functions as the remote IOWorker. Two unidirectional channels provide full duplex
 * communication. Each 'master' and 'worker' connect via MPI. 
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
public class MPIWorker extends IOWorker implements DistributedWorkerResponseInterface, NodeBlockBufferInterface {
	private static final boolean DEBUG = true;
	volatile boolean shouldRun = true;
	public String MASTERPORT = "tcp://amimaster";
	public String SLAVEPORT = "tcp://ami0";
	public static String remoteMaster = "AMIMASTER";
	private String remoteDb;
    private byte[] sendData;
    private Intercomm master;
	
	private WorkerRequestProcessor workerRequestProcessor;
	// ByteBuffer for NIO socket read/write, currently broken under arm
	//private ByteBuffer b = ByteBuffer.allocate(LogToFile.DEFAULT_LOG_BUFFER_SIZE);
	
	private NodeBlockBuffer blockBuffer;
	private ByteBuffer bout;
	private ByteBuffer bin;
	/*
	 * This routine establishes communication with a server specified by port_name. 
	 * It is collective over the calling communicator and returns an intercommunicator 
	 * in which the remote group participated in an MPI_COMM_ACCEPT.
	 * If the named port does not exist (or has been closed), MPI_COMM_CONNECT raises an error of class MPI_ERR_PORT.
	 * If the port exists, but does not have a pending MPI_COMM_ACCEPT, the connection attempt will eventually time 
	 * out after an implementation-defined time, or succeed when the server calls MPI_COMM_ACCEPT. In the case of a 
	 * time out, MPI_COMM_CONNECT raises an error of class MPI_ERR_PORT
	 */
    public MPIWorker(String dbname, int tablespace, String masterPort, String slavePort, int L3Cache) throws IOException {
    	super(dbname, tablespace, L3Cache);
    	MASTERPORT= masterPort;
    	SLAVEPORT = slavePort;
		/**
		 * Java binding of {@code MPI_COMM_CONNECT}.
		 * @param port port name
		 * @param info implementation-specific information
		 * @param root rank in comm of root node
		 * @return intercommunicator with server as remote group
		 * @throws MPIException
		 */
		try {
			master = MPI.COMM_WORLD.connect(MASTERPORT, 1);
			
		} catch (MPIException e) {
			throw new IOException(e);
		}
		// spin the request processor thread for the worker
		workerRequestProcessor = new WorkerRequestProcessor(this);
		ThreadPoolManager.getInstance().spin(workerRequestProcessor);
		blockBuffer = new NodeBlockBuffer(this);
		if( DEBUG ) {
			System.out.println("Worker on port "+SLAVEPORT+" with master "+MASTERPORT+" database:"+dbname+
					" tablespace "+tablespace);
		}
	}
    
	public MPIWorker(String dbname, String remotedb, int tablespace, String masterPort, String slavePort, int l3Cache) throws IOException {
		this(dbname, tablespace, masterPort, slavePort,l3Cache);
		this.remoteDb = remotedb;
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
			System.out.println("Adding response "+irf+" to outbound from worker to port:"+MASTERPORT);
		}
		try {
			// connect to the master and establish persistent connect
			sendData = GlobalDBIO.getObjectAsBytes(irf);
			//ByteBuffer srcs = ByteBuffer.wrap(sendData);	
		    //int rank = -1;
			bout = MPI.newByteBuffer(sendData.length);
			bout.put(sendData);
			bout.flip();
			master.send(bout, sendData.length, MPI.BYTE, 1, 99);
		} catch (IOException | MPIException e) {
				System.out.println("Socket send error "+e+" on port "+MASTERPORT);
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
			System.out.println("Usage: java com.neocoretechs.bigsack.io.cluster.MPIWorker [database] [remotedb] [tablespace] [master port] [slave port]");
		}
		// Use mmap mode 0
		ThreadPoolManager.getInstance().spin(new MPIWorker(args[0], args[1], Integer.valueOf(args[2]), args[3], args[4], 0));
	}
	
	@Override
	public void run() {

		while(shouldRun) {
			try {
				bin.clear();
				master.recv(bin, bin.capacity(), MPI.BYTE, 1, 99);		
				//IoResponseInterface iori = (IoResponseInterface) GlobalDBIO.deserializeObject(bin);                                            
				// get the original request from the stored table
				//IoRequestInterface ior = requestContext.get(iori.getUUID());
				CompletionLatchInterface iori = (CompletionLatchInterface) GlobalDBIO.deserializeObject(bin); 
				if( DEBUG ) {
					System.out.println("MPIWorker FROM REMOTE on port:"+SLAVEPORT+" "+iori);
				}
				// Hook the request up to a real IoWorker
				iori.setIoInterface(this);
				// put the received request on the processing stack
				getRequestQueue().put(iori);
			} catch(IOException ioe) {
				System.out.println("MPIWorker receive exception "+ioe+" on port "+SLAVEPORT);
				break;
			} catch (InterruptedException e) {
				// the condition here is that the blocking request queue was waiting on a 'put' since the
				// queue was at maximum capacity, and a the ExecutorService requested a shutdown during that
				// time, we should bail form the thread and exit
			    // quit the processing thread
			    break;
			} catch (MPIException e) {
				System.out.println("MPIWorker class not found on deserialization"+e+" on port "+SLAVEPORT);
				break;
			} 
		}
		// thread has been stopped by WorkBoot or by error
		try {
				Intracomm.closePort(MASTERPORT);
				MPI.Finalize();
				MPI.Init(null);
		} catch (MPIException e3) {}
	

	}

	@Override
	public String getMasterPort() {
		return MASTERPORT;
	}

	@Override
	public String getSlavePort() {
		return SLAVEPORT;
	}

	public void stopWorker() {
		// thread has been stopped by WorkBoot
		shouldRun = false;
	}
}
