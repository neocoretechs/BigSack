package com.neocoretechs.bigsack.io.cluster;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import com.neocoretechs.bigsack.io.IOWorker;
import com.neocoretechs.bigsack.io.ThreadPoolManager;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.request.IoResponseInterface;
import com.neocoretechs.bigsack.io.request.cluster.CompletionLatchInterface;


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
public class TCPWorker extends IOWorker implements DistributedWorkerResponseInterface {
	private static final boolean DEBUG = false;
	boolean shouldRun = true;
	public int MASTERPORT = 9876;
	public int SLAVEPORT = 9876;
	public static String remoteMaster = "AMIMASTER";
    private byte[] sendData;
	private InetAddress IPAddress = null;
	private ServerSocketChannel workerSocketChannel;
	private SocketAddress workerSocketAddress;
	private SocketChannel masterSocketChannel;
	private SocketAddress masterSocketAddress;
	private ByteBuffer b = ByteBuffer.allocate(10000);
	
    public TCPWorker(String dbname, int tablespace, int masterPort, int slavePort, int L3Cache) throws IOException {
    	super(dbname, tablespace, L3Cache);
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
		masterSocketAddress = new InetSocketAddress(IPAddress, MASTERPORT);
		masterSocketChannel = SocketChannel.open(masterSocketAddress);
		// start listening on the required worker port
		workerSocketAddress = new InetSocketAddress(SLAVEPORT);
		workerSocketChannel = ServerSocketChannel.open();
		workerSocketChannel.bind(workerSocketAddress);
		// spin the request processor thread for the worker
		ThreadPoolManager.getInstance().spin(new WorkerRequestProcessor(this));
		if( DEBUG ) {
			System.out.println("Worker on port "+SLAVEPORT+" with master "+MASTERPORT+" database:"+dbname+
					" tablespace "+tablespace+" address:"+IPAddress);
		}
	}
    
	/**
	 * Queue a request on this worker, the request is assumed to be on this tablespace
	 * Instead of queuing to a running thread request queue, queue this for outbound message
	 * The type is IOResponseInterface and contains the Id and the payload
	 * back to master
	 * @param irf
	 */
	public synchronized void queueResponse(IoResponseInterface irf) {
	
		if( DEBUG ) {
			System.out.println("Adding response "+irf+" to outbound from worker to "+IPAddress+" port:"+MASTERPORT);
		}
		try {
			// connect to the master and establish persistent connect
			sendData = GlobalDBIO.getObjectAsBytes(irf);
			ByteBuffer srcs = ByteBuffer.wrap(sendData);
			masterSocketChannel.write(srcs);
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
			System.out.println("Usage: java com.neocoretechs.bigsack.io.cluster.TCPWorker [database] [tablespace] [master port] [slave port]");
		}
		// Use mmap mode 0
		ThreadPoolManager.getInstance().spin(new TCPWorker(args[0], Integer.valueOf(args[1]), Integer.valueOf(args[2]), Integer.valueOf(args[3]), 0));
	}
	
	@Override
	public void run() {
		SocketChannel s = null;
		try {
			s = workerSocketChannel.accept();
		} catch (IOException e) {
			System.out.println("TCPWorker socket accept exception "+e+" on port "+SLAVEPORT);
			return;
		}
		while(shouldRun) {
			try {
				s.read(b);
				// extract the serialized request
				final CompletionLatchInterface iori = (CompletionLatchInterface)GlobalDBIO.deserializeObject(b);
				//s.close();
				if( DEBUG ) {
					System.out.println("FROM REMOTE on port:"+SLAVEPORT+" "+iori);
				}
				iori.setIoInterface(this);
				// put the received request on the processing stack
				getRequestQueue().add(iori);
				b.clear();
			} catch(IOException ioe) {
				System.out.println("TCPWorker receive exception "+ioe+" on port "+SLAVEPORT);
				break;
			} 
		}
		// thread has been stopped by WorkBoot
		try {
			s.close();
			masterSocketChannel.close();
			workerSocketChannel.close();
		} catch (IOException e) {}
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
