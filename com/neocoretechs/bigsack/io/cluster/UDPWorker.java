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
public class UDPWorker extends IOWorker {
	private static final boolean DEBUG = false;
	private static boolean shouldRun = true;
	public int UDPPORT = 9876;
	public static String remoteMaster = "AMIMASTER";
    private byte[] receiveData = new byte[10000];
    private byte[] sendData;
	private InetAddress IPAddress = null;
	
    public UDPWorker(String dbname, int tablespace, int port, int L3Cache) throws IOException {
    	super(dbname, tablespace, L3Cache);
    	UDPPORT= port;
		try {
			if(UDPMaster.TEST) {
				IPAddress = InetAddress.getLocalHost();
			} else {
				IPAddress = InetAddress.getByName(remoteMaster);
			}
		} catch (UnknownHostException e) {
			throw new RuntimeException("Bad remote master address:"+remoteMaster);
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
			System.out.println("Adding response "+irf+" to outbound from worker to "+remoteMaster);
		}
		// set up senddata
		DatagramSocket serverSocket = null;
		try {
			serverSocket = new DatagramSocket();
			serverSocket.connect(IPAddress, UDPPORT);
		} catch (SocketException e) {
			if( DEBUG )
				System.out.println("Exception setting up socket to remote master on poert "+UDPPORT+" "+e);
		}
		try {
			sendData = GlobalDBIO.getObjectAsBytes(irf);
		} catch (IOException e) {
			System.out.println("UDPWorker queueResponse "+irf+" cant get serialized form due to "+e);
		}
		DatagramPacket sendPacket =
			new DatagramPacket(sendData, sendData.length, IPAddress, UDPPORT);
		try {
			serverSocket.send(sendPacket);
		} catch (IOException e) {
			if( DEBUG )
				System.out.println("Socket send error "+e+" to address "+remoteMaster+" on port "+UDPPORT);
		}
		serverSocket.close();
	}
	/**
     * Spin the worker, get the tablespace from the cmdl param
     * @param args
     * @throws Exception
     */
	public static void main(String args[]) throws Exception {
		if( args.length < 3 ) {
			System.out.println("Usage: java com.neocoretechs.bigsack.io.cluster.UDPWorker [database] [tablespace] [port]");
		}
		// Use mmap mode 0
		ThreadPoolManager.getInstance().spin(new UDPWorker(args[0], Integer.valueOf(args[1]), Integer.valueOf(args[2]), 0));
	}
	
	@Override
	public void run() {
		while(shouldRun) {
			try {
				DatagramSocket serverSocket = new DatagramSocket(UDPPORT);
				DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
				serverSocket.receive(receivePacket);
				byte[] databytes = receivePacket.getData();
				serverSocket.close();
				// extract the serialized request
				final CompletionLatchInterface iori = (CompletionLatchInterface) GlobalDBIO.deserializeObject(databytes);
				iori.setIoInterface(this);
				// Down here at the worker level we only need to set the countdown latch to 1
				// because all operations are taking place on 1 tablespace and thread with coordination
				// at the UDPMaster level otherwise
				CountDownLatch cdl = new CountDownLatch(1);
				iori.setCountDownLatch(cdl);
				// tablespace set before request comes down
				iori.process();
				try {
					cdl.await();
				} catch (InterruptedException e) {
				}
				// we have flipped the latch from the request to the thread waiting here, so send an outbound response
				// with the result of our work
				if( DEBUG ) {
					System.out.println("Local processing complete, queuing response to "+remoteMaster);
				}
				IoResponse ioresp = new IoResponse(iori);
				// And finally, send the package back up the line
				queueResponse(ioresp);
				if( DEBUG ) {
					System.out.println("Queuing response to "+remoteMaster+" "+ioresp);
				}
				
			} catch(IOException ioe) {
				System.out.println("UDPWorker receive exception "+ioe);
			}
		}
	}

	
}
