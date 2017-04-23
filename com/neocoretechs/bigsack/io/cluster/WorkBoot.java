package com.neocoretechs.bigsack.io.cluster;

import java.io.File;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

import mpi.MPI;
import mpi.MPIException;

import com.neocoretechs.bigsack.io.IOWorker;
import com.neocoretechs.bigsack.io.ThreadPoolManager;
import com.neocoretechs.bigsack.io.cluster.mpi.MPIWorker;
/**
 * Get an address down, at the master we coordinate the assignment of addresses
 * for each tablespace and node. It comes to this known address via TCP packet of serialized
 * command.  Also sent down are the tablespace and database to operate on
 * for the worker we are spinning.  We may spin a local worker, a UDP worker,a TCP worker or an MPI worker depending
 * on passed packet at spin up time.
 * @author jg
 *
 */
public final class WorkBoot extends TCPServer {
	private static boolean DEBUG = true;
	private static boolean mpiIsInit = false;
	private static int mpiThreadProvided;
	private static String[] mpiArgs = new String[0];
	public static int port = 8000;
	private ConcurrentHashMap<String, IOWorker> dbToWorker = new ConcurrentHashMap<String, IOWorker>();
	/**
	 * Spin the worker, get the tablespace from the cmdl param
	 * @param args
	 * @throws Exception
	 */
	public static void main(String args[]) throws Exception {
		if( args.length > 0 ) {
			port = Integer.valueOf(args[0]);
		}
		WorkBoot wb = new WorkBoot();
		wb.startServer(port);
		System.out.println("WorkBoot started on "+port+" of "+InetAddress.getLocalHost().getHostName());
	}
	
	private void MpiInit() throws MPIException {
		mpiThreadProvided = MPI.InitThread(mpiArgs, MPI.THREAD_FUNNELED);
		mpiIsInit = true;
	}
	public void run() {
			while(!shouldStop) {
				try {
					Socket datasocket = server.accept();
                    // disable Nagles algoritm; do not combine small packets into larger ones
                    datasocket.setTcpNoDelay(true);
                    // wait 1 second before close; close blocks for 1 sec. and data can be sent
                    datasocket.setSoLinger(true, 1);
					//
                    ObjectInputStream ois = new ObjectInputStream(datasocket.getInputStream());
                    CommandPacketInterface o = (CommandPacketInterface) ois.readObject();
                    if( DEBUG )
                    	System.out.println("WorkBoot command received:"+o);
                    //datasocket.close();
                	// Create a new UDPWorker with database and tablespace
            		// Use mmap mode 0
                    // replace any marker of $ with tablespace number
                    String db = o.getDatabase();
                    if( db.indexOf('$') != -1) {
                    	db = db.replace('$', String.valueOf(o.getTablespace()).charAt(0));
                    }
                    db = (new File(db)).toPath().getParent().toString() + File.separator +
                    		"tablespace"+String.valueOf(o.getTablespace()) + File.separator +
                    		(new File(o.getDatabase()).getName());
                    // determine if this worker has started, if so, cancel thread and start a new one.
                    IOWorker uworker = null;
                    if( (uworker = dbToWorker.get(db)) != null &&  o.getTransport().equals("TCP")) {
                    	((TCPWorker)uworker).stopWorker();
                    } else {
                        if( (uworker = dbToWorker.get(db)) != null &&  o.getTransport().equals("MPI")) {
                        	((MPIWorker)uworker).stopWorker();
                        }
                    }
                    // bring up TCP or UDP or MPI  worker
                    if(o.getTransport().equals("UDP")) {
                    	uworker = new UDPWorker(db, o.getTablespace(), o.getMasterPort(), o.getSlavePort(), 0);
                    } else {
                    	if( o.getTransport().equals("MPI")) {
                    		if( !mpiIsInit ) MpiInit();
                    		uworker = new MPIWorker(db, o.getTablespace(), o.getMasterPort(), o.getSlavePort(), 0);
                    	} else {
                    		uworker = new TCPWorker(db, o.getTablespace(), o.getRemoteMaster(), Integer.valueOf(o.getMasterPort()), Integer.valueOf(o.getSlavePort()), 0);
                    	}
                    }
                    	
                    dbToWorker.put(db, uworker);
                    ThreadPoolManager.getInstance().spin(uworker);
                    if( DEBUG ) {
                    		System.out.println("WorkBoot starting new worker "+db+" tablespace "+o.getTablespace()+" master port:"+o.getMasterPort()+" slave port:"+o.getSlavePort());
                    }	
				} catch(Exception e) {
                    System.out.println("BigSack backchannel node configuration server socket accept exception "+e);
                    System.out.println(e.getMessage());
                    e.printStackTrace();
               }
			}
	
	}

}
