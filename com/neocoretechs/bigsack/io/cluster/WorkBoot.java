package com.neocoretechs.bigsack.io.cluster;

import java.io.File;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

import com.neocoretechs.bigsack.io.IOWorker;
import com.neocoretechs.bigsack.io.ThreadPoolManager;
/**
 * Get an UDP address down, at the master we coordinate the assignment of UDP addresses
 * for each tablespace and node. It comes to this known address via TCP packet of serialized
 * command.  Also sent down are the tablespace and database to operate on
 * for the UDP worker we are spinning. 
 * @author jg
 *
 */
public final class WorkBoot extends TCPServer {
	private static boolean DEBUG = true;
	public static int port = 8000;
	private ConcurrentHashMap<String, IOWorker> dbToWorker = new ConcurrentHashMap<String, IOWorker>();
	/**
	 * Spin the worker, get the tablespace from the cmdl param
	 * @param args
	 * @throws Exception
	 */
	public static void main(String args[]) throws Exception {
		WorkBoot wb = new WorkBoot();
		wb.startServer(port);
		System.out.println("WorkBoot started on "+port+" of "+InetAddress.getLocalHost().getHostName());
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
                    if( (uworker = dbToWorker.get(db)) != null && o.getTransport().equals("TCP")) {
                    	((TCPWorker)uworker).stopWorker();
                    }
                    // bring up TCP or UDP worker
                    if(o.getTransport().equals("UDP")) {
                    	uworker = new UDPWorker(db, o.getTablespace(), o.getMasterPort(), o.getSlavePort(), 0);
                    } else {
                    	uworker = new TCPWorker(db, o.getTablespace(), o.getMasterPort(), o.getSlavePort(), 0);
                    }
                    	
                    dbToWorker.put(db, uworker);
                    ThreadPoolManager.getInstance().spin(uworker);
                    if( DEBUG ) {
                    		System.out.println("WorkBoot starting new worker "+db+" tablespace "+o.getTablespace()+" master port:"+o.getMasterPort()+" slave port:"+o.getSlavePort());
                    }	
				} catch(Exception e) {
                    System.out.println("TCPServer socket accept exception "+e);
                    System.out.println(e.getMessage());
                    e.printStackTrace();
               }
			}
	
	}

}
