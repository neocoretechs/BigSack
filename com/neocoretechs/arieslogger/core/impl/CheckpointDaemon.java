package com.neocoretechs.arieslogger.core.impl;

import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.neocoretechs.arieslogger.core.CheckpointDaemonInterface;
import com.neocoretechs.arieslogger.core.LogFactory;

public class CheckpointDaemon implements CheckpointDaemonInterface {
	private Vector<LogFactory> logList = new Vector<LogFactory>();
	private Object mutex = new Object();
	private int client;
	public CheckpointDaemon() {
		exec(getService());
	}
	private void exec(Runnable machine) {
	    DaemonThreadFactory dtf = new DaemonThreadFactory();
	    ExecutorService executor = Executors.newSingleThreadExecutor(dtf);
        executor.execute(machine);
	}
	
/* (non-Javadoc)
 * @see com.neocoretechs.arieslogger.core.impl.CheckpointDaemonInterface#subscribe(com.neocoretechs.arieslogger.core.impl.LogFactory)
 */
@Override
public int subscribe(LogFactory logFactory) {
	if( logList.contains(logFactory)) {
		return logList.indexOf(logFactory);
	}
	logList.add(logFactory);
	return logList.size()-1;
}

public void unsubscribe(int logFactory) {
	logList.remove(logFactory);
}
/* (non-Javadoc)
 * @see com.neocoretechs.arieslogger.core.impl.CheckpointDaemonInterface#serviceNow(int)
 */
@Override
public void serviceNow(int client) {
	synchronized(mutex) {
		this.client = client;
		mutex.notify();
	}
}

private Runnable getService() {
	return new Runnable() {
	public void run() {
		// AtomicInteger getVal = new AtomicInteger();
    	// this loop basically waits until thats done.
    	try {
    		while(true) {
    	        synchronized(mutex) {
    	        		mutex.wait();
    	        		logList.get(client).checkpoint(true); // wait for any existing op to complete
    	        	}
    	    }
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
	};
}
class DaemonThreadFactory implements ThreadFactory {
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        return thread;
    }
}

}
