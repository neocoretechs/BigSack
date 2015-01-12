package com.neocoretechs.bigsack.io.cluster;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import com.neocoretechs.bigsack.io.request.IoRequestInterface;
import com.neocoretechs.bigsack.io.request.cluster.CompletionLatchInterface;
import com.neocoretechs.bigsack.io.request.cluster.IoResponse;

/**
 * Once requests from master are queued we extract them here and process them
 * @author jg
 *
 */
public final class UDPWorkerRequestProcessor implements Runnable {
	private BlockingQueue<IoRequestInterface> requestQueue;
	private UDPWorker worker;
	private boolean shouldRun = true;
	private static boolean DEBUG = true;
	public UDPWorkerRequestProcessor(UDPWorker worker) {
		this.worker = worker;
		requestQueue = worker.getRequestQueue();
	}
	@Override
	public void run() {
	  while(shouldRun ) {
		IoRequestInterface iori = null;
		try {
			iori = requestQueue.take();
		} catch (InterruptedException e1) {}
		// Down here at the worker level we only need to set the countdown latch to 1
		// because all operations are taking place on 1 tablespace and thread with coordination
		// at the UDPMaster level otherwise
		CountDownLatch cdl = new CountDownLatch(1);
		((CompletionLatchInterface)iori).setCountDownLatch(cdl);
		if( DEBUG  ) {
			System.out.println("port:"+worker.SLAVEPORT+" data:"+iori);
		}
		// tablespace set before request comes down
		
		try {
			iori.process();
			try {
				if( DEBUG )
					System.out.println("port:"+worker.SLAVEPORT+" avaiting countdown latch...");
				cdl.await();
			} catch (InterruptedException e) {}
			// we have flipped the latch from the request to the thread waiting here, so send an outbound response
			// with the result of our work if a response is required
			if( DEBUG ) {
				System.out.println("Local processing complete, queuing response to "+worker.MASTERPORT);
			}
			IoResponse ioresp = new IoResponse(iori);
			// And finally, send the package back up the line
			worker.queueResponse(ioresp);
			if( DEBUG ) {
				System.out.println("Response queued:"+ioresp);
			}
		} catch (IOException e1) {
			if( DEBUG ) {
				System.out.println("***Local processing EXCEPTION "+e1+", queuing fault to "+worker.MASTERPORT);
			}
			((CompletionLatchInterface)iori).setObjectReturn(e1);
			IoResponse ioresp = new IoResponse(iori);
			// And finally, send the package back up the line
			worker.queueResponse(ioresp);
			if( DEBUG ) {
				System.out.println("***FAULT Response queued:"+ioresp);
			}
		}
	  } //shouldRun
	}

}
