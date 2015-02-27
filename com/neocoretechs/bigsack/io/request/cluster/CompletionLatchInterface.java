package com.neocoretechs.bigsack.io.request.cluster;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.io.request.IoRequestInterface;

/**
 * Interface to allow remote cluster requests to extract the latch and/or cyclic barrier
 * to be inspected outside of the request as well.
 * @author jg
 *
 */
public interface CompletionLatchInterface extends IoRequestInterface {

	public CountDownLatch getCountDownLatch();
	public void setCountDownLatch(CountDownLatch cdl);
	public CyclicBarrier getCyclicBarrier();
	public void setCyclicBarrier(CyclicBarrier cb);
	/**
	 * These methods are used to set the usually immutable return values lodged in requests
	 * Since we are replacing the original request values with the ones returned from cluster
	 * workers, we have to be able to change the normally immutable values
	 * @param val
	 */
	public void setLongReturn(long val);
	public void setObjectReturn(Object o);

}
