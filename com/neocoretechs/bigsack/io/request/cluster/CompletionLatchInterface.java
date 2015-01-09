package com.neocoretechs.bigsack.io.request.cluster;

import java.util.concurrent.CountDownLatch;

import com.neocoretechs.bigsack.io.request.IoRequestInterface;

/**
 * Interface to allow remote cluster requests to extract the latch to be used outside request
 * @author jg
 *
 */
public interface CompletionLatchInterface extends IoRequestInterface {

	public CountDownLatch getCountDownLatch();
	public void setCountDownLatch(CountDownLatch cdl);
	
	/**
	 * These methods are used to set the usually immutable return values lodged in requests
	 * Since we are replacing the original request values with the ones returned from cluster
	 * workers, we have to be able to change the normally immutable values
	 * @param val
	 */
	public void setLongReturn(long val);
	public void setObjectReturn(Object o);

}
