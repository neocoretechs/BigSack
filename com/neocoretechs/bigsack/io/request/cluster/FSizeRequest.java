package com.neocoretechs.bigsack.io.request.cluster;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.io.IoInterface;

/**
 * Pump an fsize request down to the proper tablespace node.
 * @author jg
 *
 */
public final class FSizeRequest extends AbstractClusterWork implements CompletionLatchInterface, Serializable {
	private static final long serialVersionUID = 2428265445819080353L;
	private transient IoInterface ioUnit;
	private int tablespace;
	private long fSize = 0L;
	private transient CountDownLatch barrierCount;
	public FSizeRequest(){}
	public FSizeRequest(CountDownLatch barrierCount) {
		this.barrierCount = barrierCount;
	}
	
	@Override
	public synchronized void process() throws IOException {
		fsize();
		barrierCount.countDown();
	}
	/**
	* Return the size of the database tablespace at this node
	* @exception IOException if IO problem
	*/
	private void fsize() throws IOException {
		fSize = ioUnit.Fsize();
	}
	@Override
	public synchronized long getLongReturn() {
		return fSize;
	}

	@Override
	public synchronized Object getObjectReturn() {
		return new Long(fSize);
	}
	/**
	 * This method is called by queueRequest to set the proper tablespace from IOManager 
	 * It is the default way to set the active IO unit
	 */
	@Override
	public synchronized void setIoInterface(IoInterface ioi) {
		this.ioUnit = ioi;	
	}
	@Override
	public synchronized void setTablespace(int tablespace) {
		this.tablespace = tablespace;
	}
	public synchronized String toString() {
		return getUUID()+",tablespace:"+tablespace+"FSizeRequest:"+fSize;
	}
	/**
	 * The latch will be extracted by the UDPMaster and when a response comes back it will be tripped
	 */
	@Override
	public CountDownLatch getCountDownLatch() {
		return barrierCount;
	}

	@Override
	public void setCountDownLatch(CountDownLatch cdl) {
		barrierCount = cdl;
	}
	
	@Override
	public void setLongReturn(long val) {
		fSize = val;
	}

	@Override
	public void setObjectReturn(Object o) {
		fSize = (Long) o;	
	}
	@Override
	public CyclicBarrier getCyclicBarrier() {
		return null;
	}
	@Override
	public void setCyclicBarrier(CyclicBarrier cb) {
	}

}
