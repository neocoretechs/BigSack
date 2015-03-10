package com.neocoretechs.bigsack.io.request.cluster;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import com.neocoretechs.bigsack.io.IoInterface;

/**
 * Pump a request to determine if the tablespace has just been created down to the proper tablespace node.
 * @author jg
 *
 */
public final class IsNewRequest extends AbstractClusterWork implements CompletionLatchInterface, Serializable {
	private static final long serialVersionUID = -7217041923711244278L;
	private transient IoInterface ioUnit;
	private int tablespace;
	private boolean isnew = false;
	private transient CountDownLatch barrierCount;
	public IsNewRequest(){}
	public IsNewRequest(CountDownLatch barrierCount) {
		this.barrierCount = barrierCount;
	}
	
	@Override
	public void process() throws IOException {
		isnew = isNew();
		barrierCount.countDown();
	}
	/**
	* Return the size of the database tablespace at this node
	* @param tblsp The tablespace
	* @return The block available as a real, not virtual, block in this tablespace
	* @exception IOException if IO problem
	*/
	private boolean isNew() throws IOException {
				return ioUnit.isnew();
	}
	/**
	 * For this permutation we return a long 0 if isnew is false and a long 1 if isnew is true
	 */
	@Override
	public long getLongReturn() {
		return isnew ? 1L : 0L;
	}
	/**
	 * Returns a boolean object containing the isnew result
	 */
	@Override
	public Object getObjectReturn() {
		return new Boolean(isnew);
	}
	/**
	 * This method is called by queueRequest to set the proper tablespace from IOManager 
	 * It is the default way to set the active IO unit
	 */
	@Override
	public void setIoInterface(IoInterface ioi) {
		this.ioUnit = ioi;	
	}
	@Override
	public void setTablespace(int tablespace) {
		this.tablespace = tablespace;
	}
	public String toString() {
		return getUUID()+",tablespace:"+tablespace+":IsNewRequest "+isnew;
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
		isnew = (val == 0 ? false: true) ;
	}

	@Override
	public void setObjectReturn(Object o) {
		isnew = (Boolean) o;	
	}
	@Override
	public CyclicBarrier getCyclicBarrier() {
		return null;
	}
	@Override
	public void setCyclicBarrier(CyclicBarrier cb) {
	}

}
