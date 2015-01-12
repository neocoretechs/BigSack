package com.neocoretechs.bigsack.io.request.cluster;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.CountDownLatch;

import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.pooled.Datablock; 
/**
 * CompletionLatchInterface extend IORequestInterface to provide access to barrier synch latches from the request.
 * We isolate the latches before sending request to the nodes. When a response comes back the latches are used to coordinate
 * the responses.
 * @author jg
 *
 */
public final class FSeekAndReadFullyRequest extends AbstractClusterWork implements CompletionLatchInterface, Serializable {
	private static final long serialVersionUID = -7042979197184449526L;
	private transient IoInterface ioUnit;
	private long offset;
	private Datablock dblk;
	private int tablespace;
	private transient CountDownLatch barrierCount;
	public FSeekAndReadFullyRequest() {}
	
	public FSeekAndReadFullyRequest(CountDownLatch barrierCount, long offset, Datablock dblk) {
		this.barrierCount = barrierCount;
		this.offset = offset;
		this.dblk = dblk;
	}
	@Override
	public synchronized void process() throws IOException {
		FseekAndReadFully(this.offset, this.dblk);
		barrierCount.countDown();
	}
	/**
	 * IoInterface should be set up before we come in here. We assume toffset is real block position
	 * in this tablespace since we have come here knowing our tablespace number and so our real block number
	 * was also extracted from the virtual block we started with.
	 * @param toffset
	 * @param tblk
	 * @throws IOException
	 */
	private void FseekAndReadFully(long toffset, Datablock tblk) throws IOException {
		/*
		if (ioUnit == null) {
			throw new RuntimeException(
				"FseekAndReadRequest tablespace null "
					+ toffset
					+ " = "
					+ ioUnit);
		}
		if (tblk == null) {
			throw new RuntimeException(
				"FseekAndReadRequests Datablock null "
					+ toffset
					+ " = "
					+ ioUnit);
		}
		*/
		if (tblk.isIncore())
					throw new RuntimeException(
						"GlobalDBIO.FseekAndRead: block incore preempts read "
							+ String.valueOf(toffset)
							+ " "
							+ tblk);
		ioUnit.Fseek(offset);
		tblk.read(ioUnit);
	}
	@Override
	public synchronized long getLongReturn() {
		return offset;
	}

	@Override
	public synchronized Object getObjectReturn() {
		return this.dblk;
	}
	/**
	 * This interface implemented method is called by IoWorker before processing
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
		return getUUID()+",tablespace:"+tablespace+"FSeekAndReadFullyRequest:"+offset;
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
		offset = val;
	}

	@Override
	public void setObjectReturn(Object o) {
		dblk = (Datablock) o;	
	}


}
