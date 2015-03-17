package com.neocoretechs.bigsack.io.request;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.pooled.Datablock;

public final class FSeekAndReadFullyRequest implements IoRequestInterface {
	// ioUnit set before processing
	private IoInterface ioUnit;
	private long offset;
	private Datablock dblk;
	private int tablespace;
	private CountDownLatch barrierCount;
	public FSeekAndReadFullyRequest(CountDownLatch barrierCount, long offset, Datablock dblk) {
		this.barrierCount = barrierCount;
		this.offset = offset;
		this.dblk = dblk;
	}
	@Override
	public void process() throws IOException {
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
		if (tblk.isIncore())
					throw new RuntimeException(
						"GlobalDBIO.FseekAndRead: block incore preempts read "
							+ String.valueOf(toffset)
							+ " "
							+ tblk);
		synchronized(ioUnit) {
		ioUnit.Fseek(offset);
		tblk.read(ioUnit);
		}
	}
	@Override
	public long getLongReturn() {
		return offset;
	}

	@Override
	public Object getObjectReturn() {
		return this.dblk;
	}
	/**
	 * This interface implemented method is called by IoWorker before processing
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
		return "FSeekandReadFully request for tablespace "+tablespace+" offset "+offset+" "+ioUnit.Fname();
	}

}
