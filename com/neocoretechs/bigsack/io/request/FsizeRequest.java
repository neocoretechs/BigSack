package com.neocoretechs.bigsack.io.request;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import com.neocoretechs.bigsack.io.IoInterface;

public class FsizeRequest implements IoRequestInterface {
	private static boolean DEBUG = false;
	private int tablespace;
	private CountDownLatch barrierCount;
	private IoInterface ioUnit;
	private long size;
	public FsizeRequest(CountDownLatch barrierCount) {
		this.barrierCount = barrierCount;
	}
	@Override
	public void process() throws IOException {
		synchronized(ioUnit) {
		size = ioUnit.Fsize();
		}
		barrierCount.countDown();
	}
	@Override
	public long getLongReturn() {
		return size;
	}

	@Override
	public Object getObjectReturn() {
		return new Long(size);
	}
	/**
	 * This method is called by queueRequest to set the proper tablespace from IOManager 
	 * It is the default way to set the active IO unit
	 */
	@Override
	public void setIoInterface(IoInterface ioi) {
		ioUnit = ioi;
	}
	@Override
	public void setTablespace(int tablespace) {
		this.tablespace = tablespace;
	}
	public String toString() {
		return "FSize Request for "+ioUnit.Fname()+" tablespace "+tablespace;
	}

}
