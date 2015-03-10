package com.neocoretechs.bigsack.io.request;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.pooled.Datablock;


public final class FSeekAndReadRequest implements IoRequestInterface {
	private static final boolean DEBUG = false;
	private IoInterface ioUnit;
	private long offset;
	private Datablock dblk;
	private int tablespace;
	private CountDownLatch barrierCount;
	public FSeekAndReadRequest(CountDownLatch barrierCount, long offset, Datablock dblk) {
		this.barrierCount = barrierCount;
		this.offset = offset;
		this.dblk = dblk;
	}
	/**
	 * IoInterface should be set up before we come in here. We assume toffset is real block position
	 * in this tablespace since we have come here knowing our tablespace number and so our real block number
	 * was also extracted from the virtual block we started with.
	 * @param toffset
	 * @param tblk
	 * @throws IOException
	 */
	@Override
	public void process() throws IOException {
		assert(ioUnit != null) : "FseekAndReadRequest ioUnit is not initialized";
		if(DEBUG)
			System.out.println("FSeekAndReadRequest ioUnit:"+ioUnit);
		assert(!dblk.isIncore()) : "FseekAndReadRequest block incore preempts read " + offset + " "+ dblk;
			//if( tablespace ==1 && offset== 114688) {
			//	System.out.println("FSeekAndReadRequest.process1 pos:"+ioUnit.Ftell()+" open "+ioUnit.isopen()+" write "+ioUnit.iswriteable()+" chan "+ioUnit.getChannel().isOpen());
			//}
		ioUnit.Fseek(offset);
			//if( tablespace ==1 && offset== 114688) {
			//		System.out.println("FSeekAndReadRequest.process2 pos:"+ioUnit.Ftell()+" DATA:"+dblk.blockdump()+" open "+ioUnit.isopen()+" write "+ioUnit.iswriteable()+" chan "+ioUnit.getChannel().isOpen());
			//}
		dblk.readUsed(ioUnit);
			
		//assert(dblk.getBytesused() > 0 ) : "FseekAndReadRequest block read bad for "+this+" "+dblk.blockdump();
			
		if( DEBUG ) 
			System.out.println("FseekAndRead in "+this.toString()+" exiting");
		//if( tablespace ==1 && offset== 114688)
		//	System.out.println("MultithreadedIOManager.FseekAndReadRequest processing Tablespace_1_114688 "+dblk.blockdump());

		barrierCount.countDown();
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
		return "FSeekAndReadRequest for tablespace "+tablespace+" offset "+offset+" "+ioUnit.Fname();
	}

}
