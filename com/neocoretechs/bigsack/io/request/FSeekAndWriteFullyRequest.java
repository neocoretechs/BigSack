package com.neocoretechs.bigsack.io.request;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.pooled.Datablock;
/**
 * Request to seek a block within a tablespace and write the contents of a block buffer
 * @author jg
 *
 */
public final class FSeekAndWriteFullyRequest implements IoRequestInterface {
	private IoInterface ioUnit;
	private long offset;
	private Datablock dblk;
	private int tablespace;
	private CountDownLatch barrierCount;
	public FSeekAndWriteFullyRequest(CountDownLatch barrierCount, long offset, Datablock dblk) {
		this.barrierCount = barrierCount;
		this.offset = offset;
		this.dblk = dblk;
	}
	@Override
	public synchronized void process() throws IOException {
		FseekAndWriteFully();
		barrierCount.countDown();
	}
	private void FseekAndWriteFully() throws IOException {
		ioUnit.Fseek(offset);
		dblk.write(ioUnit);
		dblk.setIncore(false);
		//if( Props.DEBUG ) System.out.print("GlobalDBIO.FseekAndWriteFully:"+valueOf(toffset)+" "+tblk.toVblockBriefString()+"|");
	}
	@Override
	public synchronized long getLongReturn() {
		return offset;
	}

	@Override
	public synchronized Object getObjectReturn() {
		return dblk;
	}
	@Override
	public synchronized void setIoInterface(IoInterface ioi) {
		this.ioUnit = ioi;		
	}
	@Override
	public synchronized void setTablespace(int tablespace) {
		this.tablespace = tablespace;
	}
	public synchronized String toString() {
		return "FSeekAndWriteFullyRequest for tablespace "+tablespace+" offset "+offset;
	}

}
