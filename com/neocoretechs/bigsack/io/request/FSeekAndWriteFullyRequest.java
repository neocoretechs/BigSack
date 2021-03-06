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
	public void process() throws IOException {
		FseekAndWriteFully();
		barrierCount.countDown();
	}
	/**
	 * Seek the black and fully write the data and headers, perform Fforce to ensure writethrough
	 * @throws IOException
	 */
	private void FseekAndWriteFully() throws IOException {
		synchronized(ioUnit) {
			ioUnit.Fseek(offset);
			dblk.write(ioUnit);
			ioUnit.Fforce();
			dblk.setIncore(false);
		}
		//if( DEBUG ) System.out.print("GlobalDBIO.FseekAndWriteFully:"+valueOf(toffset)+" "+tblk.toVblockBriefString()+"|");
	}
	@Override
	public long getLongReturn() {
		return offset;
	}

	@Override
	public Object getObjectReturn() {
		return dblk;
	}
	@Override
	public void setIoInterface(IoInterface ioi) {
		this.ioUnit = ioi;		
	}
	@Override
	public void setTablespace(int tablespace) {
		this.tablespace = tablespace;
	}
	public String toString() {
		return "FSeekAndWriteFullyRequest for tablespace "+tablespace+" offset "+offset;
	}

}
