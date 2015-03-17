package com.neocoretechs.bigsack.io.request;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.pooled.Datablock;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
/**
 * Request to seek a block within a tablespace and write the contents of a block buffer
 * @author jg
 *
 */
public final class FSeekAndWriteRequest implements IoRequestInterface {
	private static boolean DEBUG = false;
	private IoInterface ioUnit;
	private long offset;
	private Datablock dblk;
	private int tablespace;
	private CountDownLatch barrierCount;
	public FSeekAndWriteRequest(CountDownLatch barrierCount, long offset, Datablock dblk) {
		this.barrierCount = barrierCount;
		this.offset = offset;
		this.dblk = dblk;
	}
	@Override
	public void process() throws IOException {
		if( DEBUG )
			System.out.println(this);
		FseekAndWrite();
		barrierCount.countDown();
	}
	private void FseekAndWrite() throws IOException {
		assert( (dblk.getBytesused() == 0 || dblk.getBytesinuse() == 0) && 
				(dblk.getData()[0] == 0xAC && dblk.getData()[1] == 0xED && dblk.getData()[2] == 0x00 && dblk.getData()[3] == 0x05) )
			: "Block has zero in use values but object stream header for "+ioUnit.Fname()+" tablespace "+tablespace+" pos:"+GlobalDBIO.valueOf(offset)+" Data:"+dblk.blockdump();
		synchronized(ioUnit) {
			ioUnit.Fseek(offset);
			dblk.writeUsed(ioUnit);
			dblk.setIncore(false);
		}
		//if( Props.DEBUG ) System.out.print("GlobalDBIO.FseekAndWriteFully:"+valueOf(toffset)+" "+tblk.toVblockBriefString()+"|");
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
		return "FSeekAndWriteRequest for tablespace "+tablespace+" offset "+offset;
	}

}
