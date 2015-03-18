package com.neocoretechs.bigsack.io;

import java.io.IOException;
import java.io.Serializable;

import com.neocoretechs.arieslogger.core.LogInstance;
import com.neocoretechs.arieslogger.core.impl.CompensationBlock;
import com.neocoretechs.arieslogger.logrecords.Compensation;
import com.neocoretechs.arieslogger.logrecords.Loggable;
import com.neocoretechs.arieslogger.logrecords.Undoable;
import com.neocoretechs.bigsack.Props;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.pooled.ObjectDBIO;

/**
 * 	Writes out a log record to the log stream, and call its applyChange method to
		apply the change to the rawStore.
		<BR>Any optional data the applyChange method needs is first written to the log
		stream using operation.writeOptionalData, then whatever is written to
		the log stream is passed back to the operation for the applyChange method.
 * @author jg
 *
 */
public final class UndoableBlock implements Undoable, Serializable {
	private static final long serialVersionUID = 3823704109110419908L;
	private BlockAccessIndex blkV1,blkV2; // utility blocks
	
	public UndoableBlock(BlockAccessIndex tblk, BlockAccessIndex blk) {
		blkV1 = tblk;
		blkV2 = blk;
	}

	public BlockAccessIndex getBlkV1() {
		return blkV1;
	}

	public BlockAccessIndex getBlkV2() {
		return blkV2;
	}

	/**
	 * When writing out a log record to the log stream, logger will call its applyChange method to
		apply the change to the rawStore.
		Any optional data the applyChange method needs is first written to the log
		stream using operation.writeOptionalData, then whatever is written to
		the log stream is passed back to the operation for the applyChange method.
	 */
	@Override
	public void applyChange(ObjectDBIO xact, LogInstance instance, Object in) throws IOException {
		if( Props.DEBUG ) {
			System.out.println("UndoableBlock.applyChange: instance:"+instance+" raw store"+blkV2.getBlockNum()+","+blkV2.getBlk());
		}
		blkV2.getBlk().setPageLSN(instance.getValueAsLong());
		//xact.FseekAndWrite(blkV2.getBlockNum(), blkV2.getBlk()); // sets incore false
		int tblsp = GlobalDBIO.getTablespace(blkV2.getBlockNum());
		long blkn = GlobalDBIO.getBlock(blkV2.getBlockNum());
		synchronized(xact.getIOManager().getDirectIO(tblsp)) {
			xact.getIOManager().getDirectIO(tblsp).Fseek(blkn);
			blkV2.getBlk().write(xact.getIOManager().getDirectIO(tblsp));
		}
		// deallocate
		blkV2.decrementAccesses();
	}

	@Override
	public byte[] getPreparedLog() throws IOException {
		return null;
	}
	/**
	 * 	<P> The sequence of events in recovery redo of a Loggable operation is:
		<NL>
		<LI> Get the loggable operation.  If loggable.needsRedo is false, then
		no need to redo this operation.
		<LI> If loggable.needsRedo is true, all the resources necessary for
		applying the applyChange is acquired in needsRedo.
		<LI> If the loggable is actually a compensation operation, then the
		logging system will find the undoable operation that needs to be
		undone, call compensation.setUndoOp with the undoable operation.
		<LI> The recovery system then calls loggable.applyChange, which re-applies the
		loggable operation, or re-applies the compensation operation
		<LI> The recovery system then calls loggable.releaseResource.
	 */
	@Override
	public boolean needsRedo(ObjectDBIO xact) throws IOException {
		return true;
	}

	@Override
	public void releaseResource(ObjectDBIO xact) {

	}

	@Override
	public int group() {
		return Loggable.BI_LOG;
	}

	@Override
	public Compensation generateUndo(ObjectDBIO xact) throws IOException {
		return new CompensationBlock();
	}

	public String toString() {
		return "Undoable block version 1:"+blkV1+" version 2:"+blkV2;
	}
}
