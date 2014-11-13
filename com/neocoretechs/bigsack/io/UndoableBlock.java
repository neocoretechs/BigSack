package com.neocoretechs.bigsack.io;

import java.io.IOException;
import java.io.Serializable;

import com.neocoretechs.arieslogger.core.LogInstance;
import com.neocoretechs.arieslogger.logrecords.Compensation;
import com.neocoretechs.arieslogger.logrecords.Loggable;
import com.neocoretechs.arieslogger.logrecords.Undoable;
import com.neocoretechs.bigsack.Props;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.BlockDBIO;

/**
 * 	Writes out a log record to the log stream, and call its applyChange method to
		apply the change to the rawStore.
		<BR>Any optional data the applyChange method needs is first written to the log
		stream using operation.writeOptionalData, then whatever is written to
		the log stream is passed back to the operation for the applyChange method.
 * @author jg
 *
 */
public class UndoableBlock implements Undoable, Serializable {
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
	public void applyChange(BlockDBIO xact, LogInstance instance, Object in) throws IOException {
		if( Props.DEBUG ) {
			System.out.println("UndoableBlock.applyChange: instance:"+instance+" raw store"+blkV2.getBlockNum()+","+blkV2.getBlk());
		}
		blkV2.getBlk().setPageLSN(instance.getValueAsLong());
		xact.FseekAndWrite(blkV2.getBlockNum(), blkV2.getBlk());
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
	public boolean needsRedo(BlockDBIO xact) throws IOException {
		return true;
	}

	@Override
	public void releaseResource(BlockDBIO xact) {

	}

	@Override
	public int group() {
		return Loggable.BI_LOG;
	}

	@Override
	public Compensation generateUndo(BlockDBIO xact, Object in) throws IOException {
		return new CompensationBlock();
	}

}
