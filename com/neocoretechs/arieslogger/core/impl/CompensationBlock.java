package com.neocoretechs.arieslogger.core.impl;

import java.io.IOException;
import java.io.Serializable;

import com.neocoretechs.arieslogger.core.LogInstance;
import com.neocoretechs.arieslogger.logrecords.Compensation;
import com.neocoretechs.arieslogger.logrecords.Loggable;
import com.neocoretechs.arieslogger.logrecords.Undoable;
import com.neocoretechs.bigsack.io.UndoableBlock;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.BlockDBIO;
import com.neocoretechs.bigsack.io.pooled.Datablock;
/**
 * <P> The sequence of events in recovery redo of a Loggable operation is:
		<NL>
		<LI> Get the loggable operation.  If loggable.needsRedo is false, then
		no need to redo this operation.
		<LI> If loggable.needsRedo is true, all the resources necessary for
		applying the applyChange is acquired in needsRedo.
		<LI> If the loggable is actually a compensation operation, then the
		logging system will find the undoable operation that needs to be
		undone and call compensation.setUndoOp with the undoable operation.
		<LI> The recovery system then calls loggable.applyChange, which re-applies the
		loggable operation, or re-applies the compensation operation
		<LI> The recovery system then calls loggable.releaseResource.
 * 
 * @author jg
 *
 */
public class CompensationBlock implements Compensation, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 532343753448457047L;
	private transient Undoable op;
	public CompensationBlock() {
	}
	/**
	 * 	When writing out a compensation log record to the log stream, logger calls
		applyChange method to undo the change of a previous log operation.
		We are going to get the V1 datablock from op and replace it
	 */
	@Override
	public void applyChange(BlockDBIO xact, LogInstance instance, Object in) throws IOException {
		BlockAccessIndex blk = ((UndoableBlock)op).getBlkV1();
		xact.FseekAndWrite(blk.getBlockNum(), blk.getBlk());
	}

	@Override
	public byte[] getPreparedLog() throws IOException {
		return null;
	}
	/**
	The sequence of events in recovery redo of a Loggable operation is:
	Get the loggable operation.  If loggable.needsRedo is false, then
	no need to redo this operation.
	If loggable.needsRedo is true, all the resources necessary for
		applying the applyChange is acquired in needsRedo.
	If the loggable is actually a compensation operation, then the
		logging system will find the undoable operation that needs to be
		undone and call compensation.setUndoOp with the undoable operation.
	The recovery system then calls loggable.applyChange, which re-applies the
		loggable operation, or re-applies the compensation operation
	The recovery system then calls loggable.releaseResource.
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
		return Loggable.COMPENSATION;
	}

	@Override
	public void setUndoOp(Undoable op) {
		this.op = op;
	}

}
