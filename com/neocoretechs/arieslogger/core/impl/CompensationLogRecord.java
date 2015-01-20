package com.neocoretechs.arieslogger.core.impl;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.neocoretechs.arieslogger.core.LogInstance;
import com.neocoretechs.arieslogger.logrecords.Compensation;
/**
 * A variation of LogRecord that adds an undo instance for compensation log records
 * This is in lieu of writing them out of band after a compensation record. With this, the undo instance
 * is encapsulated in the object
 * @author jg
 *
 */
public class CompensationLogRecord extends LogRecord implements Externalizable {
	private long undoInstance;
	public CompensationLogRecord() {}
	public CompensationLogRecord(long transId, Compensation comp, LogInstance inst) {
		undoInstance = inst.getValueAsLong();
		setValue(transId, comp);
	}
	
	public void reset() {
		super.reset();
		undoInstance = LogInstance.INVALID_LOG_INSTANCE;
	}
	public LogInstance getUndoInstance() { return new LogCounter(undoInstance); }
	
	public void writeExternal(ObjectOutput out) throws IOException
	{
		super.writeExternal(out);
	}

	/**
		Read this in
		@exception IOException error reading from log stream
		@exception ClassNotFoundException corrupted log stream
	*/
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
	{
		super.readExternal(in);
	}

}
