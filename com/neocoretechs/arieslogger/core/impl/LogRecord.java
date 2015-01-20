/*
*  Class com.neocoretechs.arieslogger.core.LogRecord
*  writes a record to the log file containing transaction Id, 'Loggable' operation and the group
*  value of the log record
*
*/

package com.neocoretechs.arieslogger.core.impl;

import com.neocoretechs.arieslogger.logrecords.Loggable;
import com.neocoretechs.arieslogger.logrecords.Undoable;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;

import java.io.Externalizable;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;


/**
	The log record written out to disk. This log record includes:
	<P>
    This is a holder object that may be setup using the setValue() and re-used
	rather than creating a new object for each actual log record.

	<P>	<PRE>
	The format of a log record is

	@.formatId LOG_RECORD
		the formatId is written by FormatIdOutputStream when this object is
		written out by writeObject
	@.purpose	The log record described every change to the persistent store
	@.upgrade
	@.diskLayout
		loggable group(CompressedInt)	the loggable's group value
		xactId(TransactionId)			The Transaction this log belongs to
		op(Loggable)					the log operation
	@.endFormat
	</PRE>

*/
public class LogRecord implements Externalizable {
	public static long serialVersionUID = 1193478925184398366L;
	private long	xactId;	// the transaction Id
	private int		group;	// the loggable's group value
	private Loggable op;		// the loggable

	private static final int LOG_RECORD = 0;
	private static final boolean DEBUG = false;

	public LogRecord() {}

	public void reset() {
		xactId = -1;
		op = null;
		group = Loggable.ALLGROUPS;
	}
	/**
		Write this out.
		@exception IOException error writing to log stream
	*/
	public void writeExternal(ObjectOutput out) throws IOException
	{
		out.writeInt(group);
		out.writeLong(xactId);
		out.writeObject(op);
	}

	public int getRecordSize() {
		try {
			return GlobalDBIO.getObjectAsBytes(this).length;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	/**
		Read this in
		@exception IOException error reading from log stream
		@exception ClassNotFoundException corrupted log stream
	*/
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
	{
		group = in.readInt();
		if( DEBUG ) {
			System.out.println("LogRecord.readExternal group "+group);
		}
		xactId = in.readLong();
		if( DEBUG ) {
			System.out.println("LogRecord.readExternal xactId "+xactId);
		}
		op = (Loggable) in.readObject();
		if( DEBUG ) {
			System.out.println("LogRecord.readExternal op "+op);
		}
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return LOG_RECORD;
	}

	/*
	 * class specific methods
	 */
	public void setValue(long xactId, Loggable op)
	{
		this.xactId = xactId;
		this.op = op;
		this.group = op.group();
	}


	public static int maxGroupStoredSize()
	{
		return Integer.MAX_VALUE;
	}	

	public static int maxTransactionIdStoredSize(long tranId)
	{
		return 8;
	}


	public long getTransactionId() throws IOException, ClassNotFoundException {
			return xactId;
	}

    public Loggable getLoggable() throws IOException, ClassNotFoundException {
		return op;
	}

    //public RePreparable getRePreparable() throws IOException, ClassNotFoundException 
    //{
    //    return((RePreparable) getLoggable());
	//}


	public Undoable getUndoable() throws IOException, ClassNotFoundException
	{
		if (op instanceof Undoable)
			return (Undoable) op;
		else
			return null;
	}

	public boolean isCLR()	{
		return ((group & Loggable.COMPENSATION) != 0);
	}

	public boolean isFirst()	{
		return ((group & Loggable.FIRST) != 0);
	}

	public boolean isComplete()	{
		return ((group & Loggable.LAST) != 0);
	}

	public boolean isPrepare()	{
		return ((group & Loggable.PREPARE) != 0);
	}

	public boolean requiresPrepareLocks()	{
		return ((group & Loggable.XA_NEEDLOCK) != 0);
	}

	public boolean isCommit()
	{
		if (DEBUG)
		{
			assert((group & Loggable.LAST) == Loggable.LAST);//,
				 //"calling isCommit on log record that is not last");
			assert((group & (Loggable.COMMIT | Loggable.ABORT)) != 0);//,
				 //"calling isCommit on log record before commit status is recorded");
		}
		return ((group & Loggable.COMMIT) != 0);
	}

	public boolean isAbort()
	{
		if (DEBUG)
		{
			assert((group & Loggable.LAST) == Loggable.LAST);//,
				 //"calling isAbort on log record that is not last");
			assert((group & (Loggable.COMMIT | Loggable.ABORT)) != 0);//,
				 //"calling isAbort on log record before abort status is recorded");
		}
		return ((group & Loggable.ABORT) != 0);
	}

	public int group()
	{
		return group;
	}


	public boolean isChecksum()	{
		return ((group & Loggable.CHECKSUM) != 0);
	}
}
