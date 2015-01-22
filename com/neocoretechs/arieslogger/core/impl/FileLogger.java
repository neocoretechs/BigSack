/*

    - Class com.neocoretechs.arieslogger.core.FileLogger

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.neocoretechs.arieslogger.core.impl;

import com.neocoretechs.arieslogger.core.LogInstance;
import com.neocoretechs.arieslogger.core.Logger;
import com.neocoretechs.arieslogger.core.impl.LogToFile;
import com.neocoretechs.arieslogger.core.StreamLogScan;
import com.neocoretechs.arieslogger.logrecords.Compensation;
import com.neocoretechs.arieslogger.logrecords.Loggable;
import com.neocoretechs.arieslogger.logrecords.Undoable;
import com.neocoretechs.bigsack.io.pooled.BlockDBIO;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;


/**
	Write log records to a log file as a stream
	(ie. log records added to the end of the file, no concept of pages).
<P>
	The format of a log record that is not a compensation operation is
	<PRE>
	@.formatId	no formatId, format is implied by the log file format and the
		log record content.
	@.purpose	the log record and optional data
	@.upgrade
	@.diskLayout
		Log Record
			(see com.neocoretechs.arieslogger.core.LogRecord)
		length(int)	length of optional data
		optionalData(byte[length]) optional data written by the log record
	@.endFormat
	</PRE> <HR WIDTH="100%"> 

	<P>	The form of a log record that is a compensation operation is
	<PRE>
	@.formatId	no formatId, format is implied by the log file format and the
	log record content.
	@.purpose	undo a previous log record
	@.upgrade
	@.diskLayout
		Log Record that contains the compensation operation
			(see com.neocoretechs.arieslogger.core.LogRecord)
		undoInstance(long) the log instance of the operation that is to be rolled back
			The undo instance is logically part of the LogRecord but is written
			by the logger because it is used and controlled by the rollback
			code but not by the log operation.
		There is no optional data in a compensation operation, all data
		necessary for the rollback must be stored in the operation being
		undone.
	@.endFormat
	</PRE>

    <BR>

	<P>Multithreading considerations:<BR>
	Logger must be MT-safe.	Each RawTransaction has its own private
	FileLogger object. Each logger has a logOutputBuffer and a log input
	buffer which are used to read and write to the log.  Since multiple
	threads can be in the same transaction, fileLogger must be synchronized.

	@see LogRecord
*/

public class FileLogger implements Logger {

	private static final boolean DEBUG = false;

	private LogRecord		 logRecord;

	private LogToFile logToFile;	// actually writes the log records.
	
	private ByteBuffer logOutputBuffer;
	private ByteBuffer logRecordRead;

	/**
		Make a new Logger with its own log record buffers
		MT - not needed for constructor
	 * @throws IOException 
	*/
	public FileLogger(LogToFile logFactory) throws IOException {

		this.logToFile = logFactory;
		this.logOutputBuffer = ByteBuffer.allocate(LogToFile.DEFAULT_LOG_BUFFER_SIZE); // init size
		this.logRecordRead = ByteBuffer.allocate(LogToFile.DEFAULT_LOG_BUFFER_SIZE);
 
		this.logRecord = new LogRecord();
	}

	/**
		Close the logger.
		MT - caller provide synchronization
		(RESOLVE: not called by anyone ??)
	*/
	public void reset() throws IOException
	{
		logOutputBuffer.clear();
		logRecord.reset();
	}

	/*
	** Methods of Logger
	*/

	/**
		Writes out a log record to the log stream, and call its applyChange method to
		apply the change to the rawStore.
		<BR>Any optional data the applyChange method needs is first written to the log
		stream using operation.writeOptionalData, then whatever is written to
		the log stream is passed back to the operation for the applyChange method.

		<P>MT - there could be multiple threads running in the same raw
		transactions and they can be calling the same logger to log different
		log operations.  This whole method is synchronized to make sure log
		records are logged one at a time.

		@param xact the transaction logging the change
		@param operation the log operation
		@return the instance in the log that can be used to identify the log
		record

		@exception StandardException  Standard error policy
	*/
	public synchronized LogInstance logAndDo(BlockDBIO xact, Loggable operation) throws IOException 
	{
		boolean isLogPrepared = false;
		boolean inUserCode = false;
		
		LogInstance logInstance = null;
		try {		

			logOutputBuffer.clear();
			long transactionId = xact.getTransId();

			// always use the short Id, only the BeginXact log record contains
			// the XactId (long form)
			//TransactionId transactionId = xact.getId();

			// write out the log header with the operation embedded
			// this is by definition not a compensation log record,
			// those are called thru the logAndUndo interface
			logRecord.setValue(transactionId, operation);

			inUserCode = true;
			int optionalDataLength = 0;
			int optionalDataOffset = 0;
			int completeLength = 0;
			
			byte[] buf = GlobalDBIO.getObjectAsBytes(logRecord);
			if( DEBUG ) {
				System.out.println("FileLogger.logAndDo: Log record byte array size:"+buf.length);
			}
			byte[] preparedLogArray = operation.getPreparedLog();
			if( preparedLogArray != null ) {
				optionalDataLength = preparedLogArray.length;
			}
			if(logOutputBuffer.remaining() < buf.length+optionalDataLength+4 ) {
				if(DEBUG)
				System.out.println("Not enough space in buffer for record:"+logOutputBuffer.remaining()+" reallocating to "+(buf.length+optionalDataLength+4));
				logOutputBuffer = ByteBuffer.allocate(buf.length+optionalDataLength+4);
			}
			logOutputBuffer.put(buf);
			inUserCode = false;

			//byte[] preparedLogArray = operation.getPreparedLog();
			if (preparedLogArray != null) {

				// There is a race condition if the operation is a begin tran in
				// that between the time the beginXact log record is written to
				// disk and the time the transaction object is updated in the
				// beginXact.applyChange method, other log records may be written.
				// This will render the transaction table in an inconsistent state
				// since it may think a later transaction is the earliest
				// transaction or it may think that there is no active transactions
				// where there is a bunch of them sitting on the log.
				//
				// Similarly, there is a race condition for endXact, i.e.,
				// 1) endXact is written to the log, 
				// 2) checkpoint gets that (committed) transaction as the
				//		firstUpdateTransaction
				// 3) the transaction calls postComplete, nulling out itself
				// 4) checkpoint tries to access a closed transaction object
				//
				// The solution is to sync between the time a begin tran or end
				// tran log record is sent to the log stream and its applyChange method is
				// called to update the transaction table and in memory state
				//
				// We only need to serialized the begin and end Xact log records
				// because once a transaction has been started and in the
				// transaction table, its order and transaction state does not
				// change.
				//
				// Use the logToFile as the sync object so that a checkpoint can
				// take its snap shot of the undoLWM before or after a transaction
				// is started, but not in the middle. (see LogToFile.checkpoint)
				//

				// now set the input limit to be the optional data.  
				// This limits amount of data available to logIn that applyChange can
				// use
				logOutputBuffer.put(preparedLogArray);
				//logIn.setPosition(optionalDataOffset);
				//logIn.setLimit(optionalDataLength);

			} else {
				preparedLogArray = null;
				optionalDataLength = 0;
			}

			logOutputBuffer.putInt(optionalDataLength);
			completeLength = logOutputBuffer.position() + 1 + optionalDataLength;
	
			long instance = 0;
			instance = logToFile.appendLogRecord(logOutputBuffer.array(), 0,
									completeLength, preparedLogArray,
									optionalDataOffset,
									optionalDataLength); 
			logInstance = new LogCounter(instance);
			logToFile.flush();
			operation.applyChange(xact, logInstance, logOutputBuffer);

			if (DEBUG) {	    
                System.out.println("FileLogger.logAndDo: Write log record: tranId=" + transactionId +
                    " instance: " + logInstance.toString() + " length: " +
                    completeLength + " op:" + operation);    
			}

		} finally {
				if( logOutputBuffer != null ) logOutputBuffer.clear();
		}
		return logInstance;

	}

	/**
		Writes out a compensation log record to the log stream, and call its
		applyChange method to undo the change of a previous log operation.

		<P>MT - Not needed. A transaction must be single threaded thru undo, each
		RawTransaction has its own logger, therefore no need to synchronize.
		The RawTransaction must handle synchronizing with multiple threads
		during rollback.

		@param xact the transaction logging the change
		@param compensation the compensation log operation
		@param undoInstance the log instance of the operation that is to be
		rolled back
		@param in optional data input for the compensation applyChange method

		@return the instance in the log that can be used to identify the log
		record

		@exception StandardException  Standard error policy
	 */
	public LogInstance logAndUndo(BlockDBIO xact, 
								 Compensation compensation,
								 LogInstance undoInstance,
								 Object in) throws IOException {

			logOutputBuffer.clear();
			long transactionId = xact.getTransId();
			// write out the log header with the operation embedded
			CompensationLogRecord clr = new CompensationLogRecord(transactionId, compensation, undoInstance);
			logOutputBuffer.put(GlobalDBIO.getObjectAsBytes(clr));
			// in this implementation, there is no optional data for the
			// compensation operation.  Optional data for the rollback comes
			// from the undoable operation - and is passed into this call.
			int completeLength = logOutputBuffer.position();
			long instance =  logToFile.appendLogRecord(logOutputBuffer.array(), 0, completeLength, null, 0, 0);
			LogInstance logInstance = new LogCounter(instance);
			if (DEBUG)
			{
					System.out.println("FileLogger.logAndUndo: Write CLR: Xact: " + transactionId +" clrInstance: " + logInstance.toString() + 
                        " undoInstance " + undoInstance + "\n");
                
			}
			// in and dataLength contains optional data that was written 
			// to the log during a previous call to logAndDo.
			compensation.applyChange(xact, logInstance, in);
			return logInstance;
	}

	/**
		Flush the log up to the given log instance. Calls logToFile(LogToFIle).flush()
		<P>MT - not needed, wrapper method
		@exception StandardException cannot sync log file
	*/
	public void flush() throws IOException {
		if (DEBUG){
                System.out.println("FileLogger.flush: Flush log to:" + logToFile);  
		}
		logToFile.flush();
	}

  
	/**
		Undo a part of or the entire transaction.  Begin rolling back the log
		record at undoStartAt and stopping at (inclusive) the log record at
		undoStopAt.

		<P>MT - Not needed. A transaction must be single threaded thru undo, 
        each RawTransaction has its own logger, therefore no need to 
        synchronize.  The RawTransaction must handle synchronizing with 
        multiple threads during rollback.

		@param t 			the IO controller
		@param undoStopAt	the last log record that should be rolled back
		@param undoStartAt	the first log record that should be rolled back

		@exception StandardException	Standard  error policy

		@see Logger#undo
	  */
	public void undo(BlockDBIO t, LogInstance undoStopAt, LogInstance undoStartAt) throws IOException {
		if (DEBUG)
        {
			System.out.println("Undo transaction: " + t.getTransId());
                if (undoStartAt != null)
                {
                    System.out.println("Undo transaction: " + t.getTransId() + 
                        "start at " + undoStartAt.toString() + 
                        " stop at " + undoStopAt.toString() );
                }
                else
                {
                    System.out.println("Undo transaction: " + t.getTransId() + 
                        " start at end of log, stop at " + undoStopAt.toString());
                }
        }

		// statistics
		int clrgenerated  = 0;
		int clrskipped    = 0;
		int logrecordseen = 0;

		StreamLogScan scanLog;
		Compensation  compensation = null;
		Undoable      lop          = null;

		// buffer to read the log record - initial size 4096, scanLog needs
		// to resize if the log record is larger than that.
		ByteBuffer rawInput = ByteBuffer.allocate(4096);

		try
		{
			if (undoStartAt == null)	
            {
                // don't know where to start, rollback from end of log
				scanLog = (StreamLogScan)logToFile.openBackwardsScan(undoStopAt);
            }
			else
			{
				if (undoStartAt.lessThan(undoStopAt))
                {
                    // nothing to undo!
					return;
                }

				long undoStartInstance = 
                    ((LogCounter) undoStartAt).getValueAsLong();

				scanLog = (StreamLogScan)
					logToFile.openBackwardsScan(undoStartInstance, undoStopAt);
			}

			if (DEBUG)
				assert(scanLog != null) : "cannot open log for undo";

			HashMap<LogInstance, LogRecord> records;
			// backward scan records in reverse order
			while ((records =  scanLog.getNextRecord(rawInput, t.getTransId(), 0)) != null) 
			{
				Iterator<Entry<LogInstance, LogRecord>> irecs = records.entrySet().iterator();
				while(irecs.hasNext()) {
					Entry<LogInstance, LogRecord> recEntry = irecs.next();
					LogRecord record = recEntry.getValue();
					if (DEBUG) {
						assert(record.getTransactionId() == t.getTransId()) : "getNextRecord return unqualified log record for undo";
					}
					logrecordseen++;
					if (record.isCLR()) {
						clrskipped++;
						// cast to CompensationLogRecord so we can get undo instance
						LogInstance undoInstance = ((CompensationLogRecord)record).getUndoInstance();
						if (DEBUG) {
							System.out.println("FileLogger.undo: Skipping over CLRs, reset scan to " + undoInstance);
						}
						scanLog.resetPosition(new LogCounter(undoInstance.getValueAsLong()));
						// scanLog now positioned at the beginning of the log
						// record that was rolled back by this CLR.
						// The scan is a backward one so getNextRecord will skip
						// over the record that was rolled back and go to the one
						// previous to it
						continue;
					}

					lop = record.getUndoable();

					if (lop != null) {
						compensation = lop.generateUndo(t, rawInput);
						if (DEBUG) {
							System.out.println("FileLogger.undo: Rollback log record at instance " +
                                LogCounter.toDebugString(scanLog.getLogInstanceAsLong()) + " : " + lop);
						}

						clrgenerated++;

						if (compensation != null) {
						// generateUndo may have read stuff off the
						// stream, reset it for the undo operation.
						//rawInput.setLimit(savePosition, optionalDataLength);

						// log the compensation op that rolls back the 
                        // operation at this instance 
							logAndUndo(t, compensation, new LogCounter(scanLog.getLogInstanceAsLong()), rawInput);
							compensation.releaseResource(t);
							compensation = null;
						}

						// if compensation is null, log operation is redo only
					}
					// if this is not an undoable operation, continue with next log
					// record
				} // record iterator
			}
		}
		catch (ClassNotFoundException cnfe)
		{
			throw logToFile.markCorrupt( new IOException(cnfe));
		}
	    catch (IOException ioe) 
		{
			throw logToFile.markCorrupt(ioe);
		}
		finally
		{
			if (compensation != null) 
            {
                // errored out
				compensation.releaseResource(t);
            }

			if (rawInput != null)
			{
				rawInput.clear();
			}
		}

		if (DEBUG)
        {
			System.out.println("FileLogger.undo: Finish undo, clr generated = " + clrgenerated +
                        ", clr skipped = " + clrskipped + ", record seen = " + logrecordseen + "\n");
        }
	}


	/**
		Recovery Redo loop.

		<P> The log stream is scanned from the beginning (or
		from the undo low water mark of a checkpoint) forward until the end.
		The purpose of the redo pass is to repeat history, i.e, to repeat
		exactly the same set of changes the rawStore went thru right before it
		stopped.   With each log record that is encountered in the redo pass:
		<OL>
		<LI>if it isFirst(), then the transaction factory is called upon to
		    create a new transaction object.
		<LI>if it needsRedo(), its applyChange() is called (if it is a compensation
		    operation, then the undoable operation needs to be created first
            before the applyChange is called).
		<LI>if it isComplete(), then the transaction object is closed.
		</OL>

		<P> MT - caller provides synchronization

		@param redoLWM          - if checkpoint seen, starting from this point
                                  on, apply redo if necessary

		@return the log instance of the next log record (or the instance just
		after the last log record).  This is used to determine where the log
		truly ends

		@exception StandardException Standard  error policy
		@exception IOException error reading log file
		@exception ClassNotFoundException log file corrupted

		@see LogToFile#recover
	 */
	protected long redo(BlockDBIO blockio, StreamLogScan redoScan, long redoLWM, long ttabInstance)
		 throws IOException, ClassNotFoundException {

		int scanCount    = 0;
        int redoCount    = 0;
        int prepareCount = 0; 
        int clrCount     = 0;
        int btranCount   = 0;
        int etranCount   = 0;

		// end debug info

		long tranId = -1;

        // the current log instance
		long instance = LogCounter.INVALID_LOG_INSTANCE;

		// use this scan to reconstitute operation to be undone
		// when we see a CLR in the redo scan
		StreamLogScan undoScan  = null;
		Loggable      op        = null;
		long          logEnd    = 0;  // we need to determine the log's true end

		try 
        {
			if( DEBUG ) {
				System.out.println("FileLogger.redo entering redo scan with redoLWM:"+LogCounter.toDebugString(redoLWM));
			}
			// scan the log forward in redo pass and go to the end
			HashMap<LogInstance, LogRecord> records;
			// backward scan records in reverse order
			while ((records = redoScan.getNextRecord(logOutputBuffer, -1, 0))  != null) 
			{
				Iterator<Entry<LogInstance, LogRecord>> irecs = records.entrySet().iterator();
				while(irecs.hasNext()) {
					Entry<LogInstance, LogRecord> recEntry = irecs.next();
					LogRecord record = recEntry.getValue();
					scanCount++;
					long undoInstance = 0;

					// last known good instance
					instance = recEntry.getKey().getValueAsLong();

					if (DEBUG) {
                        op = record.getLoggable();
                        tranId = record.getTransactionId();
                        if (record.isCLR())	 {
                            System.out.println(
                                "FileLogger.redo scanned " + tranId + " : " + op + 
                                " instance = " + 
                                    LogCounter.toDebugString(instance)); //+ 
                                //" undoInstance : " + 
                                    //LogCounter.toDebugString(undoInstance));
                        } else {
                            System.out.println(
                                "FileLogger.redo scanned " + tranId + " : " + op + 
                                " instance = " + 
                                    LogCounter.toDebugString(instance)
                                + " logEnd = " + 
                                    LogCounter.toDebugString(logEnd) 
                                + " available " + logOutputBuffer.remaining());
                        }      
					}

					// if the redo scan is between the undoLWM and redoLWM, we only
					// need to redo begin and end tran.  Everything else has
					// already been flushed by checkpoint.
					// We dont write the dirty pages list within a Checkpoint record. Instead, during checkpoint, 
					// we flush all database pages to disk. The redo Low Water Mark (redoLWM) is set to the current instance
					// when the checkpoint starts. The undo Low Water Mark (undoLWM) is set to the starting instance
					// of the oldest active transaction. At restart, 
					// replay the log from redoLWM or undoLWM whichever is earlier. 
					if (redoLWM != LogCounter.INVALID_LOG_INSTANCE && instance < redoLWM) {
						if ( record.isComplete() || record.isPrepare() ) {
							if( DEBUG ) {
								System.out.println("FileLogger.redo: continuing redo log loop with ineligable record "+record +" with instance "+LogCounter.toDebugString(instance));
							}
							continue;
						}
					}

					btranCount++;
					// the long transaction ID is embedded in the beginXact log
					// record.  The short ID is stored in the log record.
					//long recoveryTransaction =  record.getTransactionId();
				
					op = record.getLoggable();

					if (op.needsRedo(blockio)) {
						redoCount++;
						if (record.isCLR())	 {
							clrCount++;
							// the log operation is not complete, the operation to
							// undo is stashed away at the undoInstance.
							// Reconstitute that first.
							if (DEBUG)
								assert(op instanceof Compensation);
							// cast to CompensationLogRecord so we can get undo instance
							LogInstance undoInst = ((CompensationLogRecord)record).getUndoInstance();
							// undoScan now positioned at the beginning of the log
							// record was rolled back by this CLR.  
							// The scan is a forward one so getNextRecord will get 
							// the log record that needs to be rolled back.
							// reuse the buffer in logOutputBuffer since CLR 
							// has no optional data and has no use for it anymore 
							logOutputBuffer.clear();
							LogRecord undoRecord = LogToFile.getRecord(logToFile, undoInst);
							Undoable undoOp = undoRecord.getUndoable();

							if (DEBUG) {
								System.out.println("FileLogger.redo Redoing CLR: undoInstance = " + LogCounter.toDebugString(undoInstance) +
                                " clrinstance = " +  LogCounter.toDebugString(instance));
								//assert(undoRecord.getTransactionId() == tranId);
								assert(undoOp != null);
							}

							((Compensation)op).setUndoOp(undoOp);
							// call applyChange to roll back the block
							undoOp.applyChange(blockio, undoInst, null);
						}

						// at this point, logIn points to the optional
						// data of the loggable that is to be redone or to be
						// rolled back			
						if (DEBUG) {
                            System.out.println( "FileLogger.redo redoing " + op + " instance = " +  LogCounter.toDebugString(instance));
						}
					} //op.needsRedo

				if (record.isComplete()) {
					etranCount++;
					//recoveryTransaction.commit();
				}
			  } // while irecs.hasNext
			}// while redoScan.getNextRecord() != null
			
            logEnd = redoScan.getLogRecordEnd();
            
		} finally {
			// close all the io streams
			redoScan.close();
			redoScan = null;

			if (undoScan != null)
			{
				undoScan.close();
				undoScan = null;
			}


		}

		if (DEBUG)
        {
			System.out.println(
                    "----------------------------------------------------\n" +
                    "End of recovery redo\n" + 
                    "Scanned = " + scanCount + " log records" +
                    ", redid = " + redoCount +
                    " ( clr = " + clrCount + " )" +
                    " begintran = " + btranCount +
                    " endtran = " + etranCount + 
                    " preparetran = " + prepareCount + 
                    "\n log ends at " + LogCounter.toDebugString(logEnd) +
                    "\n----------------------------------------------------\n");
            
        }

		if (DEBUG)
		{
			// make sure logEnd and instance is consistent
			if (instance != LogCounter.INVALID_LOG_INSTANCE)	
            {
				assert(
                    LogCounter.getLogFileNumber(instance) <
                         LogCounter.getLogFileNumber(logEnd) ||
                    (LogCounter.getLogFileNumber(instance) ==
                         LogCounter.getLogFileNumber(logEnd) &&
                     LogCounter.getLogFilePosition(instance) <=
                         LogCounter.getLogFilePosition(logEnd)));
            }
			else
            {
				assert(logEnd == LogCounter.INVALID_LOG_INSTANCE);
            }
		}

        // logEnd is the last good log record position in the log
		return logEnd;			
	}


	@Override
	public void reprepare(long t, long undoId, LogInstance undoStopAt,LogInstance undoStartAt) throws IOException {
		// TODO Auto-generated method stub
		
	}


}
