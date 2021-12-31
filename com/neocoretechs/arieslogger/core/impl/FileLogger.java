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
import com.neocoretechs.arieslogger.core.StreamLogScan;
import com.neocoretechs.arieslogger.logrecords.Compensation;
import com.neocoretechs.arieslogger.logrecords.Loggable;
import com.neocoretechs.arieslogger.logrecords.Undoable;
import com.neocoretechs.bigsack.io.UndoableBlock;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.Datablock;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.pooled.MappedBlockBuffer;
import com.neocoretechs.bigsack.io.stream.DBOutputStream;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
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
	FileLogger object. Each logger has a logOutputBuffer, bytebuffers, a LogToFIle
	file manager and other class fields which are used to read and write to the log.  Since multiple
	threads can be in the same transaction, fileLogger methods must be synchronized.

	@see LogRecord
*/

public final class FileLogger implements Logger {
	private static final boolean DEBUG = false;
	private static final boolean DEBUGUNDO = false;
	private static final boolean DEBUGLOGANDDO = false;
	private LogRecord	logRecord;

	private LogToFile logToFile;	// actually writes the log records.
	
	private ByteBuffer logOutputBuffer;

	/**
	* Make a new Logger with its own log record buffers
	* @throws IOException 
	*/
	public FileLogger(LogToFile logFactory) {
		this.logToFile = logFactory;
		this.logOutputBuffer = ByteBuffer.allocate(LogToFile.DEFAULT_LOG_BUFFER_SIZE); // init size
		this.logRecord = new LogRecord();
	}

	/**
		Close the logger.
		MT - synchronized
	*/
	public synchronized void reset() throws IOException
	{
		logOutputBuffer.clear();
		logRecord.reset();
	}


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
		@param operation the log operation {@link UndoableBlock}
		@return the instance in the log that can be used to identify the log
		record

		@exception IOException  Standard error policy
	*/
	public synchronized LogInstance logAndDo(GlobalDBIO xact, Loggable operation) throws IOException 
	{
		LogInstance logInstance = null;
		try {		

			logOutputBuffer.clear();
			long transactionId = xact.getTransId();

			// write out the log header with the operation embedded
			// this is by definition not a compensation log record,
			// those are called thru the logAndUndo interface
			logRecord.setValue(transactionId, operation);

			int optionalDataLength = 0;
			int optionalDataOffset = 0;
			int completeLength = 0;
			
			byte[] buf = GlobalDBIO.getObjectAsBytes(logRecord);
			if( DEBUG || DEBUGLOGANDDO ) {
				System.out.println("FileLogger.logAndDo: Log record byte array size:"+buf.length);
			}
			byte[] preparedLogArray = operation.getPreparedLog();
			if( preparedLogArray != null ) {
				optionalDataLength = preparedLogArray.length;
			}
			if(logOutputBuffer.remaining() < buf.length+optionalDataLength+4 ) {
				if(DEBUG || DEBUGLOGANDDO)
				System.out.println("Not enough space in buffer for record:"+logOutputBuffer.remaining()+" reallocating to "+(buf.length+optionalDataLength+4));
				logOutputBuffer = ByteBuffer.allocate(buf.length+optionalDataLength+4);
			}
			logOutputBuffer.put(buf);

			//byte[] preparedLogArray = operation.getPreparedLog();
			if (preparedLogArray != null) {
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
			flush();
			logInstance = new LogCounter(instance);
			//((UndoableBlock)operation)
			operation.applyChange(xact, logInstance, logOutputBuffer);
		
			if (DEBUG || DEBUGLOGANDDO) {	    
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
		Once the record is properly written, only then do we activate the 
		persistent store replacement of page
		<P>MT - synchronized method

		@param xact the transaction logging the change
		@param compensation the compensation log operation
		@param undoInstance the log instance of the operation that is to be
		rolled back
		@param in optional data input for the compensation applyChange method

		@return the instance in the log that can be used to identify the log
		record

		@exception IOException  Standard error policy
	 */
	public synchronized /*LogInstance*/ void logAndUndo(GlobalDBIO xact, 
								 Compensation compensation,
								 LogInstance undoInstance,
								 LogRecord lr,
								 Object in) throws IOException {
			if (DEBUG){
				System.out.println("FileLogger.logAndUndo: ENTRY, CLR:"+ compensation + 
                    " undoInstance " + undoInstance + "\n");
			}
			/*
			LogInstance logInstance = null;
			try {		
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
				flush();
				logInstance = new LogCounter(instance);
				if (DEBUG) {
					System.out.println("FileLogger.logAndUndo: about to applyChange, CLR:"+clr+" " + logInstance.toString() + 
                        " undoInstance " + undoInstance + "\n");
                
				}
				// in and dataLength contains optional data that was written 
				// to the log during a previous call to logAndDo.
				compensation.applyChange(xact, logInstance, in);
			} finally {
				if( logOutputBuffer != null ) logOutputBuffer.clear();
			}
			return logInstance;
			*/
			try {
				((Compensation)compensation).setUndoOp(lr.getUndoable());
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
			compensation.applyChange(xact, undoInstance, in);
			
	}

	/**
		Flush the log up to the given log instance. Calls logToFile(LogToFIle).flush()
		<P>MT - synchronized
		@exception IOException cannot sync log file
	*/
	public synchronized void flush() throws IOException {
		if (DEBUG){
                System.out.println("FileLogger.flush: Flush log to:" + logToFile);  
		}
		logToFile.flush();
	}

  
	/**
		Undo a part of or the entire transaction.  Begin rolling back the log
		record at undoStartAt and stopping at (inclusive) the log record at
		undoStopAt.

		<P>MT - synchronized method

		@param t 			the IO controller
		@param undoStopAt	the last log record that should be rolled back
		@param undoStartAt	the first log record that should be rolled back

		@exception IOException

		@see Logger#undo
	  */
	public synchronized void undo(GlobalDBIO t, LogInstance undoStartAt, LogInstance undoStopAt) throws IOException {
		assert(undoStartAt != null) : "FileLogger.undo startAt position cannot be null";
		if(DEBUG || DEBUGUNDO)
        {
            System.out.println("Undo transaction: " +
                        "start at " + undoStartAt != null ? undoStartAt.toString() : "NULL" + 
                        " stop at " + undoStopAt != null ? undoStopAt.toString() : "NULL");
        }

		// statistics
		int clrgenerated  = 0;
		int clrskipped    = 0;
		int logrecordseen = 0;

		StreamLogScan scanLog;
		Compensation  compensation = null;

		try
		{
			if (undoStopAt != null && undoStartAt.lessThan(undoStopAt)){
			    // nothing to undo!
				if(DEBUG || DEBUGUNDO)
					System.out.printf("%s Nothing to undo! start @ %s stsop @ %s%n",this.getClass().getName(), undoStartAt, undoStopAt);
				return;
			}

			long undoStartInstance = 
			    ((LogCounter) undoStartAt).getValueAsLong();

			scanLog = (StreamLogScan)
				logToFile.openBackwardsScan(undoStartInstance, undoStopAt);

			if (DEBUG || DEBUGUNDO)
				assert(scanLog != null) : "cannot open log for undo";

			HashMap<LogInstance, LogRecord> records;
			LogInstance undoInstance = null;
			// backward scan records in reverse order
			while ((records =  scanLog.getNextRecord(0)) != null) 
			{
				Iterator<Entry<LogInstance, LogRecord>> irecs = records.entrySet().iterator();
				while(irecs.hasNext()) {
					Entry<LogInstance, LogRecord> recEntry = irecs.next();
					LogRecord record = recEntry.getValue();
					if(DEBUG || DEBUGUNDO) {
						assert(record.getTransactionId() == t.getTransId()) : "getNextRecord return unqualified log record for undo";
					}
					logrecordseen++;
					if (record.isCLR()) {
						clrskipped++;
						// cast to CompensationLogRecord so we can get undo instance
						undoInstance = ((CompensationLogRecord)record).getUndoInstance();
						if(DEBUG || DEBUGUNDO) {
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
					// extract the undoable from the record and generate the CLR
					if(extractUndoable(t, record, scanLog.getLogInstance()))
						clrgenerated++;
					// if compensation is null, log operation is redo only
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
                // err out
				compensation.releaseResource(t);
            }
		}

		if(DEBUG || DEBUGUNDO) {
			System.out.println("FileLogger.undo: Finish undo, clr generated = " + clrgenerated +
                        ", clr skipped = " + clrskipped + ", record seen = " + logrecordseen + "\n");
        }
	}

	/**
	* Commit the entire transaction. Scan the log and reset the inLog indicator in the persistent store.<p/>
	* If the block still resides in the BufferPool, make sure its inlog flag is reset and its pushed
	* back to deep store, otherwise bring it in from deep store, rest it and push the header back out.
	* <P>MT - synchronized method
	* @param t 	the IO controller
	* @param pool 
	* @exception IOException
	*/
	public synchronized void commit(GlobalDBIO t, MappedBlockBuffer pool) throws IOException {
		if(DEBUG) {
			System.out.println("Commit transaction ");
		}
		StreamLogScan scanLog;
		Compensation  compensation = null;
		try {
			scanLog = (StreamLogScan)logToFile.openCheckedForwardScan(LogInstance.INVALID_LOG_INSTANCE, null);
			if(scanLog == null) {
				if(DEBUG) {
					System.out.println("FileLogger.commit: Skipping commit");
				}
				return;
			}
			HashMap<LogInstance, LogRecord> records;
			Datablock dblk = new Datablock();
			// scan records
			while ((records =  scanLog.getNextRecord(0)) != null) {
				Iterator<Entry<LogInstance, LogRecord>> irecs = records.entrySet().iterator();
				while(irecs.hasNext()) {
					Entry<LogInstance, LogRecord> recEntry = irecs.next();
					LogRecord record = recEntry.getValue();
					Loggable loggable = record.getLoggable();
					if(loggable instanceof CheckpointOperation)
						continue;
					UndoableBlock ub = (UndoableBlock)loggable;
					BlockAccessIndex lbai = ub.getBlkV2();
					if(pool.containsKey(lbai.getBlockNum())) {
						BlockAccessIndex bai = (BlockAccessIndex) ((SoftReference)(pool.get(lbai.getBlockNum()))).get();
						bai.getBlk().setInlog(false);
					}
					long tblock = lbai.getBlockNum();
					DBOutputStream dbo = GlobalDBIO.getBlockOutputStream(lbai);
					t.updateDeepStoreInLog(dbo,tblock, false);
					dbo.close();
					if(DEBUG)
						System.out.printf("%s.commit Reset inLog for block:%s%n",this.getClass().getName(),GlobalDBIO.valueOf(lbai.getBlockNum()));
				} // record iterator
			}
		} catch (ClassNotFoundException cnfe) {
			throw logToFile.markCorrupt( new IOException(cnfe));
		} catch (IOException ioe)  {
			throw logToFile.markCorrupt(ioe);
		}
		finally {
			if (compensation != null)  {
            // err out
			compensation.releaseResource(t);
			}
		}
		if(DEBUG) {
			System.out.println("FileLogger.commit: Finish commit");
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
	  	@param ttabInstance 	- checkpoint instance from control file, startng point

		@return the log instance of the next log record (or the instance just
		after the last log record).  This is used to determine where the log
		truly ends

		@exception IOException error reading log file
		@exception ClassNotFoundException log file corrupted

		@see LogToFile#recover
	 */
	protected synchronized long redo(GlobalDBIO blockio, 
			StreamLogScan redoScan, 
			long redoLWM, 
			long ttabInstance) throws IOException, ClassNotFoundException {

		int scanCount    = 0;
        int redoCount    = 0;
        int clrCount     = 0;
        int btranCount   = 0;
        int etranCount   = 0;

		// end debug info

        // the current log instance
		long instance = LogCounter.INVALID_LOG_INSTANCE;

		StreamLogScan undoScan  = null;
		Loggable      op        = null;
		long          logEnd    = LogCounter.getLogFilePosition(ttabInstance);  // we need to determine the log's true end

		try 
        {
			if( DEBUG ) {
				System.out.println("FileLogger.redo entering redo scan with redoLWM:"+LogCounter.toDebugString(redoLWM));
			}
			// scan the log forward in redo pass and go to the end
			HashMap<LogInstance, LogRecord> records;
			while ((records = redoScan.getNextRecord(0))  != null) 
			{
				Iterator<Entry<LogInstance, LogRecord>> irecs = records.entrySet().iterator();
				//if( DEBUG )
				//	System.out.println("FileLogger.redo scanning returned log records:"+records.size());
				while(irecs.hasNext()) {
					Entry<LogInstance, LogRecord> recEntry = irecs.next();
					LogRecord record = recEntry.getValue();
					scanCount++;
					// last known good instance
					instance = recEntry.getKey().getValueAsLong();
					// if the redo scan is between the undoLWM and redoLWM, we only
					// need to redo begin and end tran.  Everything else has
					// already been flushed by checkpoint.
					// We dont write the dirty pages list within a Checkpoint record. Instead, during checkpoint, 
					// we flush all database pages to disk. The redo Low Water Mark (redoLWM) is set to the current instance
					// of the instance before checkpoint starts. 
					// replay the log from redoLWM 
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
					//if( DEBUG ) {
					//	System.out.println("FileLogger.redo got loggable "+op);
					//}
					if (op.needsRedo(blockio)) {
						//if( DEBUG ) {
						//	System.out.println("FileLogger.redo get loggable needing redo "+op);
						//}
						redoCount++;
						if (record.isCLR())	 {
							//if( DEBUG ) {
							//	System.out.println("FileLogger.redo get loggable needing redo that is CLR:"+op);
							//}
							clrCount++;
							// the log operation is not complete, the operation to
							// undo is stashed away at the undoInstance.
							// Reconstitute that first.
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
								System.out.println("FileLogger.redo Redoing CLR: undoInstance = " + undoInst +
                                " CLR instance = " +  LogCounter.toDebugString(instance));
							}

							((Compensation)op).setUndoOp(undoOp);
							// call applyChange to roll back the block
							undoOp.applyChange(blockio, undoInst, null);
							if (DEBUG) {
	                            System.out.println( "FileLogger.redo redoing CLR " + op + " instance = " +  LogCounter.toDebugString(instance));
							}
							continue;
						} // CLR processed
						// at this point, logIn points to the optional
						// data of the loggable that is to be redone or to be
						// rolled back			
						//if (DEBUG) {
                        //    System.out.println( "FileLogger.redo redoing " + op + " instance = " +  LogCounter.toDebugString(instance));
						//}
						// add the value to the array to return, its an undoable that needs undoing
						//undoInstances.add(instance);
					} //op.needsRedo
				if (record.isComplete()) {
					etranCount++;
				}
			  } // while irecs.hasNext
			}// while redoScan.getNextRecord() != null
			
            logEnd = ((Scan)redoScan).getScannedEndInstance();
            if( logEnd == LogCounter.INVALID_LOG_INSTANCE) {
            	if( instance == LogCounter.INVALID_LOG_INSTANCE ) {
            		//hmm, never really got anything, use the checkpoint
            		logEnd = LogCounter.getLogFilePosition(ttabInstance);
            	} else {
            		// instance good, logend bad
            		logEnd = LogCounter.getLogFilePosition(instance)-4;
            	}
            	// good as it gets
            }
            
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
		if( LogToFile.ALERT )
			System.out.println(
                    "----------------------------------------------------\n" +
                    "End of recovery redo for "+blockio.getDBName()+"\n" + 
                    "Scanned = " + scanCount + " log records" +
                    ", redid = " + redoCount +
                    " ( compensation = " + clrCount + " )" +
                    " incomplete/prepared = " + btranCount +
                    " complete = " + etranCount + 
                    "\n log ends at " + LogCounter.toDebugString(logEnd) +
                    "\n----------------------------------------------------\n");
            
        //}

        // logEnd is the last good log record position in the log
		return logEnd;			
	}
	/**
	 * Extract the undoable from the log record, generate the compensation record
	 * via 'generateUndo', and finally call 'logAndUndo' to apply the undo and write the CLR and
	 * its checksum.
	 * @param t The Block IO manager
	 * @param rawInput The running bytebuffer, optionally modified according to user. passed to generateUndo and logAndUndo
	 * @param record, the LogRecord returned from scan
	 * @param undoInstance The instance to undo
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @return true if success, false if record.getundoable returns null
	 */
	public synchronized boolean extractUndoable(GlobalDBIO t, 
			LogRecord record, 
			LogInstance undoInstance) throws IOException, ClassNotFoundException {
		Undoable lop = record.getUndoable();
		Compensation compensation;
		if (lop != null) {
			compensation = lop.generateUndo(t);
			if (DEBUG) {
				System.out.println("FileLogger.extractUndoable processing logRecord " +record);
			}
			if (compensation != null) {
			// log the compensation op that rolls back the 
            // operation at this instance 
				logAndUndo(t, compensation, undoInstance, record, null);
				compensation.releaseResource(t);
				return true;
			}
			// if compensation is null, log operation is redo only
		}
		return false;
	}
	
	@Override
	public void reprepare(long t, long undoId, LogInstance undoStopAt,LogInstance undoStartAt) throws IOException {
		throw new IOException("reprepare unimplemented");
	}

	@Override
	public LogInstance logAndUndo(GlobalDBIO xact, Compensation operation,
			LogInstance undoInstance, Object in) throws IOException {
		throw new IOException("Log write of CLR unimplemented, use alternate logAndUndo method");
	}


}
