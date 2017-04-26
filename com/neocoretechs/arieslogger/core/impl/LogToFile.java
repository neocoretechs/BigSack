/*

    - Class com.neocoretechs.arieslogger.core.LogToFile

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

import com.neocoretechs.arieslogger.core.LogFactory;
import com.neocoretechs.arieslogger.core.LogInstance;
import com.neocoretechs.arieslogger.core.LogScan;
import com.neocoretechs.arieslogger.core.Logger;
import com.neocoretechs.arieslogger.core.StreamLogScan;
import com.neocoretechs.arieslogger.logrecords.Loggable;
import com.neocoretechs.arieslogger.logrecords.ScanHandle;
import com.neocoretechs.bigsack.io.pooled.ObjectDBIO;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;

import java.io.File; // Plain files are used for backups
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.SyncFailedException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.zip.CRC32;

/**

	This is an implementation of the log using a non-circular file system file.
	No support for incremental log backup or media recovery.
	Only crash recovery is supported.  
	<P>
	The 'log' is a stream of log records.  The 'log' is implemented as
	a series of numbered log files.  These numbered log files are logically
	continuous so a transaction can have log records that span multiple log files.
	A single log record cannot span more than one log file.  The log file number
	is monotonically increasing.
	<P>
	The log belongs to a log factory of a RawStore.  In the current implementation,
	each RawStore only has one log factory, so each RawStore only has one log PER TABLESPACE 
	(which composed of multiple log files).
	At any given time, a log factory only writes new log records to one log file,
	this log file is called the 'current log file'.
	Every time a checkpoint is taken, a new log file is created and all subsequent
	log records will go to the new log file.  After a checkpoint is taken, old
	and useless log files will be deleted.
	<P>
	The API exposes a checkpoint method which clients can call
	<P>
	This LogFactory is responsible for the formats of 2 kinds of file: the log
	file and the log control file.  And it is responsible for the format of the
	log record wrapper.
	<P> <PRE>

	Format of log control file 

	@.formatId	FILE_STREAM_LOG_FILE
	@.purpose	The log control file contains information about which log files
	are present and where the last checkpoint log record is located.
	@.upgrade	
	@.diskLayout
		int format id
		int obsolete log file version
		long the log instance (LogCounter) of the last completed checkpoint
		int version
		int checkpoint interval
		long spare (value set to 0)
		long spare (value set to 0)
		long spare (value set to 0)

	@.endFormat
	</PRE>	
	<HR WIDTH="100%">
	<PRE>

	Format of the log file

	@.formatId	FILE_STREAM_LOG_FILE
	@.purpose	The log file contains log record which record all the changes
	to the database.  The complete transaction log is composed of a series of
	log files.
	@.upgrade
	@.diskLayout
		int format id - 	the format Id of this log file
		long log file number - this number orders the log files in a
						series to form the complete transaction log
		long prevLogRecord - log instance of the previous log record, in the
				previous log file. 

		[log record wrapper]* one or more log records with wrapper

		int endMarker - value of zero.  The beginning of a log record wrapper
				is the length of the log record, therefore it is never zero
		[int fuzzy end]* zero or more int's of value 0, in case this log file
				has been recovered and any incomplete log record set to zero.
	@.endFormat
	</PRE>	
	<HR WIDTH="100%">
	<PRE>

	Format of the log record wrapper

	@formatId none.  The format is implied by the FILE_STREAM_LOG_FILE
	@purpose	The log record wrapper provides information for the log scan.
	@.upgrade
	@.diskLayout
		length(int) length of the log record (for forward scan)
		instance(long) LogInstance of the log record
		logRecord(byte[length]) byte array that is written by the FileLogger
		length(int) length of the log record (for backward scan)
	@.endFormat
	</PRE>

	<P>Multithreading considerations:<BR>
	Log Factory must be MT-safe.
	<P>
	Class is final as it has methods with privilege blocks and implements PrivilegedExceptionAction.
	*/

public final class LogToFile implements LogFactory, java.security.PrivilegedExceptionAction
{
	private static final boolean DEBUG = false;
	private static final boolean DUMPLOG = false;
	private static final boolean MEASURE = false; // take stats of log writes etc
	static final boolean ALERT = false; // return status for recovery, also for FileLogger recover
	public static final String DBG_FLAG = DEBUG ? "LogTrace" : null;
	public static final String DUMP_LOG_ONLY = DEBUG ? "DumpLogOnly" : null;
	public static final String DUMP_LOG_FROM_LOG_FILE = DEBUG ? "bigsack.logDumpStart" : null;
	
	private static final int FILE_STREAM_LOG_FILE = 0;
	private static int fid = 1; 
	/**
		Return format identifier. format Id must fit in 4 bytes
	*/
	public int getTypeFormatId() {
		return FILE_STREAM_LOG_FILE;
	}

	// at the beginning of every log file is the following information:
	// the log file formatId
	// the log file version (int)
	// the log file number (long)
	// the log instance at the end of the last log record in the previous file (long)
	public static final int LOG_FILE_HEADER_SIZE = 24;
	// this is the offset used to increment files during scan. 
	protected static final int LOG_FILE_HEADER_PREVIOUS_LOG_INSTANCE_OFFSET = LOG_FILE_HEADER_SIZE-8;
	// Number of bytes overhead of each log record.
	// 4 bytes of length at the beginning, 8 bytes of log instance,4 bytes ending length for backwards scan

	public static final int LOG_RECORD_OVERHEAD = 16;

	protected static final String LOG_SYNC_STATISTICS = "LogSyncStatistics";

	/* how big the log file should be before checkpoint or log switch is taken */
	private static final int DEFAULT_LOG_SWITCH_INTERVAL = 10000000;		

	//log buffer size values
	public static final int DEFAULT_LOG_BUFFER_SIZE = 32768; //32K
	int logBufferSize = DEFAULT_LOG_BUFFER_SIZE;

	/* Log Control file flags. */
	private static final byte IS_BETA_FLAG = 0x1;

	private int     logSwitchInterval   = DEFAULT_LOG_SWITCH_INTERVAL;
    
	private FileLogger fileLogger = null;
	protected LogAccessFile logOut;		// an output stream to the log file
								// (access of the variable should sync on this)
	//private RandomAccessFile theLog = null;
	protected long		    endPosition = LOG_FILE_HEADER_SIZE; // end position of the current log file
	private long			lastFlush = 0;	// the position in the current log
											// file that has been flushed to disk

    // logFileNumber and bootTimeLogFileNumber:
    // ----------------------------------------
    // There are three usages of the log file number in this class:
    //
    //  1 Recover uses the log file number determined at boot-time to
    //    find which log file the redo pass should start to read.
    //  2 If the database is booted in slave replication mode, a slave
    //    thread will apply log records to the tail of the log.
    //    switchLogFile() allocates new log files when the current log
    //    file is full. logFileNumber needs to point to the log file
    //    with the highest number for switchLogFile() to work
    //    correctly.
    //  3 After the database has been fully booted, i.e. after boot()
    //    and recover(), and users are allowed to connect to the
    //    database, logFileNumber is used by switchLogFile() to
    //    allocate new log files when the current is full.
    //
    // Usage 2 and 3 are very similar. The only difference is that 1
    // and 2 are performed concurrently, whereas 3 is performed afterwards.
    // Because usage 1 and 2 are required in concurrent threads (when
    // booted in slave replication mode) that must not interfere, two
    // versions of the log file number are required:
    //
    // bootTimeLogFileNumber: Set to point to the log file with the
    //   latest checkpoint during boot(). This is done before the
    //   slave replication thread has started to apply log records
    //   received from the master. Used by recover() during the time
    //   interval in which slave replication mode may be active, i.e.
    //   during the redo pass. After the redo pass has completed,
    //   recover starts using logFileNumber (which will point to the
    //   highest log file when recovery has completed). 
    // logFileNumber: Used by recovery after the redo pass, and by
    //   switchLogFile (both in slave replication mode and after the
    //   database has been fully booted) when a new log file is
    //   allocated.
	long logFileNumber = -1; // current log file number.
								// Other than during boot and recovery time,
								// and during initializeReplicationSlaveRole if in
								// slave replication mode,
								// logFileNumber is only changed by
								// switchLogFile, which is synchronized.
								// 
								// MT - Anyone accessing this number should
								// synchronized on this if the current log file
								// must not be changed. If not synchronized,
								// the log file may have been switched.

    // Initially set to point to the log file with the latest
    // checkpoint (in boot()). Only used by recovery() after that
    long  bootTimeLogFileNumber = -1;

	private long firstLogFileNumber = -1;
								// first log file that makes up the active
								// portion (with active transactions) of the
								// log.  
								// 
								// MT - This value is set during recovery or
								// during log truncation.  In the former single
								// thread is assumed.  In the latter
								// must be synchronized with this to access
								// or change.
	
	private long maxLogFileNumber = LogCounter.MAX_LOGFILE_NUMBER;
	private CheckpointOperation currentCheckpoint;
								// last checkpoint successfully taken
								// 
								// MT - only changed or access in recovery or
								// checkpoint, both are single thread access

	long checkpointInstance;	// log instance of the current Checkpoint
	
	private long previousLogInstance = LogCounter.makeLogInstanceAsLong(1,LogToFile.LOG_FILE_HEADER_SIZE);
								// previous instance logged for writing header of new files for backward scan
								// and verification
	private long headerLogInstance = LogCounter.INVALID_LOG_INSTANCE;

	//protected boolean	ReadOnlyDB;	// true if this db is read only, i.e, cannotappend log records

	// DEBUG DEBUG - do not truncate log files
	private boolean keepAllLogs;

	// the following booleans are used to put the log factory into various
	// states
	private boolean			 recoveryNeeded = true; // log needs to be recovered
	private boolean			 inCheckpoint = false; 	// in the middle of a checkpoint
	private boolean			 inRedo = false;        // in the middle of redo loop

	/** DEBUG test only */
	int test_logWritten = 0;
	int test_numRecordToFillLog = -1;
	private int mon_flushCalls;
	private int mon_syncCalls;
	private int mon_numLogFlushWaits;
	private boolean mon_LogSyncStatistics;
	private int mon_numBytesToLog;

	/**
		If not null then something is corrupt in the raw store and this represents the original error.
	*/
	protected volatile Exception corrupt;

	/**
		On disk database version information. When running in soft upgrade this version
		may be different to jbmsVersion.
	*/
	private int onDiskMajorVersion;
	private int onDiskMinorVersion;
	private boolean onDiskBeta;
	
	private CRC32 checksum = new CRC32(); // holder for the checksum

	/**
	 * Write sync mechanism support ?
	 * fsync() is used on commits to make log is written to the 
	 * disk. fsync() calls are expensive. 
	 * use write sync on preallocated log file?
	 */

	private volatile boolean logArchived = false;
	private boolean logSwitchRequired = false;
    // log file that is yet to be copied to backup, updates to this variable 
    // needs to visible  checkpoint thread. 
	private volatile long logFileToBackup ; 
    // It is set to true when  online backup is in progress,  updates to 
    // this variable needs to visible to checkpoint thread. 
    private volatile boolean backupInProgress = false;
    
    // Name of the BigSack database to log
    private String dbName;
	private ObjectDBIO blockIO;
	private int tablespace;
   
	/**
	 * Construt the instance with the IO manager and tablespace
	 * @param blockIO
	 * @param tablespace
	 */
	public LogToFile(ObjectDBIO blockIO, int tablespace) {
		this.blockIO = blockIO;
		this.tablespace = tablespace;
		this.dbName = (new File(blockIO.getDBName())).getName();
		keepAllLogs = false; // indicates whether obsolete logs may be removed, true causes return from truncate immediately
		if (MEASURE)
			mon_LogSyncStatistics = true;
	}
	
	public void setRecoveryNeeded() {
		recoveryNeeded = true;
	}

    public String getDBName() {
		return dbName;
	}

    public long getPreviousLogInstance() { return previousLogInstance; }
    public long getHeaderLogInstance() { return headerLogInstance; }
    
	/*
	** Methods of Corruptable
	*/

	/**
     * Once the log factory is marked as corrupt then the raw store will
     * shut down.
	*/
	public synchronized IOException markCorrupt(IOException originalError) {
		boolean firsttime = false;
	
		if (corrupt == null && originalError != null) {
				corrupt = originalError;
				firsttime = true;
		}
		
		// only print the first error
		if (corrupt == originalError)
			logErrMsg(corrupt);

		// this is the first time someone detects error, shutdown the
		// system as much as possible without further damaging it
		if (firsttime) {
	
				if (logOut != null)
				{
					try
					{
						logOut.corrupt(); // get rid of open file descriptor
					}
					catch (IOException ioe)
					{ 
						// don't worry about it, just trying to clean up 
					}
				}

				// NullPointerException is preferred over corrupting the database
				logOut = null;
		}

		return originalError;
	}

	private void checkCorrupt() throws IOException
	{
			if (corrupt != null)
            {
				throw new IOException(corrupt);
            }
	}

	/*
	** Methods of LogFactory
	*/

	/**
	* @throws IOException 
	*/
	public synchronized Logger getLogger() throws IOException {
			if( fileLogger == null ) {
				fileLogger = new FileLogger(this);
			}
			return fileLogger;
	}


	/**
		Recover the rawStore to a consistent state using the log.

		<P>
		In this implementation, the log is a stream of log records stored in
		one or more flat files.  Recovery is done in 2 passes: redo and undo.
		<BR> <B>Redo pass</B>
		<BR> In the redo pass, reconstruct the state of the rawstore by
		repeating exactly what happened before as recorded in the log.
		In our case just do a scan to recover instances of records to roll back and verify
		checksums.
		<BR><B>Undo pass</B>
		<BR> In the undo pass, all incomplete transactions are rolled back in
		the order from the most recently started to the oldest. Compensation log records are written for each
		block rolled back.

		<P>MT - synchronized

		@see Loggable#needsRedo
		@see FileLogger#redo

		@exception
	*/
	@SuppressWarnings("unused")
	public synchronized void recover() throws IOException
	{
		checkCorrupt();
		//if (firstLog != null) {
		//	logOut = new LogAccessFile(firstLog, logBufferSize);
		//}

		//if( DEBUG ) {
		//	System.out.println("LogToFile.Recover, "+firstLog+" size:"+logBufferSize+" recovery:"+recoveryNeeded);
		//}
		// we don't want to set ReadOnlyDB before recovery has a chance to look
		// at the latest checkpoint and determine that the database is shutdown
		// cleanly.  If the medium is read only but there are logs that need
		// to be redone or in flight transactions, we are hosed.  The logs that
        // are redone will leave dirty pages in the cache.

		if (recoveryNeeded)
		{
			try
			{
				/////////////////////////////////////////////////////////////
				//
				// During boot time, the log control file is accessed and
				// bootTimeLogFileNumber is determined.  LogOut is not set up.
				// bootTimeLogFileNumber is the log file the latest checkpoint
				// lives in,
				// or 1.  It may not be the latest log file (the system may have
				// crashed between the time a new log was generated and the
				// checkpoint log written), that can only be determined at the
				// end of recovery redo.
				//
				/////////////////////////////////////////////////////////////
			
				FileLogger logger = (FileLogger)getLogger();

				/////////////////////////////////////////////////////////////
				//
				// try to find the most recent checkpoint 
				//
				/////////////////////////////////////////////////////////////
				if (checkpointInstance != LogCounter.INVALID_LOG_INSTANCE)
                {
					currentCheckpoint = findCheckpoint(checkpointInstance, logger);
                }

				// if we are only interested in dumping the log, start from the
				// beginning of the first log file
				if (DEBUG && DUMPLOG) {
						currentCheckpoint = null;
						System.out.println("==Dump log==");
						// unless otherwise specified, 1st log file starts at 1
						String beginLogFileNumber =System.getProperty(DUMP_LOG_FROM_LOG_FILE);
						if (beginLogFileNumber != null)
                        {
							bootTimeLogFileNumber = Long.valueOf(beginLogFileNumber).longValue();
                        }
						else
                        {
							bootTimeLogFileNumber = 1;
                        }

						System.out.println("LogToFIle.recover Determining Set Checkpoint for debug.");

						// unless otherwise specified, 1st log file starts at 1
						String checkpointStartLogStr = System.getProperty("checkpointStartLog");
						String checkpointStartOffsetStr = System.getProperty("checkpointStartOffset");

						if ((checkpointStartLogStr != null) && (checkpointStartOffsetStr != null))
                        {
							checkpointInstance =  LogCounter.makeLogInstanceAsLong(
                                    Long.valueOf(checkpointStartLogStr).longValue(),
                                    Long.valueOf(checkpointStartOffsetStr).longValue());
		                    currentCheckpoint = findCheckpoint(checkpointInstance, logger);
		                    if( currentCheckpoint == null) {
		                    	throw new IOException("Failed to find checkpoint at requested position:"+LogCounter.toDebugString(checkpointInstance));
		                    }
                        } else {
                        	System.out.println("No checkpoint set");
                        }
  
				} // end DEBUG && DUMPLOG

				long redoLWM     = LogCounter.INVALID_LOG_INSTANCE;
				long undoLWM     = LogCounter.INVALID_LOG_INSTANCE;

				StreamLogScan redoScan = null;
				/*
				 * If a checkpoint was located then determine first log from there
				 * if not, get it from the boot time file number
				 * Open a redo scan from that point on
				 */
				if (currentCheckpoint != null) {
					redoLWM = currentCheckpoint.redoLWM();
					undoLWM = currentCheckpoint.undoLWM();
					if (ALERT)
					{
						System.out.println("LogToFile.recover FOUND CHECKPOINT at " + LogCounter.toDebugString(checkpointInstance) + 
                          " " + currentCheckpoint.toString());
					}

					setFirstLogFileNumber(LogCounter.getLogFileNumber(redoLWM));

					// If the undo LWM file number is less than first log reported
					// set the first log to the undoLWM file
					if (LogCounter.getLogFileNumber(undoLWM) <  getFirstLogFileNumber())
                    {
						setFirstLogFileNumber(LogCounter.getLogFileNumber(undoLWM));
                    }

					// and open the scan forward from the undo LWM
					redoScan = (StreamLogScan)openForwardScan(undoLWM, (LogInstance)null);
				}
				else
				{
					// no checkpoint
					long start = LogCounter.makeLogInstanceAsLong(bootTimeLogFileNumber, LOG_FILE_HEADER_SIZE);

					// no checkpoint, start redo from the beginning of the 
                    // file - assume this is the first log file
					setFirstLogFileNumber(bootTimeLogFileNumber);
					if( ALERT ) {
						System.out.println("LogToFile.recover NO CHECKPOINT, starting redo from "+bootTimeLogFileNumber+" @ "+LogCounter.toDebugString(start));
					}
					redoScan = (StreamLogScan)openForwardScan(start, (LogInstance)null);
				}

				/////////////////////////////////////////////////////////////
				//
				//  Redo loop - in FileLogger
				//
				/////////////////////////////////////////////////////////////
				inRedo = true;	
				long logEnd = logger.redo( blockIO, redoScan, redoLWM, checkpointInstance);
				inRedo = false;		
   
				// if we are only interested in dumping the log, don't alter
				// the database and prevent anyone from using the log
				if (DEBUG && DUMPLOG)
				{
						System.out.println("==Log dump finished==");
						return;
				}
				/////////////////////////////////////////////////////////////
				//
				// determine where the log ends
				//
				/////////////////////////////////////////////////////////////
				// if logend == LogCounter.INVALID_LOG_SCAN, that means there 
                // is no log record in the log - most likely it is corrupted in
                // some way ...
				if (logEnd == LogCounter.INVALID_LOG_INSTANCE) {
                    throw markCorrupt(new IOException("Invalid log instance returned from redo scan log end"));
				} 
				if( DEBUG ) {
						System.out.println("LogToFile.recover log records present log file #:"+
								logFileNumber+" end:"+LogCounter.toDebugString(logEnd)+
								" current instance:"+LogCounter.toDebugString(currentInstance()));
				}
				// The end of the log is at endPosition.  Which is where
				// the next log should be appending.
				// if the last log record ends before the end of the
                // log file, then this log file has a fuzzy end.
                // Zap all the bytes to between endPosition to EOF to 0.
				//
				redoScan.checkFuzzyLogEnd();
				
				// We should have an undoInstance array that contains the values to
				// recover. We need to generate the CLR's for them and write those to the end
				// As we write each record, the undo will be activated
				// first reverse the scan array as it was forward scanned
				/*
				Collections.reverse(undoInstances);
				Iterator<Long> undoIterator = undoInstances.iterator();
				while(undoIterator.hasNext()) {
					long undoInstance = undoIterator.next();
					LogCounter undoCounter = new LogCounter(undoInstance);
					LogRecord lr = Scan.getRecord(this, undoCounter);
					fileLogger.extractUndoable(blockIO, null, lr, undoCounter);
				}
				*/
				// open the backward undo scan from the end to boot time file beginning
				// no checkpoint
				logger.undo( blockIO, new LogCounter(logEnd), new LogCounter(bootTimeLogFileNumber, LogToFile.LOG_FILE_HEADER_SIZE));
				/////////////////////////////////////////////////////////////
				//
				// End of recovery.
				//
				/////////////////////////////////////////////////////////////


				// do a checkpoint (will flush the log) if there is any rollback
				// if can't checkpoint for some reasons, flush log and carry on
				/*
				if (currentCheckpoint != null ) {
					if (!checkpoint(false))
						flush();
				}
				*/
				// Finished recovery, instead of a checkpoint our simplified protocol will get rid of old logs
				// and start the new sequence for processing
				logger.reset();
				deleteObsoleteLogfilesOnCommit();
				initializeLogFileSequence();
				recoveryNeeded = false;
			}
			catch (IOException ioe)
			{
				if (DEBUG)
					ioe.printStackTrace();
				System.out.println("Recovery fault IO "+ioe.getMessage());
				throw markCorrupt(ioe);
			}
			catch (ClassNotFoundException cnfe)
			{
				if (DEBUG)
					cnfe.printStackTrace();
				System.out.println("Recovery fault ClassNotFound "+cnfe.getMessage());
				throw markCorrupt(new IOException(cnfe));
			}
			/*
			catch (IllegalAccessException e) {
				if (DEBUG)
					e.printStackTrace();
				System.out.println("Recovery fault IllegalAccess "+e.getMessage());
				throw markCorrupt(new IOException(e));
			}
			*/
		} // if recoveryNeeded

        // done with recovery        
        
	}

 
	/**
		Checkpoint the rawStore.
		<P> MT- Only one checkpoint is to be taking place at any given time.
		<P> The steps of a checkpoint are
		<OL>
		<LI> switch to a new log file if possible
		<PRE>
			freeze the log (for the transition to a new log file)
				flush current log file
				create and flush the new log file (with file number 1 higher
                than the previous log file). The new log file becomes the
                current log file.
			unfreeze the log
		</PRE>
		<LI> start checkpoint transaction
		<LI> gather interesting information about the rawStore:
					the current log instance (redoLWM)
					the earliest active transaction begin tran log record 
                    instance (undoLWM), all the truncation LWM set by clients 
                    of raw store (replication)
		<LI> clean the buffer cache 
		<LI> log the next checkpoint log record, which contains 
				(repPoint, undoLWM, redoLWM) and commit checkpoint transaction.
		<LI> synchronously write the control file containing the next checkpoint
				log record log instance
		<LI> the new checkpoint becomes the current checkpoint.
				Somewhere near the beginning of each log file should be a
				checkpoint log record (not guaranteed to be there)
		<LI> see if the log can be truncated

		<P>
		The earliest useful log record is determined by the repPoint and the 
        undoLWM, whichever is earlier. 
		<P>
		Every log file whose log file number is smaller than the earliest 
        useful log record's log file number can be deleted.

		<P><PRE>
			Transactions can be at the following states w/r to a checkpoint -
			consider the log as a continuous stream and not as series of log 
            files for the sake of clarity.  
			|(BT)-------(ET)| marks the begin and end of a transaction.
			.                          checkpoint started
			.       |__undoLWM          |
			.       V                   |___redoLWM
			.                           |___TruncationLWM
			.                           |
			.                           V
			1 |-----------------|
			2       |--------------------------------|
			3           |-------|
			4               |--------------------------------------(end of log)
			5                                       |-^-|
			.                                   Checkpoint Log Record
			---A--->|<-------B--------->|<-------------C-----------
		</PRE>

		<P>
		There are only 3 periods of interest : <BR>
			A) before undoLWM,  B) between undo and redo LWM, C) after redoLWM.

		<P>
		Transaction 1 started in A and terminates in B.<BR>
			During redo, we should only see log records and endXact from this
			transaction in the first phase (between undoLWM and redoLWM).  No
			beginXact log record for this transaction will be seen.

		<P>
		Transaction 2 started in B (right on the undoLWM) and terminated in C.<BR>
			Any transaction that terminates in C must have a beginXact at or 
            after undoLWM.  In other words, no transaction can span A, B and C.
			During redo, we will see beginXact, other log records and endXact 
            for this transaction.

		<P>
		Transaction 3 started in B and ended in B.<BR>
			During redo, we will see beginXact, other log records and endXact 
            for this transaction.

		<P>
		Transaction 4 begins in B and never ends.<BR>
			During redo, we will see beginXact, other log records.
			In undo, this loser transaction will be rolled back.

		<P>
		Transaction 5 is the transaction taking the checkpoint.<BR>
		    The checkpoint action started way back in time but the checkpoint
			log record is only written after the buffer cache has been flushed.

		<P>
		Note that if any time elapse between taking the undoLWM and the
			redoLWM, then it will create a 4th period of interest.

		@exception - encounter exception while doing checkpoint.

        @param wait         If an existing checkpoint is in progress, then if
                            wait=true then this routine will wait for the 
                            checkpoint to complete and then do another checkpoint
                            and wait for it to finish before returning.
	 * @throws IllegalAccessException 
	*/

	public synchronized boolean checkpoint(boolean wait) throws IOException, IllegalAccessException {
		if( DEBUG ) 
			System.out.println("Checkpoint! wait:"+wait+" logOut "+logOut+" corrupt:"+corrupt+" incheckpoint:"+inCheckpoint+" end pos:"+endPosition);
		//LogInstance  redoLWM;
        // we may be called to stop the database after a bad error, make sure
		// logout is set
		if (logOut == null)
		{
			return false;
		}
		long approxLogLength;
		boolean proceed = true;
        do
        {
        	if (corrupt != null) {
                // someone else found a problem in the raw store.  
                throw new IOException(corrupt);
            }
            approxLogLength = endPosition; // current end position
            if (!inCheckpoint) {
                    // no checkpoint in progress, change status to indicate
                    // this code is doing the checkpoint.
                    inCheckpoint = true;
                    // break out of loop and continue to execute checkpoint
                    // in this routine.
                    break;
            } else {
                    // There is a checkpoint in progress.
                    if (wait) {
                        // wait until the thread executing the checkpoint 
                        // completes.
                        // In some cases like backup and compress it is not 
                        // enough that a checkpoint is in progress, the timing 
                        // is important.
                        // In the case of compress it is necessary that the 
                        // redo low water mark be moved forward past all 
                        // operations up to the current time, so that a redo of
                        // the subsequent compress operation is guaranteed
                        // to not encounter any log record on the container 
                        // previous to the compress.  In this case the 'wait'
                        // flag is passed in as 'true'.
                        //
                        // When wait is true and another thread is currently
                        // executing the checkpoint, execution waits here until
                        // the other thread which is actually doing the the 
                        // checkpoint completes.  And then the code will loop
                        // until this thread executes the checkpoint.
                        while (inCheckpoint)
                        {
                            try
                            {
                                wait();
                            } catch (InterruptedException ie){}	
                        }
                    }
                    else
                    {
                        // caller did not want to wait for already executing
                        // checkpoint to finish.  Routine will return false
                        // upon exiting the loop.
                        proceed = false;
                    }
                }
        } while (proceed);

		if (!proceed)
		{
			return false;
		}
		
		if (DEBUG) {
				System.out.println("LogToFile.checkpoint Switching log file: Approx log length = " + approxLogLength + " logSwitchInterval = "+logSwitchInterval);
		}

		try
		{
			if (approxLogLength > logSwitchInterval) {
				if (DEBUG)
				{
						System.out.println("LogToFile.checkpoint Switching log file: Approx log length = " + approxLogLength + " logSwitchInterval = "+logSwitchInterval);
				}
				switchLogFile();

			}
			// start a checkpoint transaction 

			/////////////////////////////////////////////////////
			// gather a snapshot of the various interesting points of the log
			/////////////////////////////////////////////////////
			long undoLWM_long;
			long redoLWM_long;

			// The redo LWM is the current log instance.  We are going to 
            // clean the cache shortly, any log record before this point 
            // will not ever need to be redone.
			redoLWM_long = currentInstance();
			//redoLWM = new LogCounter(redoLWM_long);

            // The undo LWM is what we need to rollback all transactions.
            // Synchronize this with the starting of a new transaction so 
            // that the transaction factory can have a consistent view
            // See FileLogger.logAndDo

			//LogCounter undoLWM = (LogCounter)(tf.firstUpdateInstant());
			//if (undoLWM == null)
			undoLWM_long = redoLWM_long; // since we are handling essentially one transaction at a time we reserve this 
			//else
			//	undoLWM_long = undoLWM.getValueAsLong();

			/////////////////////////////////////////////////////
			// clean the buffer cache
			/////////////////////////////////////////////////////
			//df.checkpoint();

			/////////////////////////////////////////////////////
			// write out the checkpoint log record
			/////////////////////////////////////////////////////
		
			// send the checkpoint record to the log
			CheckpointOperation nextCheckpoint = new CheckpointOperation( redoLWM_long, undoLWM_long, tablespace);	
			FileLogger logger = (FileLogger)getLogger();
			LogCounter checkpointInstance = (LogCounter)logger.logAndDo(blockIO, nextCheckpoint);
                
			if (checkpointInstance != null)
            {
                // since checkpoint is an internal transaction, I need to 
                // flush it to make sure it actually goes to the log
				flush(); 
            }
			else
            {
				throw new IOException("Cannot checkpoint: checkpointInstance null from logAndDo processing checkpoint "+nextCheckpoint);
            }

			//commit();

			/////////////////////////////////////////////////////
			// write out the log control file which contains the last
			// successful checkpoint log record
			/////////////////////////////////////////////////////
			if (!writeControlFile(getControlFileName(), checkpointInstance.getValueAsLong()))
			{
				throw new IOException(getControlFileName().getName());
			}
			// next checkpoint becomes the current checkpoint
			currentCheckpoint = nextCheckpoint;
			////////////////////////////////////////////////////
			// see if we can reclaim some log space
			////////////////////////////////////////////////////
			if (!logArchived())
			{
				truncateLog(currentCheckpoint);
			}	
		}
		catch (IOException ioe)
		{
			throw markCorrupt(ioe);
		}
		finally 
		{
			inCheckpoint = false;
			notifyAll();

		}
		return true;
	}


	/*
	 * Private methods that helps to implement methods of LogFactory
	 */

	/**
		Verify that we the log file is of the right format and of the right
		version and log file number.
		This is called at boot time, it closes the file
		<P>MT - not needed, no global variables used
		@param logFileName the name of the log file
		@param number the log file number
		@return true if the log file is of the current version and of the
		correct format
		@exception StandardException Standard  error policy
	*/
	private long verifyLogFormat(File logFileName, long number) throws IOException 
	{
		RandomAccessFile log = null;
		try {
			log  = privRandomAccessFile(logFileName, "r");
			return verifyLogFormat(log, number);
		} finally {
			if( log != null )
				log.close();
		}

	}

	/**
		Verify that we the log file is of the right format and of the right
		version and log file number.  The log file position is set to the
		beginning.
		int format id - 	the format Id of this log file
		int obsolete log file version - not used
		long log file number - this number orders the log files in a
						series to form the complete transaction log
		long prevLogInstance - log instance of the previous log record, in the
				previous log file. 
		<P>MT - MT-unsafe, caller must synchronize
		@param log the log file
		@param number the log file number
		@return true if the log file is of the current version and of the
		correct format
		@exception StandardException Standard  error policy
	*/
	private long verifyLogFormat(RandomAccessFile log, long number) throws IOException
	{
		long previousLogInstance;
		try 
		{
			log.seek(0);
			int logfid = log.readInt();
			int rtablespace = log.readInt();
			long logNumber = log.readLong();
			if (logfid != fid || logNumber != number || rtablespace != tablespace)
            {
				throw new IOException(dbName);
            }
			previousLogInstance = log.readLong();
	
		}
		catch (IOException ioe)
		{
			throw new IOException(ioe+" "+dbName);
		}

		return previousLogInstance;
	}


	/**
	Initialize the log to the correct format with the given version and
	log file number.  The new log file does NOT have to be empty.  After initializing,
	the file is synchronously written to disk.
		int format id - 	the format Id of this log file
		int tablespace - the tablespace this log operates on
		long log file number - this number orders the log files in a
						series to form the complete transaction log
		long prevLogRecord - log instance of the previous log record, in the
				previous log file. 
	<P>MT - synchronization provided by caller
	@param newlog the new log file to be initialized
	@param number the log file number
	@param prevLogRecordEndInstance the end position of the  previous log record
	@return true if the log file is empty, else false.
	@exception IOException if new log file cannot be accessed or initialized
*/
	private boolean forceInitLogFile(RandomAccessFile newlog, long number, long prevLogRecordEndInstance) throws IOException
	{
		//if (DEBUG) {
		//	testLogFull();
		//}
		newlog.seek(0);
		newlog.writeInt(fid);
		newlog.writeInt(tablespace);
		newlog.writeLong(number);
		newlog.writeLong(prevLogRecordEndInstance);
		newlog.writeInt(0);
		syncFile(newlog);
		newlog.seek(LogToFile.LOG_FILE_HEADER_SIZE);
		return true;
	}
	
	private synchronized void checkLogSwitch() throws IOException {
		if( DEBUG ) {
			System.out.println("LogToFile.checkLogSwitch file:"+logOut.getFilePointer()+" max:"+logSwitchInterval);
		}
		if ( logOut.getFilePointer() >= logSwitchInterval) {
					switchLogFile();
		}
	}
	/**
		Switch to the next log file if possible.
		The assumption is that the current log is full, and the existing buffer needs written to it
		A new buffer is created.
		Allocate the new one first to make sure its possible, then write the old
		This is the place where the file number is incremented.
		The call to here is through 'flush'
		This method makes a lot of suppositions about class fields, so its use is restricted
		LogFileNumber, logOut, and endPosition are possibly affected
	*/
	private synchronized void switchLogFile() throws IOException
	{
		if( DEBUG ) System.out.println("Switch log file");
		boolean switchedOver = false;

		// we have an empty log file here, refuse to switch.
		if (endPosition == LOG_FILE_HEADER_SIZE) {
			//if (DEBUG)
			//{
				System.out.println("LogToFile.switchLogFile not switching from an empty log file (" + logFileNumber + ")");
			//}	
			return;
		}

		// log file isn't being flushed right now and logOut is not being
		// used.
		File newLogFile = getLogFileName(logFileNumber+1);

		if (logFileNumber+1 >= maxLogFileNumber) {
				throw new IOException(maxLogFileNumber+" exceeded max. log file number"); 
        }

		RandomAccessFile newLog = null;	// the new log file
		try {
				// if the log file exist and cannot be deleted, cannot
				// switch log right now
                if (privExists(newLogFile) && !privDelete(newLogFile))
				{
                	if( DEBUG )
                		System.out.println("LogToFile.switchLogFile returning, Cannot delete "+newLogFile.getPath());
					return;
				}

				try {
                    newLog = privRandomAccessFile(newLogFile, "rw");
				} catch (IOException ioe)
				{
					System.out.println("LogToFile.switchLogFile IO problem opening new log file "+newLogFile.getPath()+" error "+ioe.getMessage());
					newLog = null;
				}

                if (newLog == null || !privCanWrite(newLogFile))
				{
					if (newLog != null)
						newLog.close();
					newLog = null;
					System.out.println("LogToFile.switchLogFile cannot write intended new log file "+newLogFile.getPath()+" returning with fail.");
					return;
				}
                // take care of existing log buffer
				// write out an extra 0 at the end to mark the end of the log
				// file.				
                logOut.writeInt(0);
                setEndPosition( logOut.getFilePointer() );
                // flush everything including the int we just wrote on the current log
    			logOut.flushBuffers();
    			logOut.syncLogAccessFile();
                logOut.close();
                
                // sets up checksum log record and calls initBuffer on currentBuffer
                logOut = allocateNewLogFile(newLog, newLogFile, logOut.currentBuffer.greatestInstance, logFileNumber+1, logBufferSize);
                newLog = logOut.getRandomAccessFile();
                //if (initLogFile(newLog, logFileNumber+1, LogCounter.makeLogInstanceAsLong(logFileNumber, endPosition)))
 
				// New log file init ok, close the old one and
				// switch over, after this point, need to shutdown the
				// database if any error crops up
				switchedOver = true;
				setEndPosition( newLog.getFilePointer() );
				setLastFlush(endPosition);
					
				//if (endPosition != LOG_FILE_HEADER_SIZE)
				//		throw new IOException("New log file has unexpected size:" + endPosition);
				logFileNumber++;

				//System.out.println("LogToFile.switchLogFile Cannot create new log file "+newLogFile.getPath()+" returning with fail.");

		} catch (IOException ioe) {

				// switching log file is an optional operation and there is no direct user
				// control.  Just sends a warning message to whomever, if any,
				// system adminstrator there may be

				System.out.println("LogToFile.getLogFile main body exception for log file "+newLogFile.getPath()+" error "+ioe.toString());
				try
				{
					if (newLog != null)
					{
						newLog.close();
						newLog = null;
					}
				}
				catch (IOException ioe2) {
					System.out.println("LogToFile.getLogFile main body exception subexception closing file for log file "+newLogFile.getPath()+" error "+ioe2.toString());
				}

                if (newLogFile != null && privExists(newLogFile))
				{
                    privDelete(newLogFile);
					newLogFile = null;
				}

				if (switchedOver)	// error occur after old log file has been closed!
				{
					// markCorrupt checks logOut for null, calls corrupt, then sets to null
					throw markCorrupt(ioe);
				}
		}	
	}
	/**
	 * Parameter theLog is assumed to be open log file which is synched and closed.
	 * logFile is then created as a randomaccessfile opened in RW
	 * A forceinitLogFile with fileNumber is performed on the logFile
	 * re-open RW and seek to class variable 'endPosition'
	 * The end position is set to the current pointer
	 * The last flush is set with the current pointer
	 * A sync is done on the file
	 * A new LogAccessFile is returned with the logFile, new randomaccessfile with logSize
	 * @param theLog
	 * @param logFile
	 * @param logSize 
	 * @throws IOException
	 */
	private LogAccessFile allocateNewLogFile(RandomAccessFile theLog, File logFile, 
						long prevLogInst, long fileNumber, int logSize) throws IOException {
 
		if( DEBUG) {
			LogCounter pli = new LogCounter(prevLogInst);
			System.out.println("LogToFile.allocateNewLogFile Setting log "+logFile.getName()+" file#:"+fileNumber+
					" length to "+logSize+" end "+endPosition+" previous log instance:"+pli);
		}
	    theLog.setLength(logSwitchInterval + LOG_FILE_HEADER_SIZE);
	    syncFile(theLog);
	    theLog.close();
		theLog = privRandomAccessFile(logFile, "rw");
		if (!forceInitLogFile(theLog, fileNumber, prevLogInst)) // LogCounter.INVALID_LOG_INSTANCE
        {
			throw new IOException("LogToFIle.allocateNewLogFile cannot initialize log "+logFile.getName()+" in "+dbName);
        }

		setEndPosition( theLog.getFilePointer() );
		setLastFlush(theLog.getFilePointer());
		syncFile(theLog);
		//theLog.seek(endPosition);
		return new LogAccessFile(logFile, theLog, logSize);
	}
	/**
	 * Assume we have a randomaccesfile positioned at end for append, header should be in and ready to go
	 * set up the logAccessFile instance
	 * @param logFile
	 * @throws IOException
	 */
	private LogAccessFile allocateExistingLogFile(File logFile, int logSize) throws IOException {
		if( DEBUG)
			System.out.println("LogToFile.allocateExistingLogFile "+logFile.getName()+" log size "+logSize);
		RandomAccessFile theLog = privRandomAccessFile(logFile, "rw");
		endPosition = theLog.length();
		headerLogInstance = verifyLogFormat(theLog, fid);
		// headerLogInstance set up by verify
		// should put us right after header
		assert(theLog.getFilePointer() == LOG_FILE_HEADER_SIZE) : "Existing log file allocation failed header length check";
		return new LogAccessFile(logFile, theLog, logSize);
	}
	
	/** Get rid of old and unnecessary log files

		<P> MT- only one truncate log is allowed to be taking place at any
		given time.  Synchronized on this.
	 * @throws IOException 

	 */
	private void truncateLog(CheckpointOperation checkpoint) throws IOException
	{
		long firstLogNeeded;
		if ((firstLogNeeded = getFirstLogNeeded(checkpoint))==-1)
			return;
		truncateLog(firstLogNeeded);
	}

	/** Get rid of old and unnecessary log files
	 * @param firstLogNeeded The log file number of the oldest log file
	 * needed for recovery.
	 * @throws IOException 
	 */
	private void truncateLog(long firstLogNeeded) throws IOException {
		long oldFirstLog;
		if (keepAllLogs)
			return;
		
		// when  backup is in progress, log files that are yet to
        // be copied to the backup should not be deleted,  even 
        // if they are not required  for crash recovery.
        if(backupInProgress) {
            long logFileNeededForBackup = logFileToBackup;
            // check if the log file number is yet to be copied 
            // to the backup is less than the log file required 
            // for crash recovery, if it is then make the first 
            // log file that should not be deleted is the log file 
            // that is yet to  be copied to the backup.  
            if (logFileNeededForBackup < firstLogNeeded)
                firstLogNeeded = logFileNeededForBackup;
        }

		oldFirstLog = getFirstLogFileNumber();
		setFirstLogFileNumber(firstLogNeeded);

		while(oldFirstLog < firstLogNeeded)
		{
			File uselessLogFile = getLogFileName(oldFirstLog);
            if(privDelete(uselessLogFile)) {
				if (DEBUG) {
					System.out.println("LogToFile.truncateLog removed obsolete log file " + uselessLogFile.getPath());
				}
			} else {
					System.out.println( "LogToFile.truncateLog Failed to remove obsolete log file " + uselessLogFile.getPath());
			}
    		oldFirstLog++;
		}

	}

    /**
     * Return the "oldest" log file still needed by recovery. 
     * <p>
     * Returns the log file that contains the undoLWM, ie. the oldest
     * log record of all uncommitted transactions in the given checkpoint.
     * 
     * If no checkpoint is given then returns -1, indicating all log records
     * may be necessary.
     *
     **/
	private long getFirstLogNeeded(CheckpointOperation checkpoint)
    {
		long firstLogNeeded;
		firstLogNeeded = (checkpoint != null ?  LogCounter.getLogFileNumber(checkpoint.undoLWM()) : -1);
		if (DEBUG) {
                 System.out.println("LogToFile.getfirstLogNeeded: checkpoint:"+checkpoint+" first log needed:" +firstLogNeeded+ 
                		 ",first log FileNumber = " + getFirstLogFileNumber());
		}
		return firstLogNeeded;
	}


	/**
		Carefully write out this value to the control file.
        We do safe write of this data by writing the data 
        into two files every time we write the control data.
        we write checksum at the end of the file, so if by
        chance system crashes while writing into the file,
        using the checksum we find that the control file
        is hosed then we  use the mirror file, which will have
        the control data written at last check point.

		see comment at beginning of file for log control file format.

		<P> MT- synchronized by caller
	*/
	synchronized boolean writeControlFile(File logControlFileName, long value) throws IOException
	{
		RandomAccessFile logControlFile = null;

		ByteArrayOutputStream baos = new ByteArrayOutputStream(64);
		DataOutputStream daos = new DataOutputStream(baos);

		daos.writeInt(fid);
		daos.writeInt(tablespace);
		daos.writeLong(value);

		if (onDiskMajorVersion == 0) {
			onDiskMajorVersion = 1;
			onDiskMinorVersion = 0;
			onDiskBeta = false;
		}

		// previous to 1.3, that's all we wrote.  
		// from 1.3 and onward, also write out the JBMSVersion 
		daos.writeInt(onDiskMajorVersion);
		daos.writeInt(onDiskMinorVersion);

		// build number and the isBeta indication.
		daos.writeInt(0);
		byte flags = 0;
		if (onDiskBeta) 
            flags |= IS_BETA_FLAG;
        
 		daos.writeByte(flags);

		//
		// write some spare bytes after 2.0 we have 3 + 2(8) spare bytes.
 		long spare = 0;
       
		daos.writeByte(0);
		daos.writeByte(0);
        daos.writeByte(0);
		daos.writeLong(spare);
		daos.flush();
		// write the checksum for the control data written
		checksum.reset();
		checksum.update(baos.toByteArray(), 0, baos.size());
		daos.writeLong(checksum.getValue());
		daos.flush();

		try
		{
            checkCorrupt();

			try
			{
                logControlFile = privRandomAccessFile(logControlFileName, "rw");
			}
			catch (IOException ioe)
			{
				logControlFile = null;
				if( DEBUG )
					System.out.println("logControlFile "+logControlFileName+" access failure "+ioe.getMessage()+" returning false from writeControlFile");
				return false;
			}

            if (!privCanWrite(logControlFileName)) {
				if( DEBUG )
					System.out.println("logControlFile "+logControlFileName+" write failure returning false from writeControlFile");
				return false;
            }

			//if (DEBUG)
			//{
			//		testLogFull();
			//}

			logControlFile.seek(0);
			logControlFile.write(baos.toByteArray());
            syncFile(logControlFile);
            logControlFile.close();

			// write the same data to mirror control file
			try
			{
				logControlFile = privRandomAccessFile(getMirrorControlFileName(), "rw");
			}
			catch (IOException ioe)
			{
				if( DEBUG )
					System.out.println("mirrorControlFile "+getMirrorControlFileName()+" write failure"+ioe.getMessage()+" returning false from writeControlFile");
				logControlFile = null;
				return false;
			}

			logControlFile.seek(0);
			logControlFile.write(baos.toByteArray());
            syncFile(logControlFile);

		}
		finally
		{
			if (logControlFile != null)
				logControlFile.close();
		}

		return true;

	}

	/**
	 * Read the control file passed in parameter. Return the check point instance as long
	 * @param logControlFileName
	 * @return
	 * @throws IOException
	 * @throws ChecksumException 
	 */
	private long readControlFile(File logControlFileName) throws IOException, ChecksumException
	{
		RandomAccessFile logControlFile = null;
		ByteArrayInputStream bais = null;
        DataInputStream dais = null;
		logControlFile =  privRandomAccessFile(logControlFileName, "r");
		long value = LogCounter.INVALID_LOG_INSTANCE;
		long onDiskChecksum = 0;
		long controlFilelength = logControlFile.length();
		byte barray[] = null;

		try
		{
			// The length of the file is less than the minimum in any version
            // It is possibly hosed , no point in reading data from this file
            if (controlFilelength < 16)
				onDiskChecksum = -1;
			else if (controlFilelength == 16)
			{
				barray = new byte[16];
				logControlFile.readFully(barray);
			} else 
				if (controlFilelength > 16) {
					barray = new byte[(int) logControlFile.length() - 8];
					logControlFile.readFully(barray);
					onDiskChecksum = logControlFile.readLong();
					if( onDiskChecksum !=0 ) {
						checksum.reset();
						checksum.update(barray, 0, barray.length);
					}
				}

			if ( onDiskChecksum == checksum.getValue() || onDiskChecksum ==0)
			{
				bais = new ByteArrayInputStream(barray);
				dais = new DataInputStream(bais);

				if (dais.readInt() != fid)
	            {
	                throw new IOException( dbName +" tablespace "+tablespace+" corrupted, fails verification read for "+fid);
	            }
	
				int rtablespace = dais.readInt();
				assert(tablespace == rtablespace) : "Log control file for "+dbName+" shows different tablespace than instantiated LogToFile:"+rtablespace+" iinstead of "+tablespace;
				value = dais.readLong();
	
				if (DEBUG)
				{
	              System.out.println("LogToFile.readControlFile checkpoint instance = " + LogCounter.toDebugString(value)+ " for tablespace "+rtablespace);
				}	
	
				// an int for storing version and an int for storing checkpoint interval
				// and log switch interval
				onDiskMajorVersion = dais.readInt();
				onDiskMinorVersion = dais.readInt();
				int dbBuildNumber = dais.readInt();
				int flags = dais.readByte();
						
				onDiskBeta = (flags & IS_BETA_FLAG) != 0;
				if (onDiskBeta)
				{
					// if beta, can only be booted by exactly the same version
				}
					
			} else {
				throw new ChecksumException();
			}
		}
		finally
		{
			if (logControlFile != null)
				logControlFile.close();
			if (bais != null)
				bais.close();
			if (dais != null)
				dais.close();
		}

		return value;

	}

    /**
     * Create the directory where transaction log should go.
     * @exception StandardException Standard Error Policy
    */
	private void createLogDirectory() throws IOException, DirectoryExistsException
	{
		File logDir = 
            new File(blockIO.getDBPath()+File.separator+LogFactory.LOG_DIRECTORY_NAME);

        if (privExists(logDir)) {
            // make sure log directory is empty.
            String[] logfiles = privList(logDir);
            if (logfiles != null) {
                if(logfiles.length != 0) {
                    throw new DirectoryExistsException(logDir.getPath());
                }
            }
            
        } else {
            // create the log directory.
            if (!privMkdirs(logDir)) {
                throw new IOException(logDir.getPath());
            }
            //createDataWarningFile();
        }
    }
	
	/*
		Return the directory the log should go.

		<P> MT- read only
		@exception StandardException  Standard Error Policy
	*/
	public File getLogDirectory() throws IOException
	{
		File logDir = null;

		logDir = new File(blockIO.getDBPath()+File.separator+LogFactory.LOG_DIRECTORY_NAME);

        if (!privExists(logDir))
		{
			throw new IOException(logDir.getPath());
		}

		return logDir;
	}

	/**
		Return the control file name 
		<P> MT- read only
	*/
	private File getControlFileName() throws IOException
	{
		return new File( getLogDirectory()+ File.separator + dbName + tablespace + ".log.ctrl");
	}

	/**
		Return the mirror control file name 
		<P> MT- read only
	*/
	private File getMirrorControlFileName() throws IOException
	{
		return new File( getLogDirectory() + File.separator + dbName + tablespace + ".logmirror.ctrl");
	}

	/**
	 * Given a file number, return the File composed of log dir + sep + dbname + tablespace + . + fileNumber + .log
	 * @param filenumber
	 * @return
	 * @throws IOException
	 */
	File getLogFileName(long filenumber) throws IOException
	{
		return new File( getLogDirectory() + File.separator + dbName + tablespace + "." + filenumber + ".log");
	}

	/**
		Find a checkpoint log record at the checkpointInstance
		<P> MT- read only
	*/
	private CheckpointOperation findCheckpoint(long checkpointInstance, FileLogger logger)
		 throws IOException, ClassNotFoundException {
		LogCounter check =  new LogCounter(checkpointInstance);
		RandomAccessFile scan = getLogFileAtPosition(checkpointInstance);
		Loggable lop = (Loggable)(LogToFile.getRecord(this, check, check.getLogFileNumber(), scan).getLoggable());
		scan.close();
		if (lop instanceof CheckpointOperation)
			return (CheckpointOperation)lop;
		else
			return null;
	}


	/*
	 * Functions to help the Logger open a log scan on the log.
	 */

	/**
		Scan backward from start position.
		<P> MT- read only
		@exception IOException cannot access the log
		@exception StandardException Standard  error policy
	*/
	protected synchronized LogScan openBackwardsScan(long startAt, LogInstance stopAt) throws IOException {
		checkCorrupt();

		// backward from end of log
		if (startAt == LogCounter.INVALID_LOG_INSTANCE)
			throw new IOException("Backward scan start position invalid");

		return new Scan(this, startAt, stopAt, Scan.BACKWARD);
	}

	/**
		Scan backward from end of log.
		<P> MT- read only

		@exception IOException cannot access the log
		@exception StandardException Standard  error policy
	*/
	protected synchronized LogScan openBackwardsScan(LogInstance stopAt) throws IOException {
		checkCorrupt();
		// current instance log instance of the next log record to be
		// written out, which is at the end of the log
		// ensure any buffered data is written to the actual file
		long startAt;
		startAt = currentInstance();	
		return new Scan(this, startAt, stopAt, Scan.BACKWARD_FROM_LOG_END);
	}

	/**
	  @see LogFactory#openFlushedScan
	  @exception 
	 */
	public synchronized ScanHandle openFlushedScan(LogCounter start,int groupsIWant) throws IOException
	{
		return new FlushedScanHandle(this, start, groupsIWant);
	}

	/**
		Scan Forward from start position.

		<P> MT- read only

		@param startAt - if startAt == INVALID_LOG_INSTANCE,
			start from the beginning of the log. Otherwise, start scan from startAt.
		@param stopAt - if not null, stop at this log instance (inclusive).
			Otherwise, stop at the end of the log

		@exception IOException cannot access the log
		@exception 
	*/
	protected synchronized LogScan openForwardScan(long startAt, LogInstance stopAt) throws IOException
	{
		checkCorrupt();
		if (startAt == LogCounter.INVALID_LOG_INSTANCE)
		{
			startAt = firstLogInstance();
			if( DEBUG ) {
				System.out.println("LogToFile.openForwardScan changing start to:"+LogCounter.toDebugString(startAt));
			}
		}
		return new Scan(this, startAt, stopAt, Scan.FORWARD);
	}

	/*
	 * Methods to help a log scan switch from one log file to the next 
	 */

	/**
		Open a log file and position the file at the beginning.
		Used by scan to switch to the next log file

		<P> MT- read only </p>

		<p> When the database is in slave replication mode only:
		Assumes that only recover() will call this method after
		initializeReplicationSlaveRole() has been called, and until slave
		replication has ended. If this changes, the current
		implementation will fail.</p>

		@exception StandardException Standard  error policy
		@exception IOException cannot access the log at the new position.
	*/
	protected synchronized RandomAccessFile getLogFileAtBeginning(long filenumber) throws IOException
	{
        long instance = LogCounter.makeLogInstanceAsLong(filenumber, LOG_FILE_HEADER_SIZE);
        return getLogFileAtPosition(instance);
    }


    /**
        Get a read-only handle to the log file positioned at the stated position

        <P> MT- read only

        @return null if file does not exist or of the wrong format
        @exception IOException cannot access the log at the new position.
        @exception StandardException Standard  error policy
    */
    protected synchronized RandomAccessFile getLogFileAtPosition(long logInstance) throws IOException
	{
		checkCorrupt();

		long filenum = LogCounter.getLogFileNumber(logInstance);
		long filepos = LogCounter.getLogFilePosition(logInstance);

		File fileName = getLogFileName(filenum);
        if (!privExists(fileName))
		{
			if (DEBUG)
			{
					System.out.println("getLogFileAtPosition:"+fileName.getPath() + " does not exist");
			}
			return null;
		}

		RandomAccessFile log = null;

		try
		{
            log = privRandomAccessFile(fileName, "r");
			// verify that the log file is of the right format
            // throw exception otherwise
			headerLogInstance = verifyLogFormat(log, filenum);
			if( DEBUG ) {
					System.out.println("LogToFile.getLogFileAtPosition "+fileName.getPath()+" pos:"+filepos);
			}
			log.seek(filepos);
		}
		catch (IOException ioe) {
			try
			{
				if (log != null) {
					log.close();
					log = null;
				}
				//if (DEBUG)
				//{
					System.out.println("getLogFileAtPosition:cannot get to position " + filepos +
											  " for log file " + fileName.getPath()+" "+ ioe+" file now closed.");
				//}
			}
			catch (IOException ioe2){}
			throw ioe;
		}
		return log;
	}

	/*
	** Methods of ModuleControl
	*/

	public synchronized boolean canSupport(Properties startParams)
	{
		String runtimeLogAttributes = startParams.getProperty(LogFactory.RUNTIME_ATTRIBUTES);
		if (runtimeLogAttributes != null) {
			if (runtimeLogAttributes.equals(LogFactory.RT_READONLY))
				return false;
		}

		return	true;
	}

	/**
		Boot up the log factory.
		<P> MT- caller provide synchronization
		@exception IOException log factory cannot start up
	*/
	public synchronized void boot() throws IOException
	{
		//if user does not set the right value for the log buffer size,
		//default value is used instead.
		logBufferSize =  DEFAULT_LOG_BUFFER_SIZE;
		logArchived = false;
		checkpointInstance = LogCounter.INVALID_LOG_INSTANCE;
		maxLogFileNumber = LogCounter.MAX_LOGFILE_NUMBER;

		if (DEBUG) {
			assert(fid != -1);//, "invalid log format Id");
			System.out.println("RecoveryLogManager boot starting");
		}
		
		// try to access the log
		// if it doesn't exist, create it.
		// if it does exist, run recovery
        try {
            createLogDirectory();
        } catch(DirectoryExistsException dex) {  }    		
	
		File logControlFileName = getControlFileName();
		if( DEBUG ) {
			System.out.println("LogToFile.boot control file "+logControlFileName+" for db "+dbName+" tablespace "+tablespace);
		}
		File logFile;

        if(privExists(logControlFileName)) {
			try {
					checkpointInstance = readControlFile(logControlFileName);
			} catch (ChecksumException e) {
				try {
						checkpointInstance = readControlFile(getMirrorControlFileName());
						// at this point we have a bad control file and a good mirror, restore main control file from mirro
						writeControlFile(logControlFileName,checkpointInstance);
					} catch (ChecksumException e1) {
							throw new IOException("Control files for "+dbName+" are corrupt, cannot boot this table");
					}
			}					

		}

		if (checkpointInstance != LogCounter.INVALID_LOG_INSTANCE) {
					logFileNumber = LogCounter.getLogFileNumber(checkpointInstance);
		} else {
					logFileNumber = 1;
		}

		logFile = getLogFileName(logFileNumber);
				
		if( DEBUG ) {
					LogCounter cpi = new LogCounter(checkpointInstance);
					System.out.println("boot "+dbName+" file#:"+logFileNumber+" tablespace "+tablespace+" found checkpoint instance:"+cpi);
		}

		// if log file is not there set createNewLog
        if (!privExists(logFile)) {
        	assert(logFileNumber == 1) : "Checkpoint instance log file "+logFileNumber+" cannot be located.";
            // brand new log.  Start from log file number 1.
        	if( DEBUG ) {
        			System.out.println("Boot "+dbName+" file:"+logFileNumber+" generate new log from 1");
        	}
        	// create or overwrite the log control file with an invalid
        	// checkpoint instance since there is no checkpoint yet
        	if (writeControlFile(logControlFileName, LogCounter.INVALID_LOG_INSTANCE)) {
        			initializeLogFileSequence();
        			assert(endPosition == LOG_FILE_HEADER_SIZE) : "empty log file has wrong size";
        	} else {
        			if( logOut != null ) {
        					logOut.close();
        					logOut = null;
        			}
        			throw new IOException("Unable to write control file from recovery log boot, critical failure..");
        	}
        	recoveryNeeded = false;
		} else {
			// log file exists
			if( DEBUG ) {
					System.out.println("Boot "+dbName+" tablespace "+tablespace+" file:"+logFileNumber+" for checkpoint "+LogCounter.toDebugString(checkpointInstance));
			}
			// If format is bad, delete it and set createNewLog unless its the first file
			// if we have a bad file 1 toss exception. verifyLogFormat closes file
			headerLogInstance = verifyLogFormat(logFile, logFileNumber);
						
			// log file exist, need to run recovery
			if( ALERT )
				System.out.println("Recovery indicated for "+dbName+" tablespace "+tablespace+" file#:"+logFileNumber+" end position:"+endPosition);
			recoveryNeeded = true;
		}

    	bootTimeLogFileNumber = logFileNumber;
		if( DEBUG )
			System.out.println("LogToFile boot complete for"+dbName+" tablespace"+tablespace+" log file#:"+logFileNumber+" recovery "+(recoveryNeeded ? "IS " : "NOT ")+ "needed");
	} // end of boot

	/**
	 * At the end of commit, truncate the primary log file.
	 * This can be used to override corruption by forcing writes to log files and restoring state
	 * @deprecated
	 * @throws IOException 
	 */
	public void resetLogFiles() throws IOException {
		if( DEBUG )
			System.out.println("Reset log files with current corruption as:"+corrupt);
		// reset corruption
		corrupt = null;
		File logControlFileName = getControlFileName();
		logFileNumber = 1;
		// writeControlfile does a checkCorrupt so corrupt still null if this passes
		if (writeControlFile(logControlFileName,LogCounter.INVALID_LOG_INSTANCE)) {
			File logFile = getLogFileName(logFileNumber);
            RandomAccessFile firstLog = privRandomAccessFile(logFile, "rw");
            //firstLog.getChannel().truncate(LOG_FILE_HEADER_SIZE);
			if (!forceInitLogFile(firstLog, logFileNumber, LogCounter.INVALID_LOG_INSTANCE)) {
				IOException ioe = new IOException(logFile.getPath());
				logErrMsg(ioe);
				corrupt = ioe;
				throw ioe;
            }
			setEndPosition(firstLog.getFilePointer());
			setLastFlush(firstLog.getFilePointer());
			
			assert(endPosition == LOG_FILE_HEADER_SIZE) : "empty log file has wrong size";
			
			setFirstLogFileNumber(logFileNumber);
			if(corrupt == null && !logArchived() && !keepAllLogs )	{
				deleteObsoleteLogfilesOnCommit();
				if( DEBUG )
					System.out.println("Truncated log file "+logFileNumber);
			} else {
				if( DEBUG )
					System.out.println("Truncation of log did not include obsolete file deletion");
			}
		} else {
			// most likely did not pass checkCorrupt in control file write
			RuntimeException e = new RuntimeException("Can not write control file during commit");
			logErrMsg(e);
			corrupt = e;
			throw e;
		}
		
	}
	
	public synchronized void initializeLogFileSequence() throws IOException {
		// brand new log.  Start from log file number 1.
		// create or overwrite the log control file with an invalid
		// checkpoint instance since there is no checkpoint yet
		RandomAccessFile firstLog;
		File logControlFileName = getControlFileName();
		if (writeControlFile(logControlFileName, LogCounter.INVALID_LOG_INSTANCE)) {
			setFirstLogFileNumber(1);
			logFileNumber = 1;
			File logFile = getLogFileName(logFileNumber);
            firstLog = privRandomAccessFile(logFile, "rw");
            if( logOut != null ) {
            	logOut.close();
            }
			logOut = allocateNewLogFile(firstLog, logFile, LogCounter.INVALID_LOG_INSTANCE, logFileNumber, logBufferSize);

			assert(endPosition == LOG_FILE_HEADER_SIZE) : 
					"LogToFile.initializeLogFileSequence empty log file "+logFile.getName()+" in "+dbName+" has wrong size";
		} else {
			if( logOut != null )
				logOut.close();
			logOut = null;
			throw new IOException("Unable to write control file from recovery log boot, critical failure..");
		}

		recoveryNeeded = false;
	}
	
	/**
	* Stop the log factory. Close the LogAccessFile. If not corrupt, not log archived, and not flagged to keep all,
	* remove obsolete log files
	* @throws IOException 
	*/
	public synchronized void stop() throws IOException {
		// stop our checkpoint 
		if (logOut != null) {
				try {
					logOut.close(); // closes actual file
				}
				catch (IOException ioe) {}
				logOut = null;
		}
		if(MEASURE) {
			System.out.println("LogToFile.stop invoked for db "+dbName+" tablespace "+tablespace+". Number of waits = " +
						   mon_numLogFlushWaits +
						   ". Number of times flush called = " +
						   mon_flushCalls +
						   "\nNumber of synch calls = " +
						   mon_syncCalls +
						   " Total number of log bytes written = " +
						   LogAccessFile.mon_numBytesToLog +
						   " Total number of log writes = " +
						   LogAccessFile.mon_numWritesToLog + 
						   "\nCorrupt:"+corrupt+". Log Archive:"+logArchived()+". Keep all logs:"+keepAllLogs);
			System.out.println();
		}	
		// delete obsolete log files,left around by earlier crashes
		// if we have no corruption, are not in log archive mode, and the keep all logs flag is false
		if(corrupt == null && !logArchived() && !keepAllLogs) {
			deleteObsoleteLogfiles();
		}
	}

	/* Delete the log files that might have been left around after failure.
	 * This method should be invoked immediately after the checkpoint before truncation of logs completed.
	 */
	private void deleteObsoleteLogfiles() throws IOException {
		File logDir;
		//find the first log file number that is  useful
		long firstLogNeeded = getFirstLogNeeded(currentCheckpoint);
        if (firstLogNeeded == -1)
			return;
        // when  backup is in progress, log files that are yet to
        // be copied to the backup should not be deleted,  even 
        // if they are not required  for crash recovery.
        if(backupInProgress) {
            long logFileNeededForBackup = logFileToBackup;
            // check if the log file number is yet to be copied 
            // to the backup is less than the log file required 
            // for crash recovery, if it is then make the first 
            // log file that should not be deleted is the log file 
            // that is yet to  be copied to the backup.  
            if (logFileNeededForBackup < firstLogNeeded)
                firstLogNeeded = logFileNeededForBackup;
        }
		logDir = getLogDirectory();
		/*	
		String[] logfiles = privList(logDir);
		if (logfiles != null)
		{
			File uselessLogFile = null;
			long fileNumber;
			for(int i=0 ; i < logfiles.length; i++)
			{
				// delete the log files that are not needed any more
				if(logfiles[i].endsWith(".log"))
				{
					String fileIndex = "";
					for(int k = (logfiles[i].length()-5); k > 0; k--) {
						if( logfiles[i].charAt(k) < '0' || logfiles[i].charAt(k) > '9')
							break;
						fileIndex = logfiles[i].charAt(k)+fileIndex;
					}
					fileNumber = Long.parseLong(fileIndex);
					if(fileNumber < firstLogNeeded )
					{
						uselessLogFile = new File(logDir, logfiles[i]);
						if (privDelete(uselessLogFile)) {
							if (DEBUG) {
								System.out.println("LogToFile.deleteObsoleteLogFiles truncating obsolete log file " + uselessLogFile.getPath());
							}
						} else {
								System.out.println("LogToFile.deleteObsoleteLogFiles Fail to truncate obsolete log file " + uselessLogFile.getPath());
						}
					}
				}
			}
		}
		*/
		List<Path> logfiles = listLogFiles(logDir);
		File uselessLogFile = null;
		long fileNumber;
		for(int i=0 ; i < logfiles.size(); i++) {
			// delete the log files that are not needed any more
			//File flog = new File(logfiles[i]);
			if( DEBUG )
				System.out.println("LogToFile.deleteObsoleteLogfiles examining log file: "+logfiles.get(i)+" for tablespace:"+tablespace+" for "+dbName+" for fileset size:"+logfiles.size());
			String fileIndex = "";
			for(int k = (logfiles.get(i).toString().length()-5); k > 0; k--) {
					if( logfiles.get(i).toString().charAt(k) < '0' || logfiles.get(i).toString().charAt(k) > '9')
						break;
					fileIndex = logfiles.get(i).toString().charAt(k)+fileIndex;
			}
			fileNumber = Long.parseLong(fileIndex);
			if(fileNumber < firstLogNeeded ) {
					uselessLogFile = new File(logfiles.get(i).toString());
					if (privDelete(uselessLogFile)) {
						if (DEBUG) {
							System.out.println("LogToFile.deleteObsoleteLogFiles truncating obsolete log file " + uselessLogFile.getPath());
						}
					} else {
							System.out.println("LogToFile.deleteObsoleteLogFiles Fail to truncate obsolete log file " + uselessLogFile.getPath());
					}
			}
		}
	}
	
	/* Delete the log files that might have been left around after commit.
	 * This relies on the resetLogs method which will reset log file 1 and so
	 * our task here is to delete those files from 2 onward if they exist.
	 */
	public synchronized void deleteObsoleteLogfilesOnCommit() throws IOException {
		File logDir = getLogDirectory();
		/*	
		String[] logfiles = privList(logDir);
		if (logfiles != null)
		{
			File uselessLogFile = null;
			long fileNumber;
			for(int i=0 ; i < logfiles.length; i++)
			{
				// delete the log files that are not needed any more
				//File flog = new File(logfiles[i]);
				if( DEBUG )
					System.out.println("Examining log file: "+logfiles[i]+" for "+dbName);
				if(logfiles[i].startsWith(dbName) && logfiles[i].endsWith(".log"))
				{
					String fileIndex = "";
					for(int k = (logfiles[i].length()-5); k > 0; k--) {
						if( logfiles[i].charAt(k) < '0' || logfiles[i].charAt(k) > '9')
							break;
						fileIndex = logfiles[i].charAt(k)+fileIndex;
					}
					fileNumber = Long.parseLong(fileIndex);
					if( fileNumber > 1 ) {
						uselessLogFile = new File(logDir, logfiles[i]);
						if (privDelete(uselessLogFile))
						{
							if (DEBUG) {
								System.out.println("LogToFile.deleteObsoleteLogfilesOnCommit Deleted obsolete log file " + uselessLogFile.getPath());
							}
						}
						else
						{
							// if we cant delete, throw exception
							// if we try something like truncate to header + 0 for EOF then later
							// we regret it because we have no previous instance linkage to re-init our file
							throw new IOException("Cannot delete obsolete log file "+uselessLogFile+", try manual deletion and continue");
						}
					}
				}
			}
		}
		*/
		List<Path> logfiles = listLogFiles(logDir);
		File uselessLogFile = null;
		long fileNumber;
		for(int i=0 ; i < logfiles.size(); i++)
		{
			// delete the log files that are not needed any more
			//File flog = new File(logfiles[i]);
			if( DEBUG )
				System.out.println("LogToFile.deleteObsoleteLogFilesOnCommit Examining log file: "+logfiles.get(i)+" for "+dbName+" tablespace "+tablespace);
	
				String fileIndex = "";
				for(int k = (logfiles.get(i).toString().length()-5); k > 0; k--) {
					if( logfiles.get(i).toString().charAt(k) < '0' || logfiles.get(i).toString().charAt(k) > '9')
						break;
					fileIndex = logfiles.get(i).toString().charAt(k)+fileIndex;
				}
				fileNumber = Long.parseLong(fileIndex);
				if( fileNumber > 1 ) {
					uselessLogFile = new File(logfiles.get(i).toString());
					if (privDelete(uselessLogFile))
					{
						if (DEBUG) {
							System.out.println("LogToFile.deleteObsoleteLogfilesOnCommit Deleted obsolete log file " + uselessLogFile.getPath());
						}
					}
					else
					{
						// if we cant delete, throw exception
						// if we try something like truncate to header + 0 for EOF then later
						// we regret it because we have no previous instance linkage to re-init our file
						throw new IOException("Cannot delete obsolete log file "+uselessLogFile+", try manual deletion and continue");
					}
				}
		}
	}
	/**
	 * Extract specific database and tablespace logs for this instance
	 * @param logDir
	 * @return
	 * @throws IOException
	 */
	private List<Path> listLogFiles(File logDir) throws IOException {
		Path path = /*getLogDirectory()*/logDir.toPath();
	    List<Path> result = new ArrayList<>();
	    try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, dbName+tablespace+".*.log")) {
	           for (Path entry: stream) {
	               result.add(entry);
	           }
	       } catch (DirectoryIteratorException ex) {
	           // I/O error encounted during the iteration, the cause is an IOException
	           throw ex.getCause();
	       }
	       return result;
	}

	/**
	* This is the primary workhorse method to insert log records.
	* 
	* It takes into account the addition of checksum records and log switches
	* when log size exceeds limit.
	* The endPosition is set to the file end on completion.
	* The instance returned is the file position before the current record was written, i.e.
	* The beginning of the record that was written.
	* 
	*	Method will append 'length' bytes of 'data' to the log prepended by 4 bytes of length information.
	*	and a long log instance that should represent the position of the record.
	*
	* This method is synchronized to ensure log records are added sequentially
	*	to the end of the log.
	*	MT- single threaded through this log factory.  Log records are
	*	appended one at a time.
	*	@exception IOException zero length record, corrupt, logOut null.
	*/
	public synchronized long appendLogRecord(byte[] data, int offset, int length,
			byte[] optionalData, int optionalDataOffset, int optionalDataLength) throws IOException
	{
		if( DEBUG )
			System.out.println("appendLogRecord from offset:"+offset+" length:"+length);
		//boolean testIncompleteLogWrite = false;
		if (length <= 0)
        {
			throw new IOException("zero-length log record");         
        }

		//if (DEBUG)
		//{
		//	testLogFull();	// if log is 'full' this routine will throw an
		//}

		if (corrupt != null) {
				throw new IOException(corrupt);
        }

		if (logOut == null) {
				throw new IOException("Log null");
        }
		
		// set up to call the write of log record and checksum
		previousLogInstance = LogCounter.makeLogInstanceAsLong(logFileNumber, endPosition);
		assert( previousLogInstance != LogInstance.INVALID_LOG_INSTANCE);
		
        logOut.writeLogRecord(length, previousLogInstance,
        				data, offset, optionalData, optionalDataOffset, optionalDataLength);
        
        endPosition = logOut.getFilePointer();
		if (optionalDataLength != 0) {
				if (DEBUG) {
						if (optionalData == null)
							System.out.println("optionalDataLength = " + optionalDataLength +" with null Optional data");
						else
						if (optionalData.length < (optionalDataOffset+optionalDataLength))
							System.out.println("optionalDataLength = " + optionalDataLength +" optionalDataOffset = " + optionalDataOffset + 
							" optionalData.length = " + optionalData.length);
				}
		}
		return previousLogInstance;
	}

	/*
	 * Misc private functions to access the log
	 */

	/**
		Get the current log instance - this is the log instance of the Next log
		record to be written out
		<P> MT - This method is synchronized to ensure that it always points to
		the end of a log record, not the middle of one. 
	*/
	protected synchronized long currentInstance()
	{
		return LogCounter.makeLogInstanceAsLong(logFileNumber, endPosition);
	}

	public synchronized long endPosition()
	{
		return endPosition;
	}

	/**
		Return the current log file number.

		<P> MT - this method is synchronized so that
		it is not in the middle of being changed by swithLogFile
	*/
	public synchronized long getLogFileNumber()
	{
		return logFileNumber;
	}

	/** 
		Get the first valid log instance - this is the beginning of the first
		log file

		<P>MT- synchronized on this
	*/
	private synchronized long firstLogInstance()
	{
		return LogCounter.makeLogInstanceAsLong(getFirstLogFileNumber(), LOG_FILE_HEADER_SIZE);
	}

	/**
		Flush the log such that the log record written with the instance 
        wherePosition is guaranteed to be on disk.
        Calls logOut.flushLogAccessFile
		<P>MT - only one flush is allowed to be taking place at any given time 
		(RESOLVE: right now it is single thread thru the log factory while the log is frozen) 
		@exception cannot sync log file

	*/
	public synchronized void flush() throws IOException
	{
		if( DEBUG ) {
			System.out.println("LogToFile.flush for file handle:"+logOut);
		}

		if (MEASURE)
					mon_flushCalls++;
		// THIS CORRUPT CHECK MUST BE FIRST, before any check that
		// sees if the log has already been flushed to this
		// point. This is based upon the assumption that every
		// dirty page in the cache must call flush() before it is
		// written out.  has someone else found a problem in the
		// raw store?
		if (corrupt != null) {
					throw new IOException(corrupt);
		}
	
		// In non-replicated databases, if we are not
		// corrupt and we are in the middle of redo, we
		// know the log record has already been flushed
		// since we haven't written any log yet. 
		if (recoveryNeeded && inRedo ) {
					if( DEBUG ) {
							System.out.println("LogToFile.flush: recovery needed:"+recoveryNeeded+" redo:"+inRedo+" returning from flush");
					}
					return;
		}

		try
		{
			if (MEASURE)
				mon_syncCalls++;
			if( DEBUG )System.out.println("LogToFile.flush calling flushLogAccessfile on logOut and checking log switch"); 
			logOut.flushBuffers();
			logOut.syncLogAccessFile();
			checkLogSwitch();
		}
		catch (SyncFailedException sfe) 
		{
			System.out.println("Log file synch failed "+sfe.getMessage()+" "+logOut.toString());
			throw markCorrupt(sfe);         
		}
		finally
		{
				notifyAll();
		}	
	}

    /**
     * Utility routine to call sync() on the input file descriptor.
     * <p> 
    */
    void syncFile(RandomAccessFile theLog)  throws IOException {
        for( int i=0; ; )
        {
            // 3311: JVM sync call sometimes fails under high load against NFS 
            // mounted disk.  We re-try to do this 20 times.
            try
            {
                theLog.getFD().sync();
                // the sync succeed, so return
                break;
            }
            catch (IOException ioe)
            {
                i++;
                try
                {
                    // wait for .2 of a second, hopefully I/O is done by now
                    // we wait a max of 4 seconds before we give up
                    Thread.sleep(200);
                }
                catch( InterruptedException ie )
                {          
                }
                if( i > 20 )
                {
                    throw new IOException(ioe);
                }
            }
        }
    }


	/**
	  Open a forward scan of the transaction log.

	  <P> MT- read only
	  @exception IOException  Standard  exception policy
	*/
	public synchronized LogScan openForwardFlushedScan(LogInstance startAt) throws IOException
	{
		checkCorrupt();
		// no need to flush the buffer as it's a flushed scan
		return new FlushedScan(this,((LogCounter)startAt).getValueAsLong());
	}

	/**
	  Get a forwards scan
	  @exception IOException Standard  error policy
	  */
	public synchronized LogScan openForwardScan(LogInstance startAt,LogInstance stopAt) throws IOException
	{
		long startLong;	
		if (startAt == null)
				startLong = LogCounter.INVALID_LOG_INSTANCE;
		else
				startLong = ((LogCounter)startAt).getValueAsLong();
		return openForwardScan(startLong, stopAt);
	}

    /*
     * find if the checkpoint is in the last log file. 
     *
     * <P>MT - synchronization provided by caller - RawStore boot,
     * This method is called only if a crash occured while 
     * re-encrypting the database at boot time. 
     * @return <code> true </code> if if the checkpoint is 
     *                in the last log file, otherwise 
     *                 <code> false </code>.
     */
    public synchronized boolean isCheckpointInLastLogFile() throws IOException
    {
        // check if the checkpoint is done in the last log file. 
        long logFileNumberAfterCheckpoint = 
            LogCounter.getLogFileNumber(checkpointInstance) + 1;

        // check if there is a log file after
        // the log file that has the last 
        // checkpoint record.
        File logFileAfterCheckpoint = 
            getLogFileName(logFileNumberAfterCheckpoint);
        // System.out.println("checking " + logFileAfterCheckpoint);
        if (privExists(logFileAfterCheckpoint))
            return false;
        else 
            return true;
    }
    
    /*
     * delete the log file after the checkpoint. 
     *
     * <P>MT - synchronization provided by caller - RawStore boot,
     * This method is called only if a crash occured while 
     * re-encrypting the database at boot time. 
     */
    public synchronized void deleteLogFileAfterCheckpointLogFile() throws IOException
    {
        long logFileNumberAfterCheckpoint = 
            LogCounter.getLogFileNumber(checkpointInstance) + 1;
        File logFileAfterCheckpoint = 
            getLogFileName(logFileNumberAfterCheckpoint);
        // System.out.println("deleting " + logFileAfterCheckpoint);
        if (privExists(logFileAfterCheckpoint)) 
        {
            // delete the log file (this must have beend encrypted 
            // with the new key.
            if (!privDelete(logFileAfterCheckpoint))
            {
                // throw exception, recovery can not be performed
                // without deleting the log file encyrpted with new key.
                throw new IOException(logFileAfterCheckpoint.getName());
            }
        }
    }


	/**
	  Get the instance of the first record which was not
	  flushed.

	  <P>This only works after running recovery the first time.
	  <P>MT - RESOLVE:
	  */
    public synchronized LogInstance getFirstUnflushedInstance()
	{
		if (DEBUG)
			assert(logFileNumber > 0 && getLastFlush() > 0);
		return new LogCounter(logFileNumber,getLastFlush());
	}

    public synchronized long getFirstUnflushedInstanceAsLong() {
        if (DEBUG) {
            assert(logFileNumber > 0 && getLastFlush() > 0);
        }
        return LogCounter.makeLogInstanceAsLong(logFileNumber,getLastFlush());
    }

	/**
	 * Backup restore - stop sending log record to the log stream
	 * @exception StandardException Standard  error policy
	 */
	public void freezePersistentStore() throws IOException
	{		
	}

	/**
	 * Backup restore - start sending log record to the log stream
	 * @exception StandardException Standard  error policy
	 */
	public synchronized void unfreezePersistentStore() throws IOException
	{
	}

	/**
	 * Backup restore - is the log being archived to some directory?
	 * if log archive mode is enabled return true else false
	 */
	public synchronized boolean logArchived()
	{
		return logArchived;
	}

	/**
	   Check to see if a database has been upgraded to the required
	   level in order to use a store feature.
	   @param requiredMajorVersion  required database Engine major version
	   @param requiredMinorVersion  required database Engine minor version
	   @return True if the database has been upgraded to the required level, false otherwise.
	**/
	synchronized boolean checkVersion(int requiredMajorVersion, int requiredMinorVersion) 
	{
		if(onDiskMajorVersion > requiredMajorVersion )
		{
			return true;
		}
		else
		{
			if(onDiskMajorVersion == requiredMajorVersion &&  onDiskMinorVersion >= requiredMinorVersion)
				return true;
		}
		
		return false;
	}


    /**
     *  Check to see if a database has been upgraded to the required
     *  level in order to use a store feature.
     *
     * @param requiredMajorVersion  required database Engine major version
     * @param requiredMinorVersion  required database Engine minor version
     * @param feature Non-null to throw an exception, null to return the 
     *                state of the version match.
     * @return <code> true </code> if the database has been upgraded to 
     *         the required level, <code> false </code> otherwise.
     * @exception  StandardException 
     *             if the database is not at the require version 
     *             when <code>feature</code> feature is 
     *             not <code> null </code>. 
     */
	public synchronized boolean checkVersion(int requiredMajorVersion, int requiredMinorVersion,  String feature) throws IOException 
    {
        
        boolean isRequiredVersion = checkVersion(requiredMajorVersion, requiredMinorVersion);

        // if the database is not at the required version , throw exception 
        // if the feature is non-null . 
        if (!isRequiredVersion && feature != null) 
        {
            throw new IOException(feature);
        }
        return isRequiredVersion;
    }


	/*
	** Sending information to the user without throwing exception.
	** There are times when unusual external or system related things happen in
	** the log which the user may be interested in but which doesn't impede on
	** the normal operation of the store.  When such an event occur, just send
	** a message or a warning message to the user rather than throw an
	** exception, which will rollback a user transaction or crash the database.
	**
	** logErrMsg - sends a warning message to the user 
	*/

	/**
		Print error message to user about the log
		MT - not needed, informational only
	*/
	protected synchronized void logErrMsg(String msg)
	{
		logErrMsg(new Exception(msg));
	}

	/**
		Print error message to user about the log
	*/
	protected synchronized void logErrMsg(Throwable t)
	{
		if (corrupt != null)
		{
			printErrorStack(corrupt);
		}

		if (t != corrupt)
		{
			printErrorStack(t);
		}
	}

    /**
     * print stack trace from the Throwable including
     * its nested exceptions 
     * @param t trace starts from this error
     */
	private void printErrorStack(Throwable t)
	{
		t.printStackTrace();
    }

	/**
	 *  Testing support
	 */
	/** 
		Writes out a partial log record - takes the appendLogRecord.
		Need to shutdown the database before another log record gets written,
		or the database is not recoverable.
	*/
	private long logtest_appendPartialLogRecord(byte[] data, int offset, 
												int	length,
												byte[] optionalData, 
												int optionalDataOffset, 
												int optionalDataLength)
		throws IOException
	{
		long instance = -1;
		if (DEBUG)
		{
			int bytesToWrite = 1;

			String TestPartialLogWrite = System.getProperty(TEST_LOG_PARTIAL_LOG_WRITE_NUM_BYTES);
			if (TestPartialLogWrite != null)
			{
				bytesToWrite = Integer.parseInt(TestPartialLogWrite);
			}

			System.out.println("TEST_LOG_INCOMPLETE_LOG_WRITE: writing " + bytesToWrite + 
				   " bytes out of " + length + " + " + LOG_RECORD_OVERHEAD + " log record");
								
			synchronized (this) {
					// reserve the space for the checksum log record, returns checksumLogRecordSize
					// NOTE:  bytesToWrite include the log record overhead.
					setEndPosition( endPosition +
						logOut.getLogAccessFileChecksum().setChecksumInstance( (int)logFileNumber,endPosition) );
					instance = currentInstance();

					//check if the length of the records to be written is 
					//actually smaller than the number of bytesToWrite 
					if(length + LOG_RECORD_OVERHEAD < bytesToWrite)
                    { setEndPosition( endPosition + (length + LOG_RECORD_OVERHEAD) ); }
					else
                    { setEndPosition( endPosition + bytesToWrite ); }

					while(true)		// so we can break out without returning out of
						// sync block...
					{
							if (bytesToWrite < 4)
							{
								int shift = 3;
								while(bytesToWrite-- > 0)
								{
									logOut.write((byte)((length >>> 8*shift) & 0xFF));
									shift--;
								}
								break;
							}

							// the length before the log record
							logOut.writeInt(length);
							bytesToWrite -= 4;

							if (bytesToWrite < 8)
							{
								int shift = 7;
								while(bytesToWrite-- > 0)
								{
									logOut.write((byte)((instance >>> 8*shift) & 0xFF));
									shift--;
								}
								break;
							}

							// the log instance
							logOut.writeLong(instance);
							bytesToWrite -= 8;

							if (bytesToWrite < length)
							{
								int dataLength = length - optionalDataLength;
								if(bytesToWrite < dataLength)
									logOut.write(data, offset,bytesToWrite);
								else
								{
									logOut.write(data, offset, dataLength);
									bytesToWrite -= dataLength ;
									if(optionalDataLength != 0 && bytesToWrite > 0)
										logOut.write(optionalData, optionalDataOffset, bytesToWrite);
								}
								break;
							}

							// the log data
							logOut.write(data, offset, length - optionalDataLength);
							//write optional data
							if(optionalDataLength != 0)
								logOut.write(optionalData, optionalDataOffset, optionalDataLength);

							bytesToWrite -= length;

							if (bytesToWrite < 4)
							{
								int shift = 3;
								while(bytesToWrite-- > 0)
								{
									logOut.write((byte)((length >>> 8*shift) & 0xFF));
									shift--;
								}
								break;
							}

							// the length after the log record
							logOut.writeInt(length);
							break;

						}
						// do make sure the partial write gets on disk by sync'ing it
						flush();
					}

			}

			return instance;
	}

	/**
		Simulate a log full condition

		if TEST_LOG_FULL is set to true, then the property
		TEST_RECORD_TO_FILL_LOG indicates the number of times this function is
		call before an IOException simulating a log full condition is raised.

		If TEST_RECORD_TO_FILL_LOG is not set, it defaults to 100 log record
	*/
	protected synchronized void testLogFull() throws IOException
	{
		if (DEBUG)
		{
			if (test_numRecordToFillLog < 0)
			{
				String RecordToFillLog = System.getProperty(TEST_RECORD_TO_FILL_LOG);
				if (RecordToFillLog != null)
					test_numRecordToFillLog = Integer.parseInt(RecordToFillLog);
				else
					test_numRecordToFillLog = 100;
			}

			if (++test_logWritten > test_numRecordToFillLog)
				throw new IOException("TestLogFull " + test_numRecordToFillLog +
									  " written " + test_logWritten+" for db "+dbName+" tablespace "+tablespace);

		}	
	}

	/**
	 * Get the log file to Simulate a log corruption 
	 * FOR UNIT TESTING USAGE ONLY 
	*/
	public RandomAccessFile getLogFileToSimulateCorruption(long filenum) throws IOException
	{
		if (DEBUG)
		{
			//long filenum = LogCounter.getLogFileNumber(logInstant);
			//			long filepos = LogCounter.getLogFilePosition(logInstant);
			File fileName = getLogFileName(filenum);
			return privRandomAccessFile(fileName, "rw");
		}
		
		return null;

	}

	/*********************************************************************
	 * Log Testing
	 * 
	 * Implementations may use these strings to simulate error conditions for
	 * testing purposes.
	 *
	 *********************************************************************/

	/**
	  Set to true if we want the checkpoint to only switch the log but not
	  actually do the checkpoint
	*/
	public static final String TEST_LOG_SWITCH_LOG = DEBUG ? "TEST_LOG_SWITCH_LOG" : null ;

	/**
	  Set to true if we want the upcoming log record to be only partially
	  written.  The database is corrupted if not immediately shutdown.
	  Set TEST_LOG_PARTIAL_LOG_WRITE_NUM_BYTES to the number of bytes to write
	  out, default is 1 byte.
	*/
	public static final String TEST_LOG_INCOMPLETE_LOG_WRITE = DEBUG ? "TEST_LOG_INCOMPLETE_LOG_WRITE" : null;

	/**
	  Set to the number of bytes we want the next log record to actually write
	  out, only used when TEST_LOG_INCOMPLETE_LOG_WRITE is on.  Default is 1
	  byte.
	*/
	public static final String TEST_LOG_PARTIAL_LOG_WRITE_NUM_BYTES = DEBUG ? "unittest.partialLogWrite" : null;

	/**
	  Set to true if we want to simulate a log full condition
	*/
	public static final String TEST_LOG_FULL = 
        DEBUG ? "TEST_LOG_FULL" : null;

	/**
	  Set to true if we want to simulate a log full condition while switching log
	*/
	public static final String TEST_SWITCH_LOG_FAIL1 = DEBUG ? "TEST_SWITCH_LOG_FAIL1" : null;
	public static final String TEST_SWITCH_LOG_FAIL2 =  DEBUG ? "TEST_SWITCH_LOG_FAIL2" : null;

	/**
	  Set to the number of log record we want to write before the log is
	  simulated to be full.
	*/
	public static final String TEST_RECORD_TO_FILL_LOG = 
        DEBUG ? "unittest.recordToFillLog" : null;

	/**
	 * Set to true if we want to simulate max possible log file number is 
     * being used.
	*/
	public static final String TEST_MAX_LOGFILE_NUMBER = 
        DEBUG ? "testMaxLogFileNumber" : null;

	
	//enable the log archive mode
	public void enableLogArchiveMode() throws IOException
	{
	}

	// disable the log archive mode
	public void disableLogArchiveMode() throws IOException
	{
	}


	/*
	 * Start the transaction log backup.  
     *
     * The transaction log is required to bring the database to the consistent 
     * state on restore. 
     *
	 * All the log files that are created after the backup starts 
	 * must be kept around until they are copied into the backup,
	 * even if there are checkpoints when backup is in progress. 
	 *
	 * Copy the log control files to the backup (the checkpoint recorded in the
     * control files is the backup checkpoint). Restore will use the checkpoint 
     * info in these control files to perform recovery to bring 
	 * the database to the consistent state.  
     *
     * Find first log file that needs to be copied into the backup to bring 
     * the database to the consistent state on restore. 
	 * 
     * In the end, existing log files that are needed to recover from the backup
     * checkpoint are copied into the backup, any log that gets generated after
     * this call are also copied into the backup after all the information 
     * in the data containers is written to the backup, when endLogBackup() 
     * is called.
	 *
     * @param toDir - location where the log files should be copied to.
     * @exception StandardException Standard  error policy
	 *
	 */
	public synchronized void startLogBackup(File toDir) throws IOException
	{
		// wait until the thread that is doing the checkpoint completes it.
		while(inCheckpoint) {
				try
				{
					wait();
				}	
				catch (InterruptedException ie) {   }	
		}
		backupInProgress = true;	
		// copy the control files. 
		File fromFile;
		File toFile;
		// copy the log control file
		fromFile = getControlFileName();
		toFile = new File(toDir,fromFile.getName());
		if(!privCopyFile(fromFile, toFile, 6)) {
				throw new IOException(fromFile+" "+toFile);
		}
		// copy the log mirror control file
		fromFile = getMirrorControlFileName();
		toFile = new File(toDir,fromFile.getName());
		if(!privCopyFile(fromFile, toFile, 6)) {
				throw new IOException( fromFile+" "+toFile);
		}
		// find the first log file number that is active
		logFileToBackup = getFirstLogNeeded(currentCheckpoint);
		// copy all the log files that have to go into the backup 
		backupLogFiles(toDir, getLogFileNumber() - 1);
	}	

	/*
	 * copy the log files into the given backup location
     *
     * @param toDir               - location to copy the log files to
     * @param lastLogFileToBackup - last log file that needs to be copied.
	 **/
	private void backupLogFiles(File toDir, long lastLogFileToBackup) throws IOException
	{

		while(logFileToBackup <= lastLogFileToBackup)
		{
			File fromFile = getLogFileName(logFileToBackup);
			File toFile = new File(toDir, fromFile.getName());
			if(!privCopyFile(fromFile, toFile, 6))
			{
				throw new IOException(fromFile+" "+toFile);
			}
			logFileToBackup++;
		}
	}

	/*
	 * copy all the log files that have to go into the backup
	 * and mark that backup is compeleted. 
     *
     * @param toDir - location where the log files should be copied to.
     * @exception StandardException Standard  error policy
	 */
	public synchronized void endLogBackup(File toDir) throws IOException
	{
		long lastLogFileToBackup;

        // Make sure all log records are synced to disk.  The online backup
        // copied data "through" the cache, so may have picked up dirty pages
        // which have not yet synced the associated log records to disk. 
        // Without this force, the backup may end up with page versions 
        // in the backup without their associated log records.
        flush();

		if (logArchived)
		{
			// when the log is being archived for roll-forward recovery
			// we would like to switch to a new log file.
			// otherwise during restore logfile in the backup could 
			// overwrite the more uptodate log files in the 
			// online log path. And also we would like to mark the end
			// marker for the log file other wise during roll-forward recovery,
			// if we see a log file with fuzzy end, we think that is the 
			// end of the recovery.
			switchLogFile();
			lastLogFileToBackup = getLogFileNumber() - 1;
		}
        else
		{
			// for a plain online backup partial filled up log file is ok, 
			// no need to do a log switch.
			lastLogFileToBackup = getLogFileNumber();	
		}

		// backup all the log that got generated after the backup started.
		backupLogFiles(toDir, lastLogFileToBackup);

		// mark that backup is completed.
		backupInProgress = false;
	}


	/*
	 * backup is not in progress any more, it failed for some reason.
	 **/
	public void abortLogBackup()
	{
		backupInProgress = false;
	}


	// Is the transaction in rollforward recovery
	public boolean inRFR()
	{
		/*
		 *Logging System does not differentiate between the
		 *crash-recovery and a rollforward recovery.
		 *Except in case of rollforward atttempt on 
		 *read only databases to check for pending Transaction.
		 *(See the comments in recovery() function)
		 */
		return recoveryNeeded;
	}

	/**	
	 *	redo a checkpoint during rollforward recovery
     * 
     * @throws org.apache..iapi.error.StandardException 
     */
	public synchronized void checkpointInRFR(LogInstance cinstant, long redoLWM, long undoLWM) throws IOException {
		//sync the data
		//write the log control file; this will make sure that restart of the 
		//rollfoward recovery will start this log instance next time instead of
		//from the beginning.
		if (!writeControlFile(getControlFileName(), ((LogCounter)cinstant).getValueAsLong()))
		{
				throw new IOException( "Cannot write control file "+getControlFileName());
		}
	}



	/*preallocate the given log File to the logSwitchInterval size;
	 *file is extended by writing zeros after the header till 
	 *the log file size the set by the user.
	 */	
	private void preAllocateNewLogFile(RandomAccessFile theLog) throws IOException
    {
        //preallocate a file by writing zeros into it . 
        if (DEBUG)
        {
            int currentPostion = (int)theLog.getFilePointer();
            assert(currentPostion == LOG_FILE_HEADER_SIZE) : "New Log File Is not Correctly Initialized";
        }
        /*
        int amountToWrite = logSwitchInterval - LOG_FILE_HEADER_SIZE ;
        int bufferSize = logBufferSize * 2;
        byte[] emptyBuffer = new byte[bufferSize];
        int nWrites = amountToWrite/bufferSize;
        int remainingBytes = amountToWrite % bufferSize;
        while(nWrites-- > 0)
                theLog.write(emptyBuffer);
        if(remainingBytes !=0)
                theLog.write(emptyBuffer , 0 ,remainingBytes);
        */
        theLog.setLength(logSwitchInterval + LOG_FILE_HEADER_SIZE);
        theLog.close();
        //sync the file
        //syncFile(theLog);
    } // end of preAllocateNewLogFile


	/**
	 * open the given log file name for writes; if file can not be 
	 * be opened in write sync mode then disable the write sync mode and 
	 * open the file in "rw" mode.
 	 */
	/*
	public RandomAccessFile openLogFileInWriteMode(File logFile) throws IOException
	{
		RandomAccessFile log = privRandomAccessFile(logFile, "rwd");
		return log ;
	}
	*/
    /*
        Following  methods require Priv Blocks to run under a security manager.
    */
	private int action;
	private File activeFile;
	private File toFile;
	private String activePerms;

    protected boolean privExists(File file)
    {
		return runBooleanAction(0, file);
	}

    protected boolean privDelete(File file)
    {
		return runBooleanAction(1, file);
    }

    public synchronized RandomAccessFile privRandomAccessFile(File file, String perms)
        throws IOException
    {
		action = 2;
        activeFile = file;
        activePerms = perms;
        try
        {
            //return (RandomAccessFile) java.security.AccessController.doPrivileged(this);
        	return (RandomAccessFile)run();
        }
        catch (java.lang./*security.PrivilegedAction*/Exception pae)
        {
            //throw (IOException) pae.getException();
        	throw new IOException(pae);
        }
    }

    private synchronized OutputStreamWriter privGetOutputStreamWriter(File file) throws IOException
    {
        action = 10;
        activeFile = file;
        try
        {
            return (OutputStreamWriter) java.security.AccessController.doPrivileged(this);
        }
        catch (java.security.PrivilegedActionException pae)
        {
            throw (IOException) pae.getException();
        }
    }

    protected boolean privCanWrite(File file)
    {
		return runBooleanAction(3, file);
    }

    protected boolean privMkdirs(File file)
    {
		return runBooleanAction(4, file);
    }


	private synchronized String[] privList(File file)
    {
		action = 5;//or 8 ?
        activeFile = file;

        try
        {
			//return (String[]) java.security.AccessController.doPrivileged(this);
			return (String[]) run();
		}
        catch (java.lang./*security.PrivilegedAction*/Exception pae)
        {
            return null;
        }
	}

	private synchronized boolean privCopyFile(File from, File to, int action)
	{
		//action = 9; or 6
		this.action = action;
		activeFile = from;
		toFile = to;
        try
        {
			//return ((Boolean) java.security.AccessController.doPrivileged(this)).booleanValue();
			return ((Boolean)run()).booleanValue();
		}
        catch (java.lang./*security.PrivilegedAction*/Exception pae)
        {
            return false;
        }	
	}

	private boolean privRemoveDirectory(File file)
	{
		return runBooleanAction(7, file);
	}


	private synchronized boolean runBooleanAction(int action, File file) {
		this.action = action;
		this.activeFile = file;
		/*
		try {
			return ((Boolean) java.security.AccessController.doPrivileged(this)).booleanValue();
		} catch (java.security.PrivilegedActionException pae) {
			return false;
		}
		*/
		try {
			return ((Boolean) run()).booleanValue();
		} catch (java.lang./*security.PrivilegedAction*/Exception pae) {
			return false;
		}
	}

    /** set the endPosition of the log and make sure the new position won't spill off the end of the log */
    private void setEndPosition( long newPosition )
    {
		if (DEBUG)
        {
			assert(newPosition < LogCounter.MAX_LOGFILE_SIZE) : "Log file exceeds maximum at " + newPosition;
			System.out.println("LogToFile.setEndPosition old:"+endPosition+" new:"+newPosition);
			
		}
        endPosition = newPosition;
    }

    public final Object run() throws IOException, Exception {
		switch (action) {
		case 0:
			// SECURITY PERMISSION - MP1
			return new Boolean(activeFile.exists());
		case 1:
			// SECURITY PERMISSION - OP5
           return new Boolean(activeFile.delete());
		case 2:
			// SECURITY PERMISSION - MP1 and/or OP4
			// dependening on the value of activePerms
            boolean exists = activeFile.exists();
            Object result = new RandomAccessFile(activeFile, activePerms);
            return result;
		case 3:
			// SECURITY PERMISSION - OP4
			return new Boolean(activeFile.canWrite());
		case 4:
			// SECURITY PERMISSION - OP4
            boolean created = activeFile.mkdirs();
            return new Boolean(created);
		case 5:
			// SECURITY PERMISSION - MP1
			return activeFile.list();
		case 6:
			// SECURITY PERMISSION - OP4 (Have to check these codes ??)
			Path FROM = Paths.get(activeFile.toURI());
			Path TO = Paths.get(toFile.toURI());
			//overwrite existing file, if exists
			CopyOption[] options = new CopyOption[]{
			      StandardCopyOption.REPLACE_EXISTING,
			      StandardCopyOption.COPY_ATTRIBUTES
			};
			Files.copy(FROM, TO, options);
			return new Boolean(true);
		case 7:
			// SECURITY PERMISSION - OP4
            if(!activeFile.exists())
                return new Boolean( true);
			return new Boolean(activeFile.delete());
        case 8:
            return toFile.list();
        case 9:
			TO = Paths.get(activeFile.toURI());
			FROM = Paths.get(toFile.toURI());
			//overwrite existing file, if exists
			options = new CopyOption[]{
			      StandardCopyOption.REPLACE_EXISTING,
			      StandardCopyOption.COPY_ATTRIBUTES
			};
			Files.copy(FROM, TO, options);
			return new Boolean(true);
        case 10:
        	return(new OutputStreamWriter(new FileOutputStream(activeFile),"UTF8"));

		default:
			return null;
		}
	}

	@Override
	public ScanHandle openFlushedScan() throws IOException {
		return new FlushedScanHandle(this, new LogCounter(), Loggable.ALLGROUPS);
	}

	@Override
	public void getLogFactoryProperties(Set set) throws IOException {
	}

	@Override
	public String getCanonicalLogPath() {
		return blockIO.getDBPath();
	}

	public long getLastFlush() {
		return lastFlush;
	}

	public void setLastFlush(long lastFlush) {
		this.lastFlush = lastFlush;
	}

	public long getFirstLogFileNumber() {
		return firstLogFileNumber;
	}

	public void setFirstLogFileNumber(long firstLogFileNumber) {
		this.firstLogFileNumber = firstLogFileNumber;
	}

	@Override
	public void deleteOnlineArchivedLogFiles() throws IOException {
		deleteObsoleteLogfiles();	
	}

	public static LogRecord getRecord(LogToFile logFactory, LogInstance instance) throws IOException
	{
		if (DEBUG) {
			assert(instance != null);
            System.out.println("LogToFile.getRecord " + instance);
		}
		LogRecord returnRecord;
		long instance_long = ((LogCounter)instance).getValueAsLong();
		long fnum = ((LogCounter)instance).getLogFileNumber();
		long fpos = ((LogCounter)instance).getLogFilePosition();
		if (DEBUG) {
					System.out.println("LogToFile.getRecord temp switch log # " + fnum);         
		}
		RandomAccessFile scanx = logFactory.getLogFileAtPosition(instance_long);
		scanx.seek(fpos);
		returnRecord = readRecord(scanx);
		scanx.close();
		return returnRecord;
	}
	
	public static LogRecord getRecord(LogToFile logFactory, LogInstance instance, long currentLogFileNumber, RandomAccessFile scan) throws IOException
	{
		if (DEBUG) {
			assert(instance != null);
            System.out.println("LogToFile.getRecord " + instance);
		}
		LogRecord returnRecord;
		long instance_long = ((LogCounter)instance).getValueAsLong();
		long fnum = ((LogCounter)instance).getLogFileNumber();
		long fpos = ((LogCounter)instance).getLogFilePosition();
		if (fnum != currentLogFileNumber) {
			if (DEBUG) {
					System.out.println("LogToFile.getRecord temp switch log # "+ currentLogFileNumber + " to " + fnum);         
			}
			RandomAccessFile scanx = logFactory.getLogFileAtPosition(instance_long);
			scanx.seek(fpos);
			returnRecord = readRecord(scanx);
			scanx.close();
		} else {
			if (DEBUG) {
				System.out.println("LogToFile.getRecod log # "+ currentLogFileNumber +" fpos:"+fpos );         
			}
			long recordStartPosition = scan.getFilePointer();
			scan.seek(fpos);
			returnRecord = readRecord(scan);
			scan.seek(recordStartPosition);
		}
		return returnRecord;
	}
	
	public static LogRecord readRecord(RandomAccessFile scan) throws IOException {
		int recordLength = scan.readInt();	
		// read the current log instance
		long currentInstance = scan.readLong();
		// currentInstance is coming from potential newly opened file
		// read in the log record
		byte[] data = new byte[recordLength];
		if( DEBUG ) {
			System.out.println("LogRecord.readRecord reading "+recordLength+" @ "+scan.getFilePointer());
		}
		scan.readFully(data, 0, recordLength);	
		if( DEBUG ) {
			System.out.println("Scan.getNextRecordForward read ended @"+scan.getFilePointer());
		}
		// put the data to 'input'
		ByteBuffer input = ByteBuffer.wrap(data);
		if( DEBUG ) {
			System.out.println("Scan.getNextRecordForward: put data, new buffer position:"+input.position()+
					" new file pos:"+scan.getFilePointer());
		}
		return (LogRecord)(GlobalDBIO.deserializeObject(input));
	}

}
