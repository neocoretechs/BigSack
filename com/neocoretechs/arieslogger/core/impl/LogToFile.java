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

import com.neocoretechs.arieslogger.core.CheckpointDaemonInterface;
import com.neocoretechs.arieslogger.core.LogFactory;
import com.neocoretechs.arieslogger.core.LogInstance;
import com.neocoretechs.arieslogger.core.LogScan;
import com.neocoretechs.arieslogger.core.Logger;
import com.neocoretechs.arieslogger.core.StreamLogScan;
import com.neocoretechs.arieslogger.logrecords.Loggable;
import com.neocoretechs.arieslogger.logrecords.ScanHandle;
import com.neocoretechs.bigsack.io.pooled.BlockDBIO;
import com.neocoretechs.bigsack.session.SessionManager;

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
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
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
	each RawStore only has one log factory, so each RawStore only has one log
	(which composed of multiple log files).
	At any given time, a log factory only writes new log records to one log file,
	this log file is called the 'current log file'.
	<P>
	A log file is named log<em>logNumber</em>.dat
	<P>
	Everytime a checkpoint is taken, a new log file is created and all subsequent
	log records will go to the new log file.  After a checkpoint is taken, old
	and useless log files will be deleted.
	<P>
	RawStore exposes a checkpoint method which clients can call, or a checkpoint is
	taken automatically by the RawStore when
	<OL>
	<LI> the log file grows beyond a certain size (configurable, default 100K bytes)
	<LI> RawStore is shutdown and a checkpoint hasn't been done "for a while"
	<LI> RawStore is recovered and a checkpoint hasn't been done "for a while"
	</OL>
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
		int obsolete log file version - not used
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
	private static final long INT_LENGTH = 4L;
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
	protected static final int LOG_FILE_HEADER_PREVIOUS_LOG_INSTANCE_OFFSET = LOG_FILE_HEADER_SIZE-8;
	// Number of bytes overhead of each log record.
	// 4 bytes of length at the beginning, 8 bytes of log instance,4 bytes ending length for backwards scan
	public static final int LOG_RECORD_OVERHEAD = 16;
	private static final boolean DEBUG = false;
	private static final boolean DUMPLOG = false;
	public static final String DBG_FLAG = DEBUG ? "LogTrace" : null;
	public static final String DUMP_LOG_ONLY = DEBUG ? "DumpLogOnly" : null;
	public static final String DUMP_LOG_FROM_LOG_FILE = 
		DEBUG ? "bigsack.logDumpStart" : null;
	private static final boolean MEASURE = false;

	protected static final String LOG_SYNC_STATISTICS = "LogSyncStatistics";

	/* how big the log file should be before checkpoint or log switch is taken */
	private static final int DEFAULT_LOG_SWITCH_INTERVAL = 1024*1024;		
	private static final int LOG_SWITCH_INTERVAL_MIN     = 100000;
	private static final int LOG_SWITCH_INTERVAL_MAX     = 128*1024*1024;
	private static final int CHECKPOINT_INTERVAL_MIN     = 100000;
	private static final int CHECKPOINT_INTERVAL_MAX     = 128*1024*1024;
	private static final int DEFAULT_CHECKPOINT_INTERVAL = 10*1024*1024;

	//log buffer size values
	private static final int DEFAULT_LOG_BUFFER_SIZE = 32768; //32K
	private static final int LOG_BUFFER_SIZE_MIN = 8192; //8k
	private static final int LOG_BUFFER_SIZE_MAX = LOG_SWITCH_INTERVAL_MAX;
	private int logBufferSize = DEFAULT_LOG_BUFFER_SIZE;

	/* Log Control file flags. */
	private static final byte IS_BETA_FLAG = 0x1;

	private int     logSwitchInterval   = DEFAULT_LOG_SWITCH_INTERVAL;
	private int     checkpointInterval  = DEFAULT_CHECKPOINT_INTERVAL;

	private String dataDirectory; 					// where files are stored
    
	private boolean logBeingFlushed; // is the log in the middle of a flush
									 // (access of the variable should sync on this)
	private FileLogger fileLogger = null;
	protected LogAccessFile logOut;		// an output stream to the log file
								// (access of the variable should sync on this)
	private RandomAccessFile firstLog = null;
	protected long		    endPosition = -1; // end position of the current log file
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
	private CheckpointOperation		 currentCheckpoint;
								// last checkpoint successfully taken
								// 
								// MT - only changed or access in recovery or
								// checkpoint, both are single thread access

	long checkpointInstance;
								// log instance of the current Checkpoint

	private CheckpointDaemonInterface checkpointDaemon;	// the background worker thread who is going to
								// do checkpoints for this log factory.

	private	int	myClientNumber;	
								// use this number to talk to checkpoint Daemon

	private volatile boolean checkpointDaemonCalled; 
								// checkpoint Daemon called already - it is not
								// important that this value is correct, the
								// daemon just need to be called once in a
								// while.  Deamon can handle multiple posts.

	private long logWrittenFromLastCheckPoint = 0;
	                            // keeps track of the amount of log written between checkpoints

	protected boolean	ReadOnlyDB;	// true if this db is read only, i.e, cannotappend log records

	// DEBUG DEBUG - do not truncate log files
	private boolean keepAllLogs;

	// the following booleans are used to put the log factory into various
	// states
	private boolean			 recoveryNeeded = true; // log needs to be recovered
	private boolean			 inCheckpoint = false; 	// in the middle of a checkpoint
	private boolean			 inRedo = false;        // in the middle of redo loop
	private boolean          inLogSwitch = false;

	// make sure we don't do anything after the log factory has been stopped
	private boolean			 stopped = false;

    // disable syncing of log file when running in .system.durability=test
    private boolean logNotSynced = false;

	private volatile boolean logArchived = false;
	private boolean logSwitchRequired = false;

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
		If frozen, don't allow anything on disk to change.
	 */
	private boolean isFrozen;

	/**
		On disk database version information. When running in soft upgrade this version
		may be different to jbmsVersion.
	*/
	private int onDiskMajorVersion;
	private int onDiskMinorVersion;
	private boolean onDiskBeta;
	
	private CRC32 checksum = new CRC32(); // holder for the checksum

	/**
	 *
	 * Write sync mechanism support is added  for performance reasons. 
	 * On commits, logging system has to make sure the log for committed
	 * transaction is on disk. With out write  sync , log is written to the 
	 * disk and then fsync() is used on commits to make log is written to the 
	 * disk for sure. On most of the OS , fsync() calls are expensive. 
	 * On heavy commit oriented systems, file sync make the system run slow.
	 * This problem is solved by using write sync on preallocated log file. 
	 * write sync is much faster than doing write and file sync to a file. 
	 * File should be preallocated for write syncs to perform better than
	 * the file sync method. Whenever a new log file is created, 
	 * logSwitchInterval size is preallocated by writing zeros after file after the header. 
	 */

	/*If set to true , write sync will be used to do log write other file 
	 * level sync is used.
	 */
	private boolean isWriteSynced = false;
    
    // log file that is yet to be copied to backup, updates to this variable 
    // needs to visible  checkpoint thread. 
	private volatile long logFileToBackup ; 
    // It is set to true when  online backup is in progress,  updates to 
    // this variable needs to visible to checkpoint thread. 
    private volatile boolean backupInProgress = false;
    // Name of the BigSack database to log
    private String dbName;
	private BlockDBIO blockIO;
   
	/**
		MT- not needed for constructor
	*/
	public LogToFile(BlockDBIO blockIO) {
		this.blockIO = blockIO;
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

	/*
	** Methods of Corruptable
	*/

	/**
     * Once the log factory is marked as corrupt then the raw store will
     * shut down.
	*/
	public IOException markCorrupt(IOException originalError) {
		boolean firsttime = false;
		synchronized (this) 
		{
			if (corrupt == null && originalError != null)
			{
				corrupt = originalError;
				firsttime = true;
			}
		}
		// only print the first error
		if (corrupt == originalError)
			logErrMsg(corrupt);

		// this is the first time someone detects error, shutdown the
		// system as much as possible without further damaging it
		if (firsttime)
		{
			synchronized(this)
			{
				stopped = true;

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
		}

		return originalError;
	}

	private void checkCorrupt() throws IOException
	{
		synchronized (this) 
        {
			if (corrupt != null)
            {
				throw new IOException(corrupt);
            }
		}
	}

	/*
	** Methods of LogFactory
	*/

	/**
	* @throws IOException 
	*/
	public Logger getLogger() throws IOException {
		if (ReadOnlyDB)
			return null;
		else {
			if( fileLogger == null ) {
				fileLogger = new FileLogger(this);
			}
			return fileLogger;
		}
	}


	/**
		Recover the rawStore to a consistent state using the log.

		<P>
		In this implementation, the log is a stream of log records stored in
		one or more flat files.  Recovery is done in 2 passes: redo and undo.
		<BR> <B>Redo pass</B>
		<BR> In the redo pass, reconstruct the state of the rawstore by
		repeating exactly what happened before as recorded in the log.
		<BR><B>Undo pass</B>
		<BR> In the undo pass, all incomplete transactions are rolled back in
		the order from the most recently started to the oldest.

		<P>MT - synchronization provided by caller - RawStore boot.
		This method is guaranteed to be the only method being called and can
		assume single thread access on all fields.

		@see Loggable#needsRedo
		@see FileLogger#redo

		@exception
	*/
	@SuppressWarnings("unused")
	public void recover() throws IOException
	{
		checkCorrupt();
		if (firstLog != null) {
			logOut = new LogAccessFile(firstLog, logBufferSize);
		}

		if( DEBUG ) {
			System.out.println("Recover, "+firstLog+" size:"+logBufferSize+" recovery:"+recoveryNeeded);
		}
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

						System.out.println("Determining Set Checkpoint for debug.");

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
  
				}

				long redoLWM     = LogCounter.INVALID_LOG_INSTANCE;
				long undoLWM     = LogCounter.INVALID_LOG_INSTANCE;


				StreamLogScan redoScan = null;
				if (currentCheckpoint != null)
				{	

					redoLWM = currentCheckpoint.redoLWM();
					undoLWM = currentCheckpoint.undoLWM();

					if (DEBUG)
					{
						System.out.println("Found checkpoint at " + LogCounter.toDebugString(checkpointInstance) + 
                          " " + currentCheckpoint.toString());
					}

					setFirstLogFileNumber(LogCounter.getLogFileNumber(redoLWM));

					// figure out where the first interesting log file is.
					if (LogCounter.getLogFileNumber(undoLWM) <   getFirstLogFileNumber())
                    {
						setFirstLogFileNumber(LogCounter.getLogFileNumber(undoLWM));
                    }

					// if the checkpoint record doesn't have a transaction
					// table, we need to rebuild it by scanning the log from
					// the undoLWM.  If it does have a transaction table, we
					// only need to scan the log from the redoLWM
					redoScan = (StreamLogScan)openForwardScan(undoLWM, (LogInstance)null);
				}
				else
				{
					// no checkpoint
					long start = LogCounter.makeLogInstanceAsLong(bootTimeLogFileNumber, LOG_FILE_HEADER_SIZE);

					// no checkpoint, start redo from the beginning of the 
                    // file - assume this is the first log file
					setFirstLogFileNumber(bootTimeLogFileNumber);
					if( DEBUG ) {
						System.out.println("No checkpoint, starting redo from "+bootTimeLogFileNumber+" @ "+LogCounter.toDebugString(start));
					}
					redoScan = (StreamLogScan)openForwardScan(start, (LogInstance)null);
				}


				/////////////////////////////////////////////////////////////
				//
				//  Redo loop - in FileLogger
				//
				/////////////////////////////////////////////////////////////

				// 
				// set log factory state to inRedo so that if redo caused any
				// dirty page to be written from the cache, it won't flush the
				// log since the end of the log has not been determined and we
				// know the log record that caused the page to change has
				// already been written to the log.  We need the page write to
				// go thru the log factory because if the redo has a problem,
				// the log factory is corrupt and the only way we know not to
				// write out the page in a checkpoint is if it check with the
				// log factory, and that is done via a flush - we use the WAL
				// protocol to stop corrupt pages from writing to the disk.
				//
				inRedo = true;	
				long logEnd = logger.redo( blockIO, redoScan, redoLWM, checkpointInstance);
				inRedo = false;		
                // Replication slave: When recovery has completed the
                // redo pass, the database is no longer in replication
                // slave mode and only the recover thread will access
                // this object until recover has complete. We
                // therefore do not need two versions of the log file
                // number anymore. From this point on, logFileNumber
                // is used for all references to the current log file
                // number; bootTimeLogFileNumber is no longer used.
                logFileNumber = bootTimeLogFileNumber;
				
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
				RandomAccessFile theLog = null;

				// if logend == LogCounter.INVALID_LOG_SCAN, that means there 
                // is no log record in the log - most likely it is corrupted in
                // some way ...
				if (logEnd == LogCounter.INVALID_LOG_INSTANCE)
				{
					File logFile = getLogFileName(logFileNumber);
                    if (privExists(logFile))
					{
						// if we can delete this strange corrupted file, do so,
						// otherwise, skip it
                        if (!privDelete(logFile))
						{
							logFile = getLogFileName(++logFileNumber);
						}
					}
					IOException accessException = null;
					try
					{
                        theLog =   privRandomAccessFile(logFile, "rw");
					}
					catch (IOException ioe)
					{
						theLog = null;
						accessException = ioe;
					}

                    if (theLog == null || !privCanWrite(logFile))
					{
						if (theLog != null)
							theLog.close();

						theLog = null;
						ReadOnlyDB = true;
					}
					else
					{
						try
						{
							// no previous log file or previous log position
							if (!initLogFile(theLog, logFileNumber, LogCounter.INVALID_LOG_INSTANCE))
                            {
								throw markCorrupt(new IOException(logFile.getPath()));
                            }
						}
						catch (IOException ioe)
						{
							throw markCorrupt(ioe);
						}

                        // successfully init'd the log file - set up markers,
                        // and position at the end of the log.
						setEndPosition( theLog.getFilePointer() );
						setLastFlush(endPosition);
						
						//if write sync is true , prellocate the log file
						//and reopen the file in rwd mode.
						if(isWriteSynced)
						{
							//extend the file by wring zeros to it
							preAllocateNewLogFile(theLog);
							theLog.close();
							theLog = openLogFileInWriteMode(logFile);
							//postion the log at the current end postion
							theLog.seek(endPosition);
						}
						
						if (DEBUG)
						{
							assert(endPosition == LOG_FILE_HEADER_SIZE) : "empty log file has wrong size";
						}
						
						//because we already incrementing the log number
						//here, no special log switch required for
						//backup recoveries.
						logSwitchRequired = false;
					}
				}
				else
				{
					// logEnd is the instance of the next log record in the log
					// it is used to determine the last known good position of
					// the log
					logFileNumber = LogCounter.getLogFileNumber(logEnd);

					ReadOnlyDB = false;

					File logFile = getLogFileName(logFileNumber);

					if (!ReadOnlyDB)
					{
						// if datafactory doesn't think it is readonly, we can
						// do some futher test of our own
						IOException accessException = null;
						try
						{
							if(isWriteSynced)
								theLog = openLogFileInWriteMode(logFile);
							else
								theLog = privRandomAccessFile(logFile, "rw");
						}
						catch (IOException ioe)
						{
							theLog = null;
                            accessException = ioe;
						}
                        if (theLog == null || !privCanWrite(logFile))
						{
							if (theLog != null)
								theLog.close();
							theLog = null;
							if (accessException != null)
								throw accessException;	
							ReadOnlyDB = true;
											
						}
					}

					if (!ReadOnlyDB)
					{
						setEndPosition( LogCounter.getLogFilePosition(logEnd) );
						//
						// The end of the log is at endPosition.  Which is where
						// the next log should be appending.
						//
						// if the last log record ends before the end of the
                        // log file, then this log file has a fuzzy end.
                        // Zap all the bytes to between endPosition to EOF to 0.
						//
						// the end log marker is 4 bytes (of zeros)
						//
						// if endPosition + 4 == logOut.length, we have a
                        // properly terminated log file
						//
						// if endPosition + 4 is > logOut.length, there are 0,
                        // 1, 2, or 3 bytes of 'fuzz' at the end of the log. We
                        // can ignore that because it is guaranteed to be
                        // overwritten by the next log record.
						//
						// if endPosition + 4 is < logOut.length, we have a
                        // partial log record at the end of the log.
						//
						// We need to overwrite all of the incomplete log
                        // record, because if we start logging but cannot
                        // 'consume' all the bad log, then the log will truly
                        // be corrupted if the next 4 bytes (the length of the
                        // log record) after that is small enough that the next
                        // time the database is recovered, it will be
                        // interpreted that the whole log record is in the log
                        // and will try to objectify, only to get classNotFound
                        // error or worse.
						//
						//find out if log had incomplete log records at the end.
						if (redoScan.isLogEndFuzzy())
						{
							theLog.seek(endPosition);
							long eof = theLog.length();

							/* Write zeros from incomplete log record to end of file */
							long nWrites = (eof - endPosition)/logBufferSize;
							int rBytes = (int)((eof - endPosition) % logBufferSize);
							byte zeroBuf[]= new byte[logBufferSize];
							
							//write the zeros to file
							while(nWrites-- > 0)
								theLog.write(zeroBuf);
							if(rBytes !=0)
								theLog.write(zeroBuf, 0, rBytes);
							
							if(!isWriteSynced)
								syncFile(theLog);
						}

						if (DEBUG)
						{
							if (theLog.length() != endPosition)
							{
								assert(theLog.length() > endPosition) : "log end > log file length, bad scan";
							}
						}

						// set the log to the true end position,
                        // and not the end of the file

						setLastFlush(endPosition);
						theLog.seek(endPosition);
					}
				}

				if (theLog != null)
                {
                    if (logOut != null)
                    {
                        // Close the currently open log file, if there is
                        // one. -5937.
                        logOut.close();
                    }
					logOut = new LogAccessFile(theLog, logBufferSize);
                }
				
				if(logSwitchRequired)
					switchLogFile();

				/////////////////////////////////////////////////////////////
				//
				// Undo loop - in transaction factory.  It just gets one
				// transaction at a time from the transaction table and calls
				// undo, no different from runtime.
				//
				/////////////////////////////////////////////////////////////

				/////////////////////////////////////////////////////////////
				//
				// XA prepared xact loop - in transaction factory.  At this
                // point only prepared transactions should be left in the
                // transaction table, all others should have been aborted or
                // committed and removed from the transaction table.  It just
                // gets one transaction at a time from the transaction table,
                // creates a real context and transaction, reclaims locks,
                // and leaves the new xact in the transaction table.
				//
				/////////////////////////////////////////////////////////////

               //if (DEBUG)
               // {
               // 	System.out.println("About to call rePrepare()");
               // }

                //tf.handlePreparedXacts(rawStoreFactory);

                //if (DEBUG)
                //{
                //	System.out.println("Finished rePrepare()");
                //}

				/////////////////////////////////////////////////////////////
				//
				// End of recovery.
				//
				/////////////////////////////////////////////////////////////

				// recovery is finished.  Close the transaction
				//recoveryTransaction.close();


				// notify the dataFactory that recovery is completed,
				// but before the checkpoint is written.
				//dataFactory.postRecovery();


				//////////////////////////////////////////////////////////////
				// set the transaction factory short id, we have seen all the
				// transactions in the log, and at the minimum, the checkpoint
				// transaction will be there.  Set the shortId to the next
				// value.
				//////////////////////////////////////////////////////////////
				//tf.resetTranId();

				// do a checkpoint (will flush the log) if there is any rollback
				// if can't checkpoint for some reasons, flush log and carry on
				if (!ReadOnlyDB)
				{
					boolean needCheckpoint = true;

					// if we can figure out there there is very little in the
					// log (less than 1000 bytes), we haven't done any
                    // rollbacks, then don't checkpoint. Otherwise checkpoint.
					if (currentCheckpoint != null && 
						redoLWM != LogCounter.INVALID_LOG_INSTANCE &&
						undoLWM != LogCounter.INVALID_LOG_INSTANCE)
					{
						if ((logFileNumber == LogCounter.getLogFileNumber(redoLWM))
							&& (logFileNumber == LogCounter.getLogFileNumber(undoLWM))
							&& (endPosition < (LogCounter.getLogFilePosition(redoLWM) + 1000)))
							needCheckpoint = false;
					}

						if (needCheckpoint && !checkpoint(false))
							flush(logFileNumber, endPosition);
				}

				logger.reset();

				recoveryNeeded = false;
			}
			catch (IOException ioe)
			{
				if (DEBUG)
					ioe.printStackTrace();
				throw markCorrupt(ioe);
			}
			catch (ClassNotFoundException cnfe)
			{
				throw markCorrupt(new IOException(cnfe));
			}
			catch (Throwable th)
			{
				if (DEBUG)
                {
					th.printStackTrace();
                }
				throw markCorrupt(new IOException(th));
			}
		} // if recoveryNeeded

        // done with recovery        
        
		/////////////////////////////////////////////////////////////
		// setup checkpoint daemon and cache cleaner
		/////////////////////////////////////////////////////////////
		checkpointDaemon = SessionManager.getCheckpointDaemon();
		if (checkpointDaemon != null)
        {
			myClientNumber = checkpointDaemon.subscribe(this /*onDemandOnly */);
            // use the same daemon for the cache cleaner
            //dataFactory.setupCacheCleaner(checkpointDaemon);
        }
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

	public boolean checkpoint(boolean wait) throws IOException, IllegalAccessException {
		LogInstance  redoLWM;
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
            synchronized (this)
            {
                if (corrupt != null)
                {
                    // someone else found a problem in the raw store.  
                    throw new IOException(corrupt);
                }
                approxLogLength = endPosition; // current end position
                if (!inCheckpoint)
                {
                    // no checkpoint in progress, change status to indicate
                    // this code is doing the checkpoint.
                    inCheckpoint = true;
                    // break out of loop and continue to execute checkpoint
                    // in this routine.
                    break;
                }
                else
                {
                    // There is a checkpoint in progress.
                    if (wait)
                    {
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
                            }	
                            catch (InterruptedException ie)
                            {
                                
                            }	
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
                // don't return from inside of a sync block
            }
        } while (proceed);

		if (!proceed)
		{
			return false;
		}


		if (DEBUG)
		{
			if (logSwitchInterval == 0)
			{
				System.out.println("Switching log file: Approx log length = " + approxLogLength + " logSwitchInterval = 0");
			}
		}

		try
		{
			if (approxLogLength > logSwitchInterval)
			{
				switchLogFile();

				//log switch is occurring in conjunction with the 
				//checkpoint, set the amount of log written from last 
                //checkpoint to zero.
				logWrittenFromLastCheckPoint = 0;
			}
            else
			{
				//checkpoint is happening without the log switch,
				//in the middle of a log file. Amount of log written already for
				//the current log file should not be included in calculation 
				//of when next check point is due. By assigning the negative
				//value of amount of log written for this file. Later it will
				//be subtracted when we switch the log file or while 
                //calculating whether we are due a for checkpoint at flush time.
				logWrittenFromLastCheckPoint = -endPosition;
			}

			// start a checkpoint transaction 

			/////////////////////////////////////////////////////
			// gather a snapshot of the various interesting points of the log
			/////////////////////////////////////////////////////
			long undoLWM_long;
			long redoLWM_long;

			synchronized(this)	// we could synchronized on something else, it
				// doesn't matter as long as logAndDo sync on
				// the same thing
			{
				// The redo LWM is the current log instance.  We are going to 
                // clean the cache shortly, any log record before this point 
                // will not ever need to be redone.
				redoLWM_long = currentInstance();
				redoLWM = new LogCounter(redoLWM_long);

                // The undo LWM is what we need to rollback all transactions.
                // Synchronize this with the starting of a new transaction so 
                // that the transaction factory can have a consistent view
                // See FileLogger.logAndDo

				//LogCounter undoLWM = (LogCounter)(tf.firstUpdateInstant());
				//if (undoLWM == null)
					undoLWM_long = redoLWM_long; // no active transaction 
				//else
				//	undoLWM_long = undoLWM.getValueAsLong();

			}

			/////////////////////////////////////////////////////
			// clean the buffer cache
			/////////////////////////////////////////////////////
			//df.checkpoint();


			/////////////////////////////////////////////////////
			// write out the checkpoint log record
			/////////////////////////////////////////////////////
		
			// send the checkpoint record to the log

			CheckpointOperation nextCheckpoint = new CheckpointOperation( redoLWM_long, undoLWM_long);
			
			FileLogger logger = (FileLogger)getLogger();

			LogCounter checkpointInstance = (LogCounter)logger.logAndDo(blockIO, nextCheckpoint);
                
			if (checkpointInstance != null)
            {
                // since checkpoint is an internal transaction, I need to 
                // flush it to make sure it actually goes to the log
				flush(checkpointInstance); 
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

			if (!writeControlFile(getControlFileName(),
								  checkpointInstance.getValueAsLong()))
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
			synchronized(this)
			{
				inCheckpoint = false;
				notifyAll();
			}

		}
		return true;
	}

	/**
		Flush all unwritten log record up to the log instance indicated to disk
		and sync.
		Also check to see if database is frozen or corrupt.
		<P>MT - not needed, wrapper method
		@param where flush log up to here
		@exception StandardException Standard  error policy
	*/
	public void flush(LogInstance where) throws IOException
	{
		long fileNumber;
		long wherePosition;

		if (where == null) {	
			// don't flush, just use this to check if database is frozen or
			// corrupt 
			fileNumber = 0;
			wherePosition = LogCounter.INVALID_LOG_INSTANCE;
		} else {
			LogCounter whereC = (LogCounter) where;
			fileNumber = whereC.getLogFileNumber();
			wherePosition = whereC.getLogFilePosition();
		}
		flush(fileNumber, wherePosition);
	}

	/**
		Flush all unwritten log record to disk and sync.
		Also check to see if database is frozen or corrupt.

		<P>MT - not needed, wrapper method

		@exception StandardException Standard  error policy
	*/
	public void flushAll() throws IOException
	{
		long fnum;
		long whereTo;

		synchronized(this)
		{
			fnum = logFileNumber;
			whereTo = endPosition;
		}

		flush(fnum, whereTo);
	}

	/*
	 * Private methods that helps to implement methods of LogFactory
	 */

	/**
		Verify that we the log file is of the right format and of the right
		version and log file number.

		<P>MT - not needed, no global variables used

		@param logFileName the name of the log file
		@param number the log file number
		@return true if the log file is of the current version and of the
		correct format

		@exception StandardException Standard  error policy
	*/
	private boolean verifyLogFormat(File logFileName, long number) throws IOException 
	{
		boolean ret = false;
		RandomAccessFile log = privRandomAccessFile(logFileName, "r");
		ret = verifyLogFormat(log, number);
		log.close();
		return ret;
	}

	/**
		Verify that we the log file is of the right format and of the right
		version and log file number.  The log file position is set to the
		beginning.

		<P>MT - MT-unsafe, caller must synchronize

		@param log the log file
		@param number the log file number
		@return true if the log file is of the current version and of the
		correct format

		@exception StandardException Standard  error policy
	*/
	private boolean verifyLogFormat(RandomAccessFile log, long number) throws IOException
	{
		try 
		{
			log.seek(0);
			int logfid = log.readInt();
			long logNumber = log.readLong();

			if (logfid != fid || logNumber != number)
            {
				throw new IOException(getDataDirectory());
            }
		}
		catch (IOException ioe)
		{
			throw new IOException(ioe+" "+getDataDirectory());
		}

		return true;
	}

	/**
		Initialize the log to the correct format with the given version and
		log file number.  The new log file must be empty.  After initializing,
		the file is synchronously written to disk.

		<P>MT - synchronization provided by caller

		@param newlog the new log file to be initialized
		@param number the log file number
		@param prevLogRecordEndInstance the end position of the  previous log record

		@return true if the log file is empty, else false.

		@exception IOException if new log file cannot be accessed or initialized
	*/

	private boolean initLogFile(RandomAccessFile newlog, long number, long prevLogRecordEndInstance) throws IOException
	{
		if (newlog.length() != 0)
			return false;
		//if (DEBUG) {
		//	testLogFull();
		//}
		newlog.seek(0);
		newlog.writeInt(fid);
		newlog.writeLong(number);
		newlog.writeLong(prevLogRecordEndInstance);
		newlog.writeInt(0);
		syncFile(newlog);
		return true;
	}
	private boolean forceInitLogFile(RandomAccessFile newlog, long number, long prevLogRecordEndInstance) throws IOException
	{
		//if (DEBUG) {
		//	testLogFull();
		//}
		newlog.seek(0);
		newlog.writeInt(fid);
		newlog.writeLong(number);
		newlog.writeLong(prevLogRecordEndInstance);
		newlog.writeInt(0);
		syncFile(newlog);
		return true;
	}
	/**
		Switch to the next log file if possible.

		<P>MT - log factory is single threaded thru a log file switch, the log
		is frozen for the duration of the switch
	*/
	public void switchLogFile() throws IOException
	{
		boolean switchedOver = false;

		/////////////////////////////////////////////////////
		// Freeze the log for the switch over to a new log file.
		// This blocks out any other threads from sending log
		// record to the log stream.
		// 
		// The switching of the log file and checkpoint are really
		// independent events, they are tied together just because a
		// checkpoint is the natural place to switch the log and vice
		// versa.  This could happen before the cache is flushed or
		// after the checkpoint log record is written.
		/////////////////////////////////////////////////////
		synchronized (this)
		{

			// Make sure that this thread of control is guaranteed to complete
            // it's work of switching the log file without having to give up
            // the semaphore to a backup or another flusher.  Do this by looping
            // until we have the semaphore, the log is not being flushed, and
            // the log is not frozen for backup.  Track (2985). 
			while(logBeingFlushed | isFrozen)
			{
				try
				{
					wait();
				}
				catch (InterruptedException ie)
				{
                   
				}	
			}

			// we have an empty log file here, refuse to switch.
			if (endPosition == LOG_FILE_HEADER_SIZE)
			{
				if (DEBUG)
				{
					System.out.println("LogToFile.switchLogFile not switching from an empty log file (" + logFileNumber + ")");
				}	
				return;
			}

			// log file isn't being flushed right now and logOut is not being
			// used.
			File newLogFile = getLogFileName(logFileNumber+1);

			if (logFileNumber+1 >= maxLogFileNumber)
            {
				throw new IOException(maxLogFileNumber+" exceeded max. log file number"); 
            }

			RandomAccessFile newLog = null;	// the new log file
			try 
			{
				// if the log file exist and cannot be deleted, cannot
				// switch log right now
                if (privExists(newLogFile) && !privDelete(newLogFile))
				{
					System.out.println("LogToFile.switchLogFile Cannot delete "+newLogFile.getPath());
					return;
				}

				try
				{
                    newLog =   privRandomAccessFile(newLogFile, "rw");
				}
				catch (IOException ioe)
				{
					newLog = null;
				}

                if (newLog == null || !privCanWrite(newLogFile))
				{
					if (newLog != null)
						newLog.close();
					newLog = null;

					return;
				}

				if (initLogFile(newLog, logFileNumber+1, LogCounter.makeLogInstanceAsLong(logFileNumber, endPosition)))
				{

					// New log file init ok, close the old one and
					// switch over, after this point, need to shutdown the
					// database if any error crops up
					switchedOver = true;

					// write out an extra 0 at the end to mark the end of the log
					// file.
					
					logOut.writeEndMarker(0);

					setEndPosition( endPosition + INT_LENGTH );
					//set that we are in log switch to prevent flusher 
					//not requesting  to switch log again 
					inLogSwitch = true; 
					// flush everything including the int we just wrote
					flush(logFileNumber, endPosition);
					
					
					// simulate out of log error after the switch over
					if (DEBUG)
					{
							//throw new IOException("TestLogSwitchFail2");
					}


					logOut.close();		// close the old log file
					
					logWrittenFromLastCheckPoint += endPosition;

					setEndPosition( newLog.getFilePointer() );
					setLastFlush(endPosition);
					
					if(isWriteSynced)
					{
						//extend the file
						preAllocateNewLogFile(newLog);
						newLog.close();
						newLog = openLogFileInWriteMode(newLogFile);
						newLog.seek(endPosition);
					}

					logOut = new LogAccessFile(newLog, logBufferSize);
					newLog = null;

		
					if (endPosition != LOG_FILE_HEADER_SIZE)
							throw new IOException(
											"new log file has unexpected size" +
											 + endPosition);
					logFileNumber++;

				}
				else	// something went wrong, delete the half baked file
				{
					newLog.close();
					newLog = null;

					if (privExists(newLogFile))
					    privDelete(newLogFile);

					System.out.println("LogToFile.switchLogFile Cannot create new log file "+newLogFile.getPath());
					newLogFile = null;
 				}

			}
			catch (IOException ioe)
			{

				inLogSwitch = false;
				// switching log file is an optional operation and there is no direct user
				// control.  Just sends a warning message to whomever, if any,
				// system adminstrator there may be

				System.out.println(newLogFile.getPath()+" "+ioe.toString());

				try
				{
					if (newLog != null)
					{
						newLog.close();
						newLog = null;
					}
				}
				catch (IOException ioe2) {}

                if (newLogFile != null && privExists(newLogFile))
				{
                    privDelete(newLogFile);
					newLogFile = null;
				}

				if (switchedOver)	// error occur after old log file has been closed!
				{
					logOut = null; // limit any damage
					throw markCorrupt(ioe);
				}
			}
			
			inLogSwitch = false;
		}
		// unfreezes the log
	}

	/**
		Flush all unwritten log record up to the log instance indicated to disk
		without syncing.

		<P>MT - not needed, wrapper method

		@param wherePosition flush log up to here

		@exception IOException Failed to flush to the log
	*/
	private void flushBuffer(long fileNumber, long wherePosition) throws IOException
	{
		synchronized (this) {
			if (fileNumber < logFileNumber)	// history
				return;

			// A log instance indicates the start of a log record
			// but not how long it is. Thus the amount of data in
			// the logOut buffer is irrelevant. We can only
			// not flush the buffer if the real synced flush
			// included this required log instance. This is because
			// we never flush & sync partial log records.

			if (wherePosition < getLastFlush()) // already flushed
				return;

			// We don't update lastFlush here because lastFlush
			// is the last position in the log file that has been
			// flushed *and* synced to disk. Here we only flush.
			// ie. lastFlush should be renamed lastSync.
			//
			// We could have another variable indicating to which
			// point the log has been flushed which this routine
			// could take advantage of. This would only help rollbacks though.

			logOut.flushLogAccessFile();
		}
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
			File uselessLogFile = null;
			uselessLogFile = getLogFileName(oldFirstLog);
            if (privDelete(uselessLogFile))
			{
					if (DEBUG)
					{
						
							System.out.println("truncating useless log file " + uselessLogFile.getPath());
					}
			}
			else
			{
					if (DEBUG)
					{
		
							System.out.println( "Fail to truncate useless log file " + uselessLogFile.getPath());
					}
			}
		}

		oldFirstLog++;
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

		// one truncation at a time
		synchronized (this)
		{
			firstLogNeeded = 
                (checkpoint != null ? 
                     LogCounter.getLogFileNumber(checkpoint.undoLWM()) : -1);

			if (DEBUG)
			{
                 System.out.println("getfirstLogNeeded: checkpoint:"+checkpoint+
                		 " first log needed:" +firstLogNeeded+ 
                		 ",first log FileNumber = " + getFirstLogFileNumber());
			}
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
	boolean writeControlFile(File logControlFileName, long value) throws IOException
	{
		RandomAccessFile logControlFile = null;

		ByteArrayOutputStream baos = new ByteArrayOutputStream(64);
		DataOutputStream daos = new DataOutputStream(baos);

		daos.writeInt(fid);
		daos.writeInt(0);
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
		// (5 bytes from our first spare long)
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
				logControlFile =
                    privRandomAccessFile(getMirrorControlFileName(), "rw");
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

	/*
		Carefully read the content of the control file.

		<P> MT- read only
	*/
	private long readControlFile(File logControlFileName) throws IOException
	{
		RandomAccessFile logControlFile = null;
		ByteArrayInputStream bais = null;
        DataInputStream dais = null;
		logControlFile =  privRandomAccessFile(logControlFileName, "r");
		boolean upgradeNeeded = false;
		long value = LogCounter.INVALID_LOG_INSTANCE;
		long onDiskChecksum = 0;
		long controlFilelength = logControlFile.length();
		byte barray[] = null;

		try
		{
			// The length of the file is less than the minimum in any version
            // It is possibly hosed , no point in reading data from this file
            // skip reading checksum  control file is before 1.5
            if (controlFilelength < 16)
				onDiskChecksum = -1;
			else if (controlFilelength == 16)
			{
				barray = new byte[16];
				logControlFile.readFully(barray);
			}else if (controlFilelength > 16)
            {
				barray = new byte[(int) logControlFile.length() - 8];
				logControlFile.readFully(barray);
				onDiskChecksum = logControlFile.readLong();
				if (onDiskChecksum !=0 )
				{
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
	                throw new IOException( getDataDirectory());
	            }
	
				int obsoleteVersion = dais.readInt();
				value = dais.readLong();
	
				if (DEBUG)
				{
	                        System.out.println("log control file ckp instance = " + 
	                        LogCounter.toDebugString(value));
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
            new File(SessionManager.getDbPath()+File.separator+LogFactory.LOG_DIRECTORY_NAME);

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

		logDir = new File(SessionManager.getDbPath()+File.separator+LogFactory.LOG_DIRECTORY_NAME);

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
		return new File( getLogDirectory()+ File.separator + dbName + "log.ctrl");
	}

	/**
		Return the mirror control file name 
		<P> MT- read only
	*/
	private File getMirrorControlFileName() throws IOException
	{
		return new File( getLogDirectory() + File.separator + dbName + "logmirror.ctrl");
	}

	/**
		Given a log file number, return its file name 
		<P> MT- read only
	*/
	private File getLogFileName(long filenumber) throws IOException
	{
		return new File( getLogDirectory() + File.separator + dbName + filenumber + ".log");
	}

	/**
		Find a checkpoint log record at the checkpointInstance
		<P> MT- read only
	*/
	private CheckpointOperation findCheckpoint(long checkpointInstance, FileLogger logger)
		 throws IOException, ClassNotFoundException
	{
		StreamLogScan scan = (StreamLogScan)
			openForwardScan(checkpointInstance, (LogInstance)null);

		// estimated size of a checkpoint log record, which contains 3 longs
		// and assorted other log record overhead
		Loggable lop = logger.readLogRecord(scan, 1000);
								
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
	protected LogScan openBackwardsScan(long startAt, LogInstance stopAt) throws IOException {
		checkCorrupt();

		// backward from end of log
		if (startAt == LogCounter.INVALID_LOG_INSTANCE)
			return openBackwardsScan(stopAt);


		// ensure any buffered data is written to the actual file
		flushBuffer(LogCounter.getLogFileNumber(startAt),
			        LogCounter.getLogFilePosition(startAt));

		return new Scan(this, startAt, stopAt, Scan.BACKWARD);
	}

	/**
		Scan backward from end of log.
		<P> MT- read only

		@exception IOException cannot access the log
		@exception StandardException Standard  error policy
	*/
	protected LogScan openBackwardsScan(LogInstance stopAt) throws IOException {
		checkCorrupt();

		// current instance log instance of the next log record to be
		// written out, which is at the end of the log
		// ensure any buffered data is written to the actual file
		long startAt;
		synchronized (this)
		{
			// flush the whole buffer to ensure the complete
			// end of log is in the file.
			logOut.flushLogAccessFile();
			startAt = currentInstance();	
		}

		return new Scan(this, startAt, stopAt, Scan.BACKWARD_FROM_LOG_END);
	}

	/**
	  @see LogFactory#openFlushedScan
	  @exception 
	 */
	public ScanHandle openFlushedScan(LogCounter start,int groupsIWant) throws IOException
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
	protected LogScan openForwardScan(long startAt, LogInstance stopAt) throws IOException
	{
		checkCorrupt();

		if (startAt == LogCounter.INVALID_LOG_INSTANCE)
		{
			startAt = firstLogInstance();
			if( DEBUG ) {
				System.out.println("LogToFile.openForwardScan changing start to:"+LogCounter.toDebugString(startAt));
			}
		}

		// ensure any buffered data is written to the actual file
		if (stopAt != null) {
			LogCounter stopCounter = (LogCounter) stopAt;
			flushBuffer(stopCounter.getLogFileNumber(),
						stopCounter.getLogFilePosition());
		} else {
			synchronized (this) {
				if (logOut != null)
					// flush to the end of the log
					logOut.flushLogAccessFile();
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
	protected RandomAccessFile getLogFileAtBeginning(long filenumber) throws IOException
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
    protected RandomAccessFile getLogFileAtPosition(long logInstance) throws IOException
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
			if (!verifyLogFormat(log, filenum))
			{
				if (DEBUG)
				{
					System.out.println("getLogFileAtPosition:"+fileName.getPath() + " format mismatch");
				}

				log.close();
				log = null;
			}
			else
			{
				if( DEBUG ) {
					System.out.println("LogToFile.getLogFileAtPosition "+fileName.getPath()+" pos:"+filepos);
				}
				log.seek(filepos);
			}
		}
		catch (IOException ioe)
		{
			try
			{
				if (log != null)
				{
					log.close();
					log = null;
				}

				if (DEBUG)
				{
					System.out.println("getLogFileAtPosition:cannot get to position " + filepos +
											  " for log file " + fileName.getPath()+" "+ ioe);
				}
			}
			catch (IOException ioe2){}
			throw ioe;
		}

		return log;

	}

	/*
	** Methods of ModuleControl
	*/

	public boolean canSupport(Properties startParams)
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
	public void	boot(boolean create) throws IOException
	{
		setDataDirectory(SessionManager.getDbPath());
		// try to access the log
		// if it doesn't exist, create it.
		// if it does exist, run recovery

		boolean createNewLog = create;
		
        if(create) {
        	try {
            createLogDirectory();
        	} catch(DirectoryExistsException dex) { createNewLog = false; }
        }    		
		//if user does not set the right value for the log buffer size,
		//default value is used instead.
		logBufferSize =  DEFAULT_LOG_BUFFER_SIZE;
		logArchived = false;

		/* check if the storage factory supports write sync (rws and rwd). If
		 * so, use it unless .storage.fileSyncTransactionLog property is
		 * set true by user.
		 */
		isWriteSynced = false;
		logNotSynced = true;


		if (DEBUG)
			assert(fid != -1);//, "invalid log format Id");
		
		if( DEBUG )
			System.out.println("RecoveryLog boot starting");
		
		checkpointInstance = LogCounter.INVALID_LOG_INSTANCE;

		File logControlFileName = getControlFileName();
		File logFile;

		if (!createNewLog)
		{
                if (privExists(logControlFileName))
				{
					checkpointInstance = readControlFile(logControlFileName);					
					if (checkpointInstance == LogCounter.INVALID_LOG_INSTANCE && privExists(getMirrorControlFileName()))
                    {
						checkpointInstance = readControlFile(getMirrorControlFileName());
                    }

				}

				if (checkpointInstance != LogCounter.INVALID_LOG_INSTANCE)
					logFileNumber = LogCounter.getLogFileNumber(checkpointInstance);
				else
					logFileNumber = 1;

				logFile = getLogFileName(logFileNumber);

				// if log file is not there or if it is of the wrong format, create a
				// brand new log file and do not attempt to recover the database

                if (!privExists(logFile))
				{
					createNewLog = true;
				}
				else if (!verifyLogFormat(logFile, logFileNumber))
				{

					// blow away the log file if possible
                    if (!privDelete(logFile) && logFileNumber == 1)
                    {
						throw new IOException(getDataDirectory());
                    }

					// If logFileNumber > 1, we are not going to write that 
                    // file just yet.  Just leave it be and carry on.  Maybe 
                    // when we get there it can be deleted.

					createNewLog = true;
				}
		}

		if (createNewLog)
		{
				// brand new log.  Start from log file number 1.

				// create or overwrite the log control file with an invalid
				// checkpoint instance since there is no checkpoint yet
				if (writeControlFile(logControlFileName,
									 LogCounter.INVALID_LOG_INSTANCE))
				{
					setFirstLogFileNumber(1);
					logFileNumber = 1;
					logFile = getLogFileName(logFileNumber);

                    if (privExists(logFile))
					{
                        if (!privDelete(logFile))
                        {
							throw new IOException(getDataDirectory());
                        }
					}

					// don't need to try to delete it, we know it isn't there
                    firstLog = privRandomAccessFile(logFile, "rw");

					if (!initLogFile(firstLog, logFileNumber, LogCounter.INVALID_LOG_INSTANCE))
                    {
						throw new IOException(logFile.getPath());
                    }

					setEndPosition( firstLog.getFilePointer() );
					setLastFlush(firstLog.getFilePointer());

                    //if write sync is true , preallocate the log file
                    //and reopen the file in rwd mode.
                    if(isWriteSynced)
                    {
                        //extend the file by wring zeros to it
                        preAllocateNewLogFile(firstLog);
                        firstLog.close();
                        firstLog = openLogFileInWriteMode(logFile);
                        //position the log at the current log end position
                        firstLog.seek(endPosition);
                    }

					if (DEBUG)
					{
						assert(endPosition == LOG_FILE_HEADER_SIZE) : "empty log file has wrong size";
					}
				}
				else
				{
					// read only database
					ReadOnlyDB = true;
					logOut = null;
					firstLog = null;
				}

				recoveryNeeded = false;
		}
		else
		{
				// log file exist, need to run recovery
				System.out.println("Recovery indicated for starting log file "+logFileNumber+" end position:"+endPosition);
				recoveryNeeded = true;
		}

		maxLogFileNumber = LogCounter.MAX_LOGFILE_NUMBER;

		bootTimeLogFileNumber = logFileNumber;
		
		if( DEBUG )
			System.out.println("RecoveryLog boot complete, log file "+logFileNumber+" recovery "+(recoveryNeeded ? "" : "not ")+ "needed");
	} // end of boot

	/**
	 * At the end of commit, truncate the primary log file.
	 * This can be used to override corruption by forcing writes to log files and restoring state
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
            firstLog = privRandomAccessFile(logFile, "rw");
            firstLog.getChannel().truncate(LOG_FILE_HEADER_SIZE);
			if (!forceInitLogFile(firstLog, logFileNumber, LogCounter.INVALID_LOG_INSTANCE))
            {
				IOException ioe = new IOException(logFile.getPath());
				logErrMsg(ioe);
				corrupt = ioe;
				throw ioe;
            }
			setEndPosition( firstLog.getFilePointer() );
			setLastFlush(firstLog.getFilePointer());
			if (DEBUG)
			{
				assert(endPosition == LOG_FILE_HEADER_SIZE) : "empty log file has wrong size";
			}
			setFirstLogFileNumber(logFileNumber);
			if(corrupt == null && ! logArchived() && !keepAllLogs )	{
				deleteObsoleteLogfiles();
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
	/**
	*    Stop the log factory
	*	<P> MT- caller provide synchronization
	* @throws IOException 
	*/
	public void stop() throws IOException {
		// stop our checkpoint 
		if (checkpointDaemon != null) {
			checkpointDaemon.unsubscribe(myClientNumber);
			checkpointDaemon = null;
		}

		synchronized(this)
		{
			stopped = true;
			if (logOut != null) {
				try {
					logOut.flushLogAccessFile();
					logOut.close();
				}
				catch (IOException ioe) {}
				logOut = null;
			}
		}
  
		if (DEBUG) {
			System.out.println("number of waits = " +
						   mon_numLogFlushWaits +
						   "\nnumber of times flush called = " +
						   mon_flushCalls +
						   "\nnumber of sync called = " +
						   mon_syncCalls +
						   "\ntotal number of bytes written to log = " +
						   LogAccessFile.mon_numBytesToLog +
						   "\ntotal number of writes to log file = " +
						   LogAccessFile.mon_numWritesToLog);
		}	

		// delete obsolete log files,left around by earlier crashes
		if(corrupt == null && ! logArchived() && !keepAllLogs && !ReadOnlyDB)	
			deleteObsoleteLogfiles();
	}



	/* delete the log files, that might have been left around if we crashed
	 * immediately after the checkpoint before truncations of logs completed.
	 * see bug no: 3519 , for more details.
	 */

	private void deleteObsoleteLogfiles() throws IOException {
		File logDir;
		//find the first  log file number that is  useful
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
						if (privDelete(uselessLogFile))
						{
							if (DEBUG)
							{
								System.out.println("truncating obsolete log file " + uselessLogFile.getPath());
							}
						}
						else
						{
							if (DEBUG)
							{
								System.out.println("Fail to truncate obsolete log file " + uselessLogFile.getPath());
							}
						}
					}
				}
			}
		}
	}

	/*
	 * Serviceable methods
	 */

	public boolean serviceASAP()
	{
		return false;
	}

	// @return true, if this work needs to be done on a user thread immediately
	public boolean serviceImmediately()
	{
		return false;
	}	


	public void getLogFactoryProperties() {
		/* log switch interval */
		logSwitchInterval = LOG_SWITCH_INTERVAL_MAX;
		checkpointInterval = CHECKPOINT_INTERVAL_MAX;
	}

	/*
	** Implementation specific methods
	*/

	/**
		Append length bytes of data to the log prepended by a long log instance
		and followed by 4 bytes of length information.

		<P>
		This method is synchronized to ensure log records are added sequentially
		to the end of the log.

		<P>MT- single threaded through this log factory.  Log records are
		appended one at a time.

		@exception StandardException Log Full.

	*/
	public long appendLogRecord(byte[] data, int offset, int length,
			byte[] optionalData, int optionalDataOffset, int optionalDataLength) throws IOException
	{
		long instance;
		boolean testIncompleteLogWrite = false;

		if (ReadOnlyDB)
        {
			throw new IOException("Read-only DB");
        }

		if (length <= 0)
        {
			throw new IOException("zero-length log record");         
        }

		//if (DEBUG)
		//{
		//	testLogFull();	// if log is 'full' this routine will throw an
		//}

		synchronized (this) {
				// has someone else found a problem in the raw store?
				if (corrupt != null)
                {
					throw new IOException(corrupt);
                }

				if (logOut == null)
                {
					throw new IOException("Log null");
                }

				/*
				 * NOTE!!
				 *
				 * subclass which logs special record to the stream depends on
				 * the EXACT byte sequence of the following segment of code.  
				 * If you change this, not only will you need to write upgrade
				 * code for this class, you also need to find all the subclass
				 * which write log record to log stream directly to make sure 
				 * they are OK
				 */

				// see if the log file is too big, if it is, switch to the next
				// log file. account for an extra INT_LENGTH because switchLogFile()
                // writes an extra 0 at the end of the log. in addition, a checksum log record
                // may need to be written (see -2254).
                int checksumLogRecordSize = logOut.getChecksumLogRecordSize();
				if ( (endPosition + LOG_RECORD_OVERHEAD + length + INT_LENGTH + checksumLogRecordSize) >=
                     LogCounter.MAX_LOGFILE_SIZE)
				{
					switchLogFile();

					// still too big??  Giant log record?
                    if ( (endPosition + LOG_RECORD_OVERHEAD + length + INT_LENGTH + checksumLogRecordSize) >=
                         LogCounter.MAX_LOGFILE_SIZE)
                    {
						throw new IOException(
                                "Max log size "+ 
                                new Long(logFileNumber)+ 
                                new Long(endPosition)+
                                new Long(length)+
                                new Long(LogCounter.MAX_LOGFILE_SIZE));
                    }
				}

				//reserve the space for the checksum log record
				setEndPosition( endPosition + logOut.reserveSpaceForChecksum(length, logFileNumber,endPosition) + LOG_RECORD_OVERHEAD);

				// don't call currentInstance since we are already in a
				// synchronzied block 
				instance = 
                    LogCounter.makeLogInstanceAsLong(logFileNumber, endPosition);

                logOut.writeLogRecord(
                    length, instance, data, offset, 
                    optionalData, optionalDataOffset, optionalDataLength);

				if (optionalDataLength != 0) 
                {
					if (DEBUG)
					{
						if (optionalData == null)
							System.out.println(
							"optionalDataLength = " + optionalDataLength +
							" with null Optional data");

						if (optionalData.length < (optionalDataOffset+optionalDataLength))
							System.out.println(
							"optionalDataLength = " + optionalDataLength +
							" optionalDataOffset = " + optionalDataOffset + 
							" optionalData.length = " + optionalData.length);
					}
				}

				setEndPosition( endPosition + (length + LOG_RECORD_OVERHEAD) );
		}

		return instance;
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
		<P>MT - only one flush is allowed to be taking place at any given time 
		(RESOLVE: right now it single thread thru the log factory while the log
		is frozen) 
		@exception cannot sync log file

	*/
	protected void flush(long fileNumber, long wherePosition) throws IOException
	{

		long potentialLastFlush = 0;

		synchronized (this)
		{
			if (MEASURE)
				mon_flushCalls++;
				boolean waited;
				do
				{
					// THIS CORRUPT CHECK MUST BE FIRST, before any check that
					// sees if the log has already been flushed to this
					// point. This is based upon the assumption that every
					// dirty page in the cache must call flush() before it is
					// written out.  has someone else found a problem in the
					// raw store?

					if (corrupt != null)
					{
						throw new IOException(corrupt);
					}

					// now check if database is frozen
					while (isFrozen)
					{
						try 
						{
							wait();
						} 
						catch (InterruptedException ie) 
						{
						}
					}

					// if we are just testing to see to see the database is 
                    // frozen or corrupt (wherePosition == INVALID_LOG_INSTANCE)
                    // then we can return now.
					// if the log file is already flushed up to where we are 
					// interested in, just return.
					if (wherePosition == LogCounter.INVALID_LOG_INSTANCE ||
						fileNumber < logFileNumber ||
						wherePosition < getLastFlush())
					{
						if( DEBUG ) {
							System.out.println("LogToFile.flush: Position @ invalid instance returning from flush");
						}
						return;
					}

					// In non-replicated databases, if we are not
					// corrupt and we are in the middle of redo, we
					// know the log record has already been flushed
					// since we haven't written any log yet. If in
					// slave replication mode, however, log records
					// received from the master may have been
					// written to the log.
					if (recoveryNeeded && inRedo ) 
					{
						if( DEBUG ) {
							System.out.println("LogToFile.flush: recovery needed:"+recoveryNeeded+" redo:"+inRedo+" returning from flush");
						}
						return;
					}

					if (DEBUG)
					{
						if (fileNumber > getLogFileNumber())
							System.out.println("Attempted flush of nonexistant file " + fileNumber + " " + logFileNumber);
                        System.out.println("Flush log to " + wherePosition);
					}

					// There could be multiple threads who wants to flush the 
                    // log file, see if I can be the one.
					if (logBeingFlushed)
					{
						waited = true;
						try
						{
							mon_numLogFlushWaits++;
							wait();	// release log semaphore to let non-flushing
							// threads log stuff while all the flushing 
							// threads wait.

							// now we continue back to see if the sync
							// we waited for, flushed the portion
							// of log we are interested in.
						}
						catch (InterruptedException ie)
						{
						}
					}
					else
					{
						waited = false;

						// logBeingFlushed is false, I am flushing the log now.
						if(!isWriteSynced)
						{
							// Flush any data from the buffered log
							logOut.flushLogAccessFile();
						} else {
							//add active buffers to dirty buffer list
							//to flush to the disk.
							logOut.switchLogBuffer();
						}

						potentialLastFlush = endPosition; // we will flush to to the end

						// once logBeingFlushed is set, need to release
						// the logBeingFlushed flag in finally block.
						logBeingFlushed = true;	

                    }

				} while (waited) ;
				// if I have waited, go down do loop again - hopefully,
				// someone else have already flushed it for me already.
		} // unfreeze log manager to accept more log records

		boolean syncSuceed = false;
		try
		{
			if (DEBUG)
			{
				assert(logBeingFlushed);//, 
									 //"flushing log without logBeingFlushed set");
				assert(potentialLastFlush > 0);//,
									 //"potentialLastFlush not set");
				//testLogFull();
			}	
			mon_syncCalls++;
			if (isWriteSynced)
			{
				//LogAccessFile.flushDirtyBuffers() will allow only one write
				//sync at a time, flush requests will get queued 
				logOut.flushDirtyBuffers();
			}
			else
			{
				if (!logNotSynced)
				    logOut.syncLogAccessFile();
			}
			syncSuceed = true;
		}
		catch (SyncFailedException sfe) 
		{
			throw markCorrupt(sfe);         
		}
		finally
		{
			synchronized(this)
			{
				logBeingFlushed = false; // done flushing

				// update lastFlush under synchronized this instead of synchronized(logOut)
				if (syncSuceed)
				{
					setLastFlush(potentialLastFlush);
				}
				// We may actually have flushed more than that because someone
				// may have done a logOut.flushBuffer right before the sync
				// call. But this is guaranteed to be flushed.
				notifyAll();
			}
		}
		// get checkpoint Daemon to work
		if ((logWrittenFromLastCheckPoint + potentialLastFlush) > checkpointInterval &&
					checkpointDaemon != null &&	!checkpointDaemonCalled && !inLogSwitch)
		{
			// following synchronized block is required to make 
			// sure only one checkpoint request get scheduled.
			synchronized(this)
			{
				// recheck if checkpoint is still required, it is possible some other
				// thread might have already scheduled a checkpoint and completed it. 
				if ((logWrittenFromLastCheckPoint + potentialLastFlush) > checkpointInterval &&
					checkpointDaemon != null &&	!checkpointDaemonCalled && !inLogSwitch)
				{
					checkpointDaemonCalled = true;
					checkpointDaemon.serviceNow(myClientNumber);
				}
			}
		} else {
			// switch the log if required, this case will occur 
			// if log switch interval is less than the checkpoint interval
			// other wise , checkpoint daemon would be doing log switches along
			// with the checkpoints.
			if (potentialLastFlush > logSwitchInterval && !checkpointDaemonCalled && !inLogSwitch) {
				// following synchronized block is required to make sure only
				// one thread switches the log file at a time.
				synchronized(this)
				{
					// recheck if log switch is still required, it is possible some other
					// thread might have already switched the log file. 
					if (potentialLastFlush > logSwitchInterval && !checkpointDaemonCalled && !inLogSwitch)
					{
						inLogSwitch = true;
						switchLogFile();
					}
				}
			}
		}
	}

    /**
     * Utility routine to call sync() on the input file descriptor.
     * <p> 
    */
    private void syncFile(RandomAccessFile theLog)  throws IOException {
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
	  @exception StandardException  Standard  exception policy
	*/
	public LogScan openForwardFlushedScan(LogInstance startAt) throws IOException
	{
		checkCorrupt();
		// no need to flush the buffer as it's a flushed scan
		return new FlushedScan(this,((LogCounter)startAt).getValueAsLong());
	}

	/**
	  Get a forwards scan
	  @exception StandardException Standard  error policy
	  */
	public LogScan openForwardScan(LogInstance startAt,LogInstance stopAt) throws IOException
	{
		long startLong;	
		if (startAt == null)
				startLong = LogCounter.INVALID_LOG_INSTANCE;
		else
				startLong = ((LogCounter)startAt).getValueAsLong();
		return openForwardScan(startLong, stopAt);
	}

    /*
     * set up a new log file to start writing 
     * the log records into the new log file 
     * after this call.
     *
     * <P>MT - synchronization provided by caller - RawStore boot,
     * This method is called while re-encrypting the database 
     * at database boot time.
     */
    public void startNewLogFile() throws IOException
    {
        // switch the database to a new log file.
        switchLogFile();
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
    public boolean isCheckpointInLastLogFile() throws IOException
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
    public void deleteLogFileAfterCheckpointLogFile() throws IOException
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
		// if I get into this synchronized block, I know I am not in the middle
		// of a write because writing to the log file is synchronized under this.
		synchronized(this)
		{
			isFrozen = true;
		}			
	}

	/**
	 * Backup restore - start sending log record to the log stream
	 * @exception StandardException Standard  error policy
	 */
	public void unfreezePersistentStore() throws IOException
	{
		synchronized(this)
		{
			isFrozen = false;
			notifyAll();
		}			
	}

	/**
	 * Backup restore - is the log being archived to some directory?
	 * if log archive mode is enabled return true else false
	 */
	public boolean logArchived()
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
	boolean checkVersion(int requiredMajorVersion, int requiredMinorVersion) 
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
	public boolean checkVersion(int requiredMajorVersion, int requiredMinorVersion,  String feature) throws IOException 
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
	protected void logErrMsg(String msg)
	{
		logErrMsg(new Exception(msg));
	}

	/**
		Print error message to user about the log
		MT - not needed, informational only
	*/
	protected void logErrMsg(Throwable t)
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
					// reserve the space for the checksum log record
					// NOTE:  bytesToWrite include the log record overhead.
					setEndPosition( endPosition +
						logOut.reserveSpaceForChecksum(((length + LOG_RECORD_OVERHEAD) 
														< bytesToWrite ? length :
														(bytesToWrite - LOG_RECORD_OVERHEAD)),
													   logFileNumber,endPosition) );
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
						flush(logFileNumber, endPosition);

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
	protected void testLogFull() throws IOException
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
									  " written " + test_logWritten);

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

	//delete the online archived log files
	public void deleteOnlineArchivedLogFiles() throws IOException
	{
		deleteObsoleteLogfiles();
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
	public void startLogBackup(File toDir) throws IOException
	{
		
		// synchronization is necessary to make sure NO parallel 
		// checkpoint happens when the current checkpoint information 
		// is being copied to the backup.

		synchronized(this) 
		{
			// wait until the thread that is doing the checkpoint completes it.
			while(inCheckpoint)
			{
				try
				{
					wait();
				}	
				catch (InterruptedException ie)
				{
                    
				}	
			}
		
			backupInProgress = true;
		
			// copy the control files. 
			File fromFile;
			File toFile;
			// copy the log control file
			fromFile = getControlFileName();
			toFile = new File(toDir,fromFile.getName());
			if(!privCopyFile(fromFile, toFile, 6))
			{
				throw new IOException(fromFile+" "+toFile);
			}

			// copy the log mirror control file
			fromFile = getMirrorControlFileName();
			toFile = new File(toDir,fromFile.getName());
			if(!privCopyFile(fromFile, toFile, 6))
			{
				throw new IOException( fromFile+" "+toFile);
			}

			// find the first log file number that is active
			logFileToBackup = getFirstLogNeeded(currentCheckpoint);
		}

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
	public void endLogBackup(File toDir) throws IOException
	{
		long lastLogFileToBackup;

        // Make sure all log records are synced to disk.  The online backup
        // copied data "through" the cache, so may have picked up dirty pages
        // which have not yet synced the associated log records to disk. 
        // Without this force, the backup may end up with page versions 
        // in the backup without their associated log records.
        flush(logFileNumber, endPosition);

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
	public void checkpointInRFR(LogInstance cinstant, long redoLWM, long undoLWM) throws IOException {
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
        int amountToWrite = logSwitchInterval - LOG_FILE_HEADER_SIZE ;
        int bufferSize = logBufferSize * 2;
        byte[] emptyBuffer = new byte[bufferSize];
        int nWrites = amountToWrite/bufferSize;
        int remainingBytes = amountToWrite % bufferSize;
        while(nWrites-- > 0)
                theLog.write(emptyBuffer);
        if(remainingBytes !=0)
                theLog.write(emptyBuffer , 0 ,remainingBytes);
        //sync the file
        syncFile(theLog);
    } // end of preAllocateNewLogFile


	/**
	 * open the given log file name for writes; if file can not be 
	 * be opened in write sync mode then disable the write sync mode and 
	 * open the file in "rw" mode.
 	 */
	public RandomAccessFile openLogFileInWriteMode(File logFile) throws IOException
	{
		RandomAccessFile log = privRandomAccessFile(logFile, "rwd");
		return log ;
	}

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

    private synchronized RandomAccessFile privRandomAccessFile(File file, String perms)
        throws IOException
    {
		action = 2;
        activeFile = file;
        activePerms = perms;
        try
        {
            return (RandomAccessFile) java.security.AccessController.doPrivileged(this);
        }
        catch (java.security.PrivilegedActionException pae)
        {
            throw (IOException) pae.getException();
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
			return (String[]) java.security.AccessController.doPrivileged(this);
		}
        catch (java.security.PrivilegedActionException pae)
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
			return ((Boolean) java.security.AccessController.doPrivileged(this)).booleanValue();
		}
        catch (java.security.PrivilegedActionException pae)
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

		try {
			return ((Boolean) java.security.AccessController.doPrivileged(this)).booleanValue();
		} catch (java.security.PrivilegedActionException pae) {
			return false;
		}
	}

    /** set the endPosition of the log and make sure the new position won't spill off the end of the log */
    private void setEndPosition( long newPosition )
    {
		if (DEBUG)
        {
			assert(newPosition < LogCounter.MAX_LOGFILE_SIZE) :
							 "log file would spill past its legal end if the end were set to = " + newPosition;
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
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getCanonicalLogPath() {
		return SessionManager.getDbPath();
	}

	public String getDataDirectory() {
		return dataDirectory;
	}

	public void setDataDirectory(String dataDirectory) {
		this.dataDirectory = dataDirectory;
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



}
