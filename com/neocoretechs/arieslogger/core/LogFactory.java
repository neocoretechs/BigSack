/*

    - Class com.neocoretechs.arieslogger.core.impl.LogFactory

 */

package com.neocoretechs.arieslogger.core;

import com.neocoretechs.arieslogger.logrecords.Corruptable;
import com.neocoretechs.arieslogger.logrecords.ScanHandle;

import java.io.File;
import java.io.IOException;
import java.util.Set;

public interface LogFactory extends Corruptable {

	/**
		The name of a runtime property in the service set that defines any runtime
		attributes a log factory should have. It is (or will be) a comma separated list
		of attributes.
		At the moment only one attribute is known and checked for.
	*/
	public static final String RUNTIME_ATTRIBUTES =  "storage.log";

	/**
		An attribute that indicates the database is readonly
	*/
	public static final String RT_READONLY = "readonly";

	/**
		The name of the default log directory.
	 */
	public static final String LOG_DIRECTORY_NAME = "log";

	public static final String MODULE = "com.neocoretechs.arieslogger.core.impl.LogFactory";

	public Logger getLogger() throws IOException;


	/**
		Recover the database to a consistent state using the log. 
		Each implementation of the log factory has its own recovery algorithm,
		please see the implementation for a description of the specific
		recovery algorithm it uses.

		@param dataFactory - the data factory
		@param transactionFactory - the transaction factory

		@exception Exception - encounter exception while recovering.
	 */
	public void recover() throws IOException;

	/**
		Checkpoint the rawstore.

		@param wait - if true waits for any existing checkpoint to complete 
                         and then executes and waits for another checkpoint.
                      if false if another thead is executing a checkpoint 
                      routine will return immediately.

		@return true if checkpoint is successful,  Will return false if wait
                is false and the routine finds another thread executing a 
                checkpoint.
	 * @throws IllegalAccessException 

		@exception Exception - exception while doing checkpoint.
	*/
	public boolean checkpoint(boolean wait) throws IOException, IllegalAccessException;

	/**
		Flush all unwritten log record up to the log instance indicated to disk.
		@exception Exception cannot flush log file due to sync error
	*/
	public void flush() throws IOException;

	/**
		Get a LogScan to scan flushed records from the log.
		<P> MT- read only
		@param startAt - the LogInstance where we start our scan. null means
		start at the beginning of the log. This function raises an error
		if startAt is a LogInstance which is not in the log.
		@return the LogScan.
		@exception Exception Standard  error policy
	    NOTE: This will be removed after the LogSniffer Rewrite.
	*/
	LogScan openForwardFlushedScan(LogInstance startAt) throws IOException;

	/**
	    Get a ScanHandle to scan flushed records from the log.
		<P> MT- read only
		@param startAt - the LogInstance where we start our scan. null means
		start at the beginning of the log. This function raises an error
		if startAt is a LogInstance which is not in the log.
		@param groupsIWant - log record groups the scanner wants.
		@return the LogScan.
		@exception Exception Standard  error policy
		*/
	ScanHandle openFlushedScan() throws IOException;

	/**
		Get a LogScan to scan the log in a forward direction.
		<P> MT- read only
		@param startAt - the LogInstance where we start our scan. null means
		start at the beginning of the log. This function raises an error
		if startAt is a LogInstance which is not in the log.
		@param stopAt - the LogInstance where we stop our scan. null means
		stop at the end of the log. This function raises an error
 		if stopAt is a LogInstance which is not in the log.
		@return the LogScan.

		@exception Exception Standard  error policy
	*/
	LogScan openForwardScan(LogInstance startAt,LogInstance stopAt) throws IOException;
    /**
	  Get the instance for the last record in the log.
	  */
    LogInstance getFirstUnflushedInstance();

    /**
     * Get the log instance long value of the first log record that has not 
     * been flushed. Only works after recover() has finished, or (if in slave 
     * replication mode) after calling initializeReplicationSlaveRole.
     *
     * @return the log instance long value of the first log record that has not 
     * been flushed
     */
    public long getFirstUnflushedInstanceAsLong();

	/**
		Backup restore support
	 */

	/**
		Stop making any change to the persistent store
		@exception Exception Standard  exception policy.
	 */
	public void freezePersistentStore() throws IOException;
		 
	/**
		Can start making change to the persistent store again
		@exception Exception Standard  exception policy.
	 */
	public void unfreezePersistentStore() throws IOException;

	/**
	   checks whether is log archive mode is enabled or not.
	   @return true if the log is being archived.
	*/
	public boolean logArchived();
        
	/**
		Get JBMS properties relevant to the log factory
		@exception Exception Standard  Error Policy
	 */
	public void getLogFactoryProperties(Set set)  throws IOException;

	 /**
		Return the location of the log directory.
		@exception Exception Standard  Error Policy
	  */
	public File getLogDirectory() throws IOException;

	 /**
		Return the canonical directory of the PARENT of the log directory.  The
		log directory live in the "log" subdirectory of this path.  If the log
		is at the default location (underneath the database directory), this
		returns null.  Should only be called after the log factory is booted.
	  */
	public String getCanonicalLogPath();


	/*
	 * Enable the log archive mode, when log archive mode is 
	 * on the system keeps all the old log files instead
	 * of deleting them at the checkpoint.
	 * logArchive mode is persistent across the boots.
	 * @exception Exception - thrown on error
	*/
	public void enableLogArchiveMode() throws IOException;

		
	/*
	 * Disable the log archive mode, when log archive mode is 
	 * off the system will delete  old log files(not required 
	 * for crash recovery) after each checkpoint. 
	 * @exception Exception - thrown on error
	*/
	public void disableLogArchiveMode() throws IOException;

	/*
	 * Deletes the archived log files store in the log directory path.
	 * This call is typically used after a successful version level
	 * backup to clean up the old log files that are no more
	 * required to do roll-forward recovery from the last
	 * backup taken.
	*/
	public void deleteOnlineArchivedLogFiles() throws IOException;

	//Is the transaction in rollforward recovery
	public boolean inRFR();

	/**	
	 * redoing a checkpoint  during rollforward recovery
	 * @param cinstant The LogInstance of the checkpoint
	 * @param redoLWM  Redo Low Water Mark in the check point record
	 * @param undoLWM Undo Low Water Mark in the checkpoint
	 * @param df - the data factory
	 * @exception Exception - encounter exception during checkpoint
	 */
	public void checkpointInRFR(LogInstance cinstant, long redoLWM, long undoLWM) throws IOException;

	
	/*
	 * start the transaction log backup, the transaction log is  is required
	 * to bring the database to the consistent state on restore. 
	 * copies the log control information , active log files to the given 
	 * backup directory and marks that backup is in progress.
     * @param toDir - location where the log files should be copied to.
     * @exception Exception Standard  error policy
	*/
	public void startLogBackup(File toDir) throws IOException;

	
	/*
	 * copy all the log files that has to go into the backup directory
	 * and mark that backup has come to an end. 
     * @param toDir - location where the log files should be copied to.
     * @exception Exception Standard  error policy
	*/
	public void endLogBackup(File toDir) throws Exception;

	
	/*
	 * Abort any activity related to backup in the log factory.
	 * Backup is not in progress any more, it failed for some reason.
	 **/
	public void abortLogBackup();
    
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
    public boolean isCheckpointInLastLogFile() throws IOException;
    
    /*
     * delete the log file after the checkpoint. 
     *
     * <P>MT - synchronization provided by caller - RawStore boot,
     * This method is called only if a crash occured while 
     * re-encrypting the database at boot time. 
     */
    public void deleteLogFileAfterCheckpointLogFile() throws IOException;

    
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
     * @exception  Exception 
     *             if the database is not at the require version 
     *             when <code>feature</code> feature is 
     *             not <code> null </code>. 
     */
	public boolean checkVersion(int requiredMajorVersion, 
                                int requiredMinorVersion, 
                                String feature) 
        throws IOException;

}

