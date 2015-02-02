/*

    - Class com.neocoretechs.arieslogger.core.FlushedScan

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
import com.neocoretechs.arieslogger.core.StreamLogScan;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

/**

	Scan the the log which is implemented by a series of log files.n
	This log scan knows how to move across log file if it is positioned at
	the boundary of a log file and needs to getNextRecord.

	<PRE>
	4 bytes - length of user data, i.e. N
	8 bytes - long representing log instance
	N bytes of supplied data
	4 bytes - length of user data, i.e. N
	</PRE>

*/
public class FlushedScan implements StreamLogScan {

	private RandomAccessFile scan;		// an output stream to the log file
	private LogToFile logFactory; 				// log factory knows how to to skip
										// from log file to log file

	private boolean open;						// true if the scan is open

	private long currentLogFileNumber; 			// the log file the scan is currently on

	private long currentLogFileFirstUnflushedPosition;
	                                    // The length of the unflushed portion
										// of the current log file. This is the
										// length of the file for all but the
										// last log file.

	private long currentInstance;				// the log instance the scan is
										// currently on - only valid after a
										// successful getNextRecord

	private long firstUnflushed = -1;			// scan until we reach the first
										// unflushed byte in the log.
	private long firstUnflushedFileNumber;
	private long firstUnflushedFilePosition;

	/**
	  The length of the next record. Read from scan and set by
	  currentLogFileHasUnflushedRecord. This is used to retain the length of a
	  log record in the case currentLogFileHasUnflushedRecord reads the length
	  and determines that some bytes in the log record are not yet flushed.
	  */
	int nextRecordLength;

	/**
	  Flag to indicate that the length of the next log record has been read by
	  currentLogFileHasUnflushedRecord.

	  This flag gets reset in two ways:

	  <OL>
	  <LI> currentLogFileHasUnflushedRecord determines that the entire log
	  record is flushed and returns true. In this case getNextRecord reads and
	  returns the log record.
	  <LI> we switch log files --due to a partial log record at the end of an
	  old log file.
	  </OL>
	  */
	boolean readNextRecordLength;
	
	//RESOLVE: This belongs in a shared place.
	static final int LOG_REC_LEN_BYTE_LENGTH = 4;
	private static final boolean DEBUG = false;

	public FlushedScan(LogToFile logFactory, long startAt) throws IOException {
        if (DEBUG)
        {
    		assert(startAt != LogCounter.INVALID_LOG_INSTANCE) : "cannot start scan on an invalid log instance";
	    }
		setCurrentLogFileNumber(LogCounter.getLogFileNumber(startAt));
		this.setLogFactory(logFactory);
		scan =  logFactory.getLogFileAtPosition(startAt);
		setFirstUnflushed();
		setOpen(true);
		setCurrentInstance(LogCounter.INVALID_LOG_INSTANCE); // set at getNextRecord
	}

	/*
	** Methods of LogScan
	*/

	/**
		Read a log record into the byte array provided.  Resize the input
		stream byte array if necessary.
		@return the length of the data written into data, or -1 if the end of the
		scan has been reached.
		@exception IOException
	*/
	public HashMap<LogInstance, LogRecord> getNextRecord(int groupmask) throws IOException {
			boolean candidate;
			LogRecord lr;

			do
			{
				if (!isOpen() || !positionToNextRecord()) 
					return null;

				// this log record is a candidate unless proven otherwise
				lr = null;
				candidate = true;

				setCurrentInstance(scan.readLong());
				byte[] data = new byte[nextRecordLength];

				scan.readFully(data, 0, nextRecordLength);
				// put the data to 'input'
				ByteBuffer input = ByteBuffer.wrap(data);
				
				lr = (LogRecord)(GlobalDBIO.deserializeObject(input));


				if (Scan.multiTrans && groupmask != 0 && (groupmask & lr.group()) == 0)
						candidate = false; // no match, throw this log record out 

				if (!candidate)
				{
					// the starting record position is in the currentInstance,
					// calculate the next record starting position using that
					// and the nextRecordLength
					long nextRecordStartPosition =
						LogCounter.getLogFilePosition(getCurrentInstance()) +
						nextRecordLength + LogToFile.LOG_RECORD_OVERHEAD;

					scan.seek(nextRecordStartPosition);
				}

			} while (candidate == false);
			HashMap<LogInstance, LogRecord> retLog = new HashMap<LogInstance, LogRecord>();
			retLog.put(new LogCounter(currentInstance), lr);
			return retLog;
	}

	/**
		Reset the scan to the given LogInstance.
		@param instance the position to reset to
		@exception IOException scan cannot access the log at the new position.
	*/
	public void resetPosition(LogInstance instance) throws IOException
	{
        if (DEBUG)
        {
    		throw new IOException("Unsupported feature");
    	}
	}

	/**
		Get the log instance that is right after the record just retrived
		@return INVALID_LOG_INSTANCE if this is not a FORWARD scan or, no
		record have been returned yet or the scan has completed.
	*/
	public long getLogRecordEnd()
	{
		return LogCounter.INVALID_LOG_INSTANCE;
	}

	
	/**
	   returns true if there is partially writen log records before the crash 
	   in the last log file. Partiall wrires are identified during forward 
	   scans for log recovery.
	 */
	public boolean isLogEndFuzzy()
	{
		return false;
	}

	/**
		Return the log instance (as an integer) the scan is currently on - this is the log
		instance of the log record that was returned by getNextRecord.
	*/
	public long getLogInstanceAsLong()
	{
		return getCurrentInstance();
	}

	/**
		Return the log instance the scan is currently on - this is the log
		instance of the log record that was returned by getNextRecord.
	*/
	public LogInstance getLogInstance()
	{
		if (getCurrentInstance() == LogCounter.INVALID_LOG_INSTANCE)
			return null;
		else
			return new LogCounter(getCurrentInstance());
	}

	/**
		Close the scan.
	*/
	public void close()
	{
		if (scan != null)
		{
			try
			{
				scan.close();
			}
			catch (IOException ioe)
			{}

			scan = null;
		}
		setCurrentInstance(LogCounter.INVALID_LOG_INSTANCE);
		setOpen(false);
	}

	/*
	  Private methods.
	  */
	private void setFirstUnflushed() throws IOException
	{
		LogInstance firstUnflushedInstant = getLogFactory().getFirstUnflushedInstance();
		firstUnflushed = ((LogCounter)firstUnflushedInstant).getValueAsLong();
		setFirstUnflushedFileNumber(LogCounter.getLogFileNumber(firstUnflushed));
		setFirstUnflushedFilePosition(LogCounter.getLogFilePosition(firstUnflushed));

		setCurrentLogFileFirstUnflushedPosition();
	}

	private void setCurrentLogFileFirstUnflushedPosition() throws IOException
	{
		/*
		  Note we get the currentLogFileLength without synchronization.
		  This is safe because one of the following cases apply:

		  <OL>
		  <LI> The end of the flushed section of the log is in another file.
		  In this case the end of the current file will not change.
		  <LI> The end of the log is in this file. In this case we
		  end our scan at the firstUnflushedInstant and do not use
		  currentLogFileLength.
		  </OL>
		  */
		if (getCurrentLogFileNumber() == getFirstUnflushedFileNumber())
			currentLogFileFirstUnflushedPosition = getFirstUnflushedFilePosition();
		else if (getCurrentLogFileNumber() < getFirstUnflushedFileNumber())
			currentLogFileFirstUnflushedPosition = scan.length();
		else
        {
			// RESOLVE 
		   	throw new IOException("Log bad start");
		}
	}

	private void switchLogFile() throws IOException {

			readNextRecordLength = false;
			scan.close();
			scan = null;
			scan = getLogFactory().getLogFileAtBeginning(setCurrentLogFileNumber(getCurrentLogFileNumber() + 1));
			setCurrentLogFileFirstUnflushedPosition();
	}

	private boolean currentLogFileHasUnflushedRecord() throws IOException
	{
		if (DEBUG)
			assert(scan != null);//, "scan is null");
		long curPos = scan.getFilePointer();

		if (!readNextRecordLength)
		{
			if (curPos + LOG_REC_LEN_BYTE_LENGTH >
				                 getCurrentLogFileFirstUnflushedPosition())
				return false;

			nextRecordLength = scan.readInt();
			curPos+=4;
			readNextRecordLength = true;
		}

		if (nextRecordLength==0) return false;

		int bytesNeeded =
			nextRecordLength + LOG_REC_LEN_BYTE_LENGTH;

		if (curPos + bytesNeeded > getCurrentLogFileFirstUnflushedPosition())
		{
			return false;
		}
		else
		{
			readNextRecordLength = false;
			return true;
		}
	}

	private boolean positionToNextRecord() throws IOException
	{
		//If the flushed section of the current log file contains our record we
		//simply return.
		if (currentLogFileHasUnflushedRecord()) return true;

		//Update our cached copy of the first unflushed instance.
		setFirstUnflushed();

		//In the call to setFirstUnflushed, we may have noticed that the current
		//log file really does contain our record. If so we simply return.
		if (currentLogFileHasUnflushedRecord()) return true;

		//Our final chance of finding a record is if we are not scanning the log
		//file with the last flushed instance we can switch logfiles. Note that
		//we do this in a loop to cope with empty log files.
		while(getCurrentLogFileNumber() < getFirstUnflushedFileNumber())
		{
			  switchLogFile();
		      if (currentLogFileHasUnflushedRecord()) return true;
		}

		//The log contains no more flushed log records so we return false.
		setCurrentInstance(LogCounter.INVALID_LOG_INSTANCE);
		return false;
	}

	public long getCurrentLogFileNumber() {
		return currentLogFileNumber;
	}

	public long setCurrentLogFileNumber(long currentLogFileNumber) {
		this.currentLogFileNumber = currentLogFileNumber;
		return currentLogFileNumber;
	}

	public boolean isOpen() {
		return open;
	}

	public void setOpen(boolean open) {
		this.open = open;
	}

	public long getCurrentLogFileFirstUnflushedPosition() {
		return currentLogFileFirstUnflushedPosition;
	}

	public void setCurrentLogFileFirstUnflushedPosition(
			long currentLogFileFirstUnflushedPosition) {
		this.currentLogFileFirstUnflushedPosition = currentLogFileFirstUnflushedPosition;
	}

	public long getCurrentInstance() {
		return currentInstance;
	}

	public void setCurrentInstance(long currentInstance) {
		this.currentInstance = currentInstance;
	}

	public long getFirstUnflushed() {
		return firstUnflushed;
	}

	public void setFirstUnflushed(long firstUnflushed) {
		this.firstUnflushed = firstUnflushed;
	}

	public long getFirstUnflushedFileNumber() {
		return firstUnflushedFileNumber;
	}

	public void setFirstUnflushedFileNumber(long firstUnflushedFileNumber) {
		this.firstUnflushedFileNumber = firstUnflushedFileNumber;
	}

	public long getFirstUnflushedFilePosition() {
		return firstUnflushedFilePosition;
	}

	public void setFirstUnflushedFilePosition(long firstUnflushedFilePosition) {
		this.firstUnflushedFilePosition = firstUnflushedFilePosition;
	}

	public LogToFile getLogFactory() {
		return logFactory;
	}

	public void setLogFactory(LogToFile logFactory) {
		this.logFactory = logFactory;
	}

	@Override
	public void checkFuzzyLogEnd() throws IOException {
		// TODO Auto-generated method stub
		
	}
}
