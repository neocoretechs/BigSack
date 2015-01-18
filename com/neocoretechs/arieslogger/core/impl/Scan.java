/*

    - Class com.neocoretechs.arieslogger.core.Scan

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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import com.neocoretechs.arieslogger.core.LogInstance;
import com.neocoretechs.arieslogger.core.StreamLogScan;
import com.neocoretechs.arieslogger.logrecords.Loggable;

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

public class Scan implements StreamLogScan {

	// value for scanDirection
	public static final byte FORWARD = 1;
	public static final byte BACKWARD = 2;
	public static final byte BACKWARD_FROM_LOG_END = 4;
	private static final boolean DEBUG = true;

	private RandomAccessFile scan;		// an output stream to the log file
	private LogToFile logFactory; 		// log factory knows how to to skip
										// from log file to log file

	private long currentLogFileNumber; 	// the log file the scan is currently on

	private long currentLogFileLength;	// the size of the current log file
										// used only for FORWARD scan to determine when
										// to switch the next log file

	private long knownGoodLogEnd; // For FORWARD scan only
								// during recovery, we need to determine the end
								// of the log.  Everytime a complete log record
								// is read in, knownGoodLogEnd is set to the
								// log instance of the next log record if it is
								// on the same log file.
								// 
								// only valid afer a successfull getNextRecord
								// on a FOWARD scan. 


	private long currentInstance;		// the log instance the scan is
										// currently on - only valid after a
										// successful getNextRecord

	private long stopAt;				// scan until we find a log record whose 
										// log instance < stopAt if we scan BACKWARD
										// log instance > stopAt if we scan FORWARD
										// log instance >= stopAt if we scan FORWARD_FLUSHED


	private byte scanDirection; 		// BACKWARD or FORWARD

	private boolean fuzzyLogEnd = false;   //get sets to true during forward scan
 	                                      //for recovery, if there were
	                                      //partial writes at the end of the log before crash;
	                                      //during forward scan for recovery.


	/**
	    For backward scan, we expect a scan positioned at the end of the next log record.
		For forward scan, we expect a scan positioned at the beginning of the next log record.

		For forward flushed scan, we expect stopAt to be the instance for the
		   first not-flushed log record. Like any forward scan, we expect a scan
		   positioned at the beginning of the next log record.

		@exception StandardException Standard  error policy
		@exception IOException cannot access the log at the new position.
	*/
	public Scan(LogToFile logFactory, long startAt, LogInstance stopAt, byte direction) throws IOException
	{
		if (DEBUG)
			assert startAt == LogCounter.INVALID_LOG_INSTANCE : "cannot start scan on an invalid log instance";

		this.logFactory = logFactory;
		currentLogFileNumber = LogCounter.getLogFileNumber(startAt);
		currentLogFileLength = -1;
		knownGoodLogEnd = LogCounter.INVALID_LOG_INSTANCE;// set at getNextRecord for FORWARD scan
		currentInstance = LogCounter.INVALID_LOG_INSTANCE; // set at getNextRecord
		if (stopAt != null)
			this.stopAt = ((LogCounter) stopAt).getValueAsLong();
		else
			this.stopAt = LogCounter.INVALID_LOG_INSTANCE;

		switch(direction) {
			case FORWARD:
				scan =  logFactory.getLogFileAtPosition(startAt);
				scanDirection = FORWARD;
				if (DEBUG)
					if (scan == null)
						throw new IOException("scan null at " + LogCounter.toDebugString(startAt));

			// NOTE: just get the length of the file without syncing.
			// this only works because the only place forward scan is used
			// right now is on recovery redo and nothing is being added to 
			// the current log file.  When the forward scan is used for some
			// other purpose, need to sync access to the end of the log
				currentLogFileLength = scan.length();
				break;

			case BACKWARD:
			// startAt is at the front of the log record, for backward
			// scan we need to be positioned at the end of the log record
				scan =  logFactory.getLogFileAtPosition(startAt);
				int logsize = scan.readInt();

			// skip forward over the log record and all the overhead, but remember
			// we just read an int off the overhead
				scan.seek(scan.getFilePointer() + logsize + LogToFile.LOG_RECORD_OVERHEAD - 4);
				scanDirection = BACKWARD;
				break;

			case BACKWARD_FROM_LOG_END:
			// startAt is at the end of the log, no need to skip the log record
				scan =  logFactory.getLogFileAtPosition(startAt);
				scanDirection = BACKWARD;
				break;

		}
		if( DEBUG ) {
			System.out.println("Scan: "+logFactory.getDBName()+" dir:"+scanDirection+" start:"+ LogCounter.toDebugString(startAt)+" stop:"+stopAt+" current#:"+currentLogFileNumber+" currLen:"+currentLogFileLength);
		}
	}

	/*
	** Methods of StreamLogScan
	*/

	/**
		Read the next log record.
		Switching log to a previous log file if necessary, 
		Resize the input stream byte array if necessary.  
		@see StreamLogScan#getNextRecord

		@return the next LogRecord, or null if the end of the
		scan has been reached.

		@exception StandardException Standard  error policy
	*/
	public LogRecord getNextRecord(ByteBuffer input,  long tranId,  int groupmask) throws IOException
	{
		if (scan == null)
			return null;

		LogRecord lr = null;
		try
		{
			if (scanDirection == BACKWARD)
				lr = getNextRecordBackward(input, tranId, groupmask);
			else if (scanDirection == FORWARD)
				lr = getNextRecordForward(input, tranId, groupmask);

			return lr;

		}
		catch (IOException ioe)
		{
			if (DEBUG)
				ioe.printStackTrace();

			throw logFactory.markCorrupt(ioe);
		}
		catch (ClassNotFoundException cnfe)
		{
			if (DEBUG)
				cnfe.printStackTrace();

			throw logFactory.markCorrupt(new IOException(cnfe));
		}
		finally
		{
			if (lr == null)
				close();		// no more log record, close the scan
		}

	}

	/**
		Read the previous log record.
		Switching log to a previous log file if necessary, 
		Resize the input stream byte array if necessary.  
		@see StreamLogScan#getNextRecord

		Side effects include: 
				on a successful read, setting currentInstance.
				on a log file switch, setting currentLogFileNumber.

		@return the previous LogRecord, or null if the end of the
		scan has been reached.
	*/
	private LogRecord getNextRecordBackward(ByteBuffer input, 
									  long tranId,  
									  int groupmask) throws IOException, ClassNotFoundException
	{
		if (DEBUG)
			assert scanDirection == BACKWARD : "can only called by backward scan";

		// scan is positioned just past the last byte of the record, or
		// right at the beginning of the file (end of the file header)
		// may need to switch log file

		boolean candidate;
		int readAmount;			// the number of bytes actually read

		LogRecord lr;
		long curpos = scan.getFilePointer();

		do
		{
			// this log record is a candidate unless proven otherwise
			candidate = true; 
			lr = null;
			readAmount = -1;

			if (curpos == LogToFile.LOG_FILE_HEADER_SIZE)
			{
				// don't go thru the trouble of switching log file if we
				// will have gone past stopAt
				if (stopAt != LogCounter.INVALID_LOG_INSTANCE &&
					LogCounter.getLogFileNumber(stopAt) == currentLogFileNumber)
				{
					if (DEBUG)
                    {
                            System.out.println( "stopping at " + currentLogFileNumber);
                    }

					return null;  // no more log record
				}
				
				// figure out where the last log record is in the previous
				// log file
				scan.seek(LogToFile.LOG_FILE_HEADER_PREVIOUS_LOG_INSTANCE_OFFSET);
				long previousLogInstance = scan.readLong();
				scan.close();

				if (DEBUG)
				{
					assert(previousLogInstance != LogCounter.INVALID_LOG_INSTANCE);//,
									 //"scanning backward beyond the first log file");
					if (currentLogFileNumber != 
							LogCounter.getLogFileNumber(previousLogInstance) + 1)
						throw new IOException(
						"scanning backward but get incorrect log file number " + 
						 "expected " + (currentLogFileNumber -1) + 
						 "get " +
						 LogCounter.getLogFileNumber(previousLogInstance));

					assert(LogCounter.getLogFilePosition(previousLogInstance) > 
									 LogToFile.LOG_FILE_HEADER_SIZE);//,
									 //"scanning backward encounter completely empty log file");

					System.out.println(
									"scanning backwards from log file " +
									currentLogFileNumber + ", switch to (" + 
									LogCounter.getLogFileNumber(previousLogInstance) + "," +
									LogCounter.getLogFilePosition(previousLogInstance) + ")"
									);
				}

				// log file switch, set this.currentLogFileNumber
				currentLogFileNumber = LogCounter.getLogFileNumber(previousLogInstance);

				scan = logFactory.getLogFileAtPosition(previousLogInstance);

				// scan is located right past the last byte of the last log
				// record in the previous log file.  currentLogFileNumber is
				// set.  We asserted that the scan is not located right at the
				// end of the file header, in other words, there is at least
				// one log record in this log file.
				curpos = scan.getFilePointer();

				// if the log file happens to be empty skip and proceed. 
				// ideally this case should never occur because log switch is
				// not suppose to happen on an empty log file. 
				// But it is safer to put following check incase if it ever
				// happens to avoid any recovery issues. 
				if (curpos == LogToFile.LOG_FILE_HEADER_SIZE)
					continue;
			}

			scan.seek(curpos - 4);
			int recordLength = scan.readInt(); // get the length after the log record

			// calculate where this log record started.
			// include the eight bytes for the long log instance at the front
			// the four bytes of length in the front and the four bytes we just read
			long recordStartPosition = curpos - recordLength - LogToFile.LOG_RECORD_OVERHEAD; 

			if (DEBUG)
			{
				if (recordStartPosition < LogToFile.LOG_FILE_HEADER_SIZE)
					throw new IOException(
								 "next position " + recordStartPosition +
								 " recordLength " + recordLength + 
								 " current file position " + scan.getFilePointer());

				scan.seek(recordStartPosition);

				// read the length before the log record and check it against the
				// length after the log record
				int checkLength = scan.readInt();

				if (checkLength != recordLength)
				{
					long inst = LogCounter.makeLogInstanceAsLong(currentLogFileNumber, recordStartPosition);

					throw logFactory.markCorrupt(
                        new IOException("Corrupt:"+
                            new Long(checkLength)+" "+
                            new Long(recordLength)+" "+
                            new Long(inst)+" "+
                            new Long(currentLogFileNumber)));
				}
			}
			else
			{
				// skip over the length
				scan.seek(recordStartPosition+4);
			}

			// scan is positioned just before the log instance
			// read the current log instance - this is the currentInstance if we have not
			// exceeded the scan limit
			currentInstance = scan.readLong();

			if (DEBUG)
			{
				// sanity check the current instance against the scan position
				if (LogCounter.getLogFileNumber(currentInstance) !=
					currentLogFileNumber ||
					LogCounter.getLogFilePosition(currentInstance) !=
					recordStartPosition)
					throw new IOException(
								 "Wrong LogInstance on log record " +
								LogCounter.toDebugString(currentInstance) + 
								 " version real position (" +
								 currentLogFileNumber + "," +
								 recordStartPosition + ")");
			}

			// if stopAt == INVALID_LOG_INSTANCE, no stop instance, read till
			// nothing more can be read.  Else check scan limit
			if (currentInstance < stopAt && stopAt != LogCounter.INVALID_LOG_INSTANCE)
			{
				currentInstance = LogCounter.INVALID_LOG_INSTANCE;
				return null;	// we went past the stopAt
			}

			byte[] data =  new byte[recordLength];

			scan.readFully(data, 0, recordLength);
			
			// put the data to 'input'
			input.put(data);
			
			lr = (LogRecord)(new ObjectInputStream(new ByteArrayInputStream(input.array())).readObject());

			// skip the checksum log records, there is no need to look at them 
			// during backward scans. They are used only in forwardscan during recovery. 
			if(lr.isChecksum()) {
				candidate = false; 
			} else {
				if (groupmask != 0 || tranId != -1) {

				// skip the checksum log records  
				if(lr.isChecksum())
					candidate = false; 

				if (candidate && groupmask != 0 && (groupmask & lr.group()) == 0)
					candidate = false; // no match, throw this log record out 

				if (candidate && tranId != -1)
				{
					long tid = lr.getTransactionId();
					if (tid != tranId) // nomatch
						candidate = false; // throw this log record out
				}

				}
			}

			// go back to the start of the log record so that the next time
			// this method is called, it is positioned right past the last byte
			// of the record.
			curpos = recordStartPosition;
			scan.seek(curpos);

		} while (candidate == false);

		return lr;

	}

	/**
		Read the next log record.
		Switching log to a previous log file if necessary, 
		Resize the input stream byte array if necessary.  
		@see StreamLogScan#getNextRecord

		Side effects include: 
				on a successful read, setting currentInstance, knownGoodLogEnd
				on a log file switch, setting currentLogFileNumber, currentLogFileLength.
				on detecting a fuzzy log end that needs clearing, it will call
				logFactory to clear the fuzzy log end.

		@return the next LogRecord, or null if the end of the
		scan has been reached.
	*/
	private LogRecord getNextRecordForward(ByteBuffer input, 
									 long tranId,  
									 int groupmask) throws IOException, ClassNotFoundException
	{
		if (DEBUG) {
			assert(scanDirection == FORWARD) : "can only called by forward scan";
			System.out.println("Scan.getNextRecordForward: entering with file pos:"+scan.getFilePointer());
		}
		// NOTE:
		//
		// if forward scan, scan is positioned at the first byte of the
		// next record, or the end of file - note the the 'end of file'
		// is defined at the time the scan is initialized.  If we are
		// on the current log file, it may well have grown by now...
		//
		// This is not a problem in reality because the only forward
		// scan on the log now is recovery redo and the log does not
		// grow.  If in the future, a foward scan of the log is used
		// for some other reasons, need to keep this in mind.
		//

		// first we need to make sure the entire log record is on the
		// log, or else this is a fuzzy log end.

		// RESOLVE: can get this from knownGoodLogEnd if this is not the first
		// time getNext is called.  Probably just as fast to call
		// scan.getFilePointer though...
		long recordStartPosition = scan.getFilePointer();

		boolean candidate;
		LogRecord lr;

		do
		{
			// this log record is a candidate unless proven otherwise
			candidate = true;
			lr = null;
			if( DEBUG ) {
				System.out.println("Scane.getNextRecordForward resordStartPosition:"+recordStartPosition+" currentLogFileLength:"+currentLogFileLength);
			}
			if(recordStartPosition == currentLogFileLength) {
				// since there is no end of log file marker, we are at the
				// end of the log.
				if (DEBUG) {
			           System.out.println("Scan.getNextRecordForward:detected normal end on log file "); 
                }
				return null;
			}
			// if we are not right at the end but this position + 4 is at
			// or exceeds the end, we know we don't have a complete log
			// record.  This is the log file and chalk it up as the fuzzy
			// end.
			if (recordStartPosition > currentLogFileLength)
			{
				// since there is no end of log file marker, we are at the
				// end of the log.
				if (DEBUG)
                {
                        System.out.println("Scan.getNextRecordForward:detected fuzzy log end on log file " + 
                                currentLogFileNumber + 
                            " record start position " + recordStartPosition + 
                            " file length " + currentLogFileLength);
             
                }
				fuzzyLogEnd = true ;
				// don't bother to write the end of log file marker because
				// if it is not overwritten by the next log record then
				// the next time the database is recovered it will come
				// back right here
				return null;
			}

			// read in the length before the log record
			int recordLength = scan.readInt();
			if( DEBUG )
				System.out.println("Scan.getNextRecordForward: record len from scan:"+recordLength+" recordStartPosition:"+recordStartPosition+" currentLogFileLength:"+currentLogFileLength);

			// recordLength == 0

			if (DEBUG) {
                        if (recordStartPosition + 4 == currentLogFileLength)
                        {
                            System.out.println("Scan.getNextRecordForward:detected proper log end on log file " + 
                                currentLogFileNumber);
                        }
                        else
                        {
                            System.out.println("Scan.getNextRecordForward:detected problem log end on log file " + 
                                        currentLogFileNumber +
                                    " end marker at " + 
                                        recordStartPosition + 
                                    " real end at " + currentLogFileLength);
                        }
			}
				
			// record 0, we have to move to next file
	
			/**********************************************
			 * Increment the current log
			 */
			if(recordLength == 0) {
				LogCounter li = new LogCounter(++currentLogFileNumber,LogToFile.LOG_FILE_HEADER_PREVIOUS_LOG_INSTANCE_OFFSET);
				resetPosition(li);
				if (scan == null) { // we have seen the last log file
					if( DEBUG )
						System.out.println("Scan.getNextRecordForward "+currentLogFileNumber+" passed last file, returning null");
					return null;
				}
				// Verify that the header of the new log file refers
				// to the end of the log record of the previous file
				// (Rest of header has been verified by getLogFileAtBeginning)
				long previousLogInstance = scan.readLong();
            	if (previousLogInstance != knownGoodLogEnd) {
                    // If there is a mismatch, something is wrong and
                    // we return null to stop the scan.  The same
                    // behavior occurs when getLogFileAtBeginning
                    // detects an error in the other fields of the header.
                    if (DEBUG) {
                    	System.out.println("Scan.getNextRecordForward: Known Good End mismatch in log file, early scan termination " 
                                                + currentLogFileNumber  
                                                + ": previous log record: "
                                                + previousLogInstance
                                                + " known previous log record: "
                                                + knownGoodLogEnd);
                    }
                    return null;
            	}
    			// scan is position just past the log header
				recordStartPosition = scan.getFilePointer();
				//scan.seek(recordStartPosition);

				if (DEBUG) {
					System.out.println("Scan.getNextRecordForward:switched to next log file " +  currentLogFileNumber+ " at position "+ recordStartPosition);
            	}
				// Set new known end to current log, start record
            	// Advance knownGoodLogEnd to make sure that if this log file is the last log file and empty, logging
            	// continues in this file, not the old file.
            	knownGoodLogEnd = LogCounter.makeLogInstanceAsLong(currentLogFileNumber, recordStartPosition);

				if (recordStartPosition+4 >= currentLogFileLength) // empty log file
				{
					if (DEBUG)
                    {
                            System.out.println( "Scan.getNextRecordForward:log file " + currentLogFileNumber + " is empty");
                    }

					return null;
				}

				// we have successfully switched to the next log file.
				// scan is positioned just before the next log record
				// see if this one is written in entirety
				recordLength = scan.readInt();	
			} // recordLength == 0
			
			/****************************
			 * we know the entire log record is on this log file
			 * recordStartPosition should point to last record
			 */
			// read the current log instance
			currentInstance = scan.readLong();
			// currentInstance is coming from potential newly opened file

			// sanity check it 
			if (DEBUG)
			{
				if (LogCounter.getLogFileNumber(currentInstance) !=
					currentLogFileNumber ||
					LogCounter.getLogFilePosition(currentInstance) !=
					recordStartPosition)
					//throw new IOException(
					System.out.println(
							  "Wrong LogInstance on log record " +
								LogCounter.toDebugString(currentInstance) + 
								 " version real position (" +
								 currentLogFileNumber + "," +
								 recordStartPosition + ")");
				System.out.println("Scan.getNextRecordForward: Current instance: "+
								 LogCounter.toDebugString(currentInstance));
			}


			// read in the log record
			byte[] data = new byte[recordLength];

			// If the log is encrypted, we must do the filtering after
			// reading  the record.
	
			//if (groupmask == 0 && tranId == -1)
			//{
					// no filter, get the whole thing
			if( DEBUG ) {
				System.out.println("Scan.getNextRecordForward reading "+recordLength+" @ "+scan.getFilePointer());
			}
			
			scan.readFully(data, 0, recordLength);
			
			if( DEBUG ) {
				System.out.println("Scan.getNextRecordForward read ended @"+scan.getFilePointer());
			}
					//input.setLimit(0, recordLength);
			//}
			//else
			//{
					// Read only enough so that group and the tran id is in
					// the data buffer.  Group is stored as compressed int
					// and tran id is stored as who knows what.  read min
					// of peekAmount or recordLength
					//readAmount = (recordLength > peekAmount) ? peekAmount : recordLength; 

					// in the data buffer, we now have enough to peek
					//scan.readFully(data, 0, readAmount);
					//input.setLimit(0, readAmount);
			//}
			if( input.limit() < data.length ) {
				input = ByteBuffer.allocate(data.length);
			} else {
				input.clear();
			}
			// put the data to 'input'
			input.put(data);
			
			if( DEBUG ) {
				System.out.println("Scan.getNextRecordForward: put data, new buffer position:"+input.position()+" new file pos:"+scan.getFilePointer());
			}
			
			lr = (LogRecord)(new ObjectInputStream(new ByteArrayInputStream(input.array())).readObject());
			
			if( DEBUG ) {
				System.out.println("Scan.getNextRecordForward RecordLength:"+recordLength);
			}
			
			if (groupmask != 0 || tranId != -1)
			{
				if (groupmask != 0 && (groupmask & lr.group()) == 0)
					candidate = false; // no match, throw this log record out 
				if (candidate && tranId != -1)
				{
					long tid = lr.getTransactionId();
					if (tid != tranId) // nomatch
						candidate = false; // throw this log record out
				}
			}
			/*check if the log record length written before and after the 
			 *log record are equal, if not the end of of the log is reached.
			 *This can happen if system crashed before writing the length field 
			 *in the end of the records completely. If the length is partially
			 *written or not written at all  it will not match with length written 
			 *in the beginning of the log record. Currently preallocated files
			 *are filled with zeros, log record length can never be zero; 
			 *if the lengths are not matching, end of the properly written log
			 *is reached.
			 *Note: In case of Non-preallocated files earlier fuzzy case check with log
			 * file lengths should have found the end. But in preallocated files, log file
			 *length is not sufficient to find the log end. This check 
			 *is must to find the end in preallocated log files. 
			 */
			// read the length after the log record and check it against the
			// length before the log record, make sure we go to the correct
			// place for skipped log record.
			//if (!candidate)
			//	scan.seek(recordStartPosition - 4);
			int checkLength = scan.readInt();
			if( checkLength != recordLength ) {
				if( checkLength < recordLength ) {
					fuzzyLogEnd = true ;
					if( DEBUG ) {
						System.out.println("Scan.getNextRecordForward: fuzzy log end detected:"+checkLength+" expected: "+recordLength+" returning...");
					}
					return null;
				} else {				
					//If checklength > recordLength then it can be not be a partial write
					//probably it is corrupted for some reason , this should never
					//happen throw error in debug mode. In non debug case , let's
					//hope it's only is wrong and system can proceed. 
						
					if (DEBUG)
					{	
						throw logFactory.markCorrupt
						(new IOException("Corrupt:"+
                            new Long(checkLength)+" "+
                            new Long(recordLength)+" "+
                            LogCounter.toDebugString(currentInstance)+" "+
                            new Long(currentLogFileNumber)));

					}			
					//In non debug case, do nothing , let's hope it's only
					//length part that is incorrect and system can proceed. 
				}
			}
			// next record start position is right after this record
			recordStartPosition += recordLength + LogToFile.LOG_RECORD_OVERHEAD;
			knownGoodLogEnd = LogCounter.makeLogInstanceAsLong(currentLogFileNumber, recordStartPosition);

			if (DEBUG)
			{
				if (recordStartPosition != scan.getFilePointer())
					throw new IOException("Calculated end " + recordStartPosition + 
									 " != real end " + scan.getFilePointer());
			}
			else
			{
				// seek to the start of the next log record
				scan.seek(recordStartPosition);
			}

			// the scan is now positioned just past this log record and right
			// at the beginning of the next log record
			/** if the current log record is a checksum log record then
			 * using the information available in this record validate
			 * that data in the log file by matching the checksum in 
			 * checksum log record and by recalculating the checksum for the 
			 * specified length of the data in the log file. checksum values
			 * should match unless the right was incomplete before the crash.
			 */
			if(lr.isChecksum())
			{
				// checksum log record should not be returned to the logger recovery redo
				// routines, it is just used to identify the incomplete log writes.

				candidate = false;
				Loggable op = lr.getLoggable(); 
				if (DEBUG)
                {
						System.out.println( "Scan.getNextRecordForward:"+
											"scanned " + op + 
											" instance = " + 
											LogCounter.toDebugString(currentInstance) + 
											" logEnd = " +  LogCounter.toDebugString(knownGoodLogEnd));
				}

				ChecksumOperation clop = (ChecksumOperation) op;
				int ckDataLength =  clop.getDataLength(); 
				// resize the buffer to be size of checksum data length if required.
				if (data.length < ckDataLength)
				{
					// make a new array of sufficient size and reset the array
					// in the input stream
					data = new byte[ckDataLength];
					//input.setData(data);
					//input.setLimit(0, ckDataLength);
				}
				
				boolean validChecksum = false;
				// check if the expected number of bytes by the checksum log
				// record actually exist in the file and then verify if checksum
				// is valid to identify any incomplete out of order writes.
				if((recordStartPosition + ckDataLength) <= currentLogFileLength)
				{
					// read the data into the buffer
					scan.readFully(data, 0, ckDataLength);
					// verify the checksum 
					if(clop.isChecksumValid(data, 0 , ckDataLength))
						validChecksum = true;
				}

				if(!validChecksum)
				{
					// declare that the end of the transaction log is fuzzy, checksum is invalid
					// only when the writes are incomplete; this can happen
					// only when writes at the end of the log were partially
					// written before the crash. 

					if (DEBUG)
                    {
						System.out.println("Scan.getNextRecordForward:"+
                                "detected fuzzy log end on log file while doing checksum checks " + 
								currentLogFileNumber + 
                                " checksum record start position " + recordStartPosition + 
                                " file length " + currentLogFileLength + 
								" checksumDataLength=" + ckDataLength);	
					}
					
					fuzzyLogEnd = true;
					scan.close();
					scan = null;
					return null;
				}

				// reset the scan to the start of the next log record
				scan.seek(recordStartPosition);
			}

		} while (candidate == false) ;

		return lr;
	}


	/**
	 * Works on 'scan' instance
	 * resets currentLogFileNumber, currentLogFileLength, knownGoodLogEnd, currentInstance
	 * Reset the scan to the given LogInstance.
	 * If stopAt is 'not invalid' check direction and instance against it
	 * throw IOException if beyond scan limit

		@param instance the position to reset to
		@exception IOException scan cannot access the log at the new position.
		@exception IOException 
	*/

	public void resetPosition(LogInstance instance) throws IOException
	{
		if (DEBUG) {
			assert(instance != null);
            System.out.println("Scan.resetPosition resetting to " + instance+" with stopAt:"+stopAt);
		}

		long instance_long = ((LogCounter)instance).getValueAsLong();
		// if passed instance invalid OR
		// if stopAt is valid, AND scan direction is forward instance > stopAt OR direction back and stopAt less
		// toss exception
		if ((instance_long == LogCounter.INVALID_LOG_INSTANCE) ||
			(stopAt != LogCounter.INVALID_LOG_INSTANCE &&
			 (scanDirection == FORWARD && instance_long > stopAt) ||
			 (scanDirection == BACKWARD && instance_long < stopAt)))
		{
			close();
			throw new IOException("Beyond scan limit"+ instance+" "+new LogCounter(stopAt));
		} else {
			long fnum = ((LogCounter)instance).getLogFileNumber();
			if (fnum != currentLogFileNumber) {
				if (DEBUG) {
					System.out.println(
										"Scan.resetPosition " + scanDirection +
										" resetting to " + instance + 
										" need to switch log from " + 
										currentLogFileNumber + " to " + fnum);
                   
				}
				scan.close();
				scan = logFactory.getLogFileAtPosition(instance_long);
				currentLogFileNumber= fnum;
			}
			// other purpose, need to sync access to the end of the log
			//
			currentLogFileLength = scan.length();
			long fpos = ((LogCounter)instance).getLogFilePosition();
			scan.seek(fpos);
			//
			if (DEBUG) {
                       System.out.println("Scan.resetPosition reset to " + instance);
			}

			currentInstance = instance_long;

			//scan is being reset, it is possibly that, scan is doing a random 
			//access of the log file. set the knownGoodLogEnd to  the instance
			//scan 	is being reset to.
			//Note: reset gets called with undo forward scan for CLR processing during 
			//recovery, if this value is not reset checks to find the end of log 
			//getNextRecordForward() will fail because undoscan scans log file
			//back & forth to redo CLR's.
			knownGoodLogEnd = currentInstance;

			if (DEBUG)
            {
                   System.out.println( "Scan.resetPosition EXIT reset to " + LogCounter.toDebugString(currentInstance));  
			}
		}
	}

	/**
		Return the log instance (as an integer) the scan is currently on - this is the log
		instance of the log record that was returned by getNextRecord.
	*/
	public long getInstance()
	{
		return currentInstance;
	}

	/**
		Return the log instance at the end of the log record on the current
		LogFile in the form of a log instance.
        After the scan has been closed, the end of the last log record will be
        returned except when the scan ended in an empty log file.  In that
        case, the start of this empty log file will be returned.  (This is
        done to make sure new log records are inserted into the newest log
        file.)
	*/
	public long getLogRecordEnd()
	{
		return knownGoodLogEnd;
	}

	/**
	   returns true if there is partially written log records before the crash 
	   in the last log file. Partial writes are identified during forward 
	   redo scans for log recovery.
	*/
	public boolean isLogEndFuzzy()
	{
		return fuzzyLogEnd;
	}

	/**
		Return the log instance the scan is currently on - this is the log
		instance of the log record that was returned by getNextRecord.
	*/
	public LogInstance getLogInstance()
	{
		if (currentInstance == LogCounter.INVALID_LOG_INSTANCE)
			return null;
		else
			return new LogCounter(currentInstance);
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

		logFactory = null;
		currentLogFileNumber = -1;
		currentLogFileLength = -1;
        // Do not reset knownGoodLogEnd, it needs to be available after the
        // scan has closed.
		currentInstance = LogCounter.INVALID_LOG_INSTANCE;
		stopAt = LogCounter.INVALID_LOG_INSTANCE;
		scanDirection = 0;
	}

	@Override
	public long getBlockNumber() {
		return currentInstance;
	}


}
