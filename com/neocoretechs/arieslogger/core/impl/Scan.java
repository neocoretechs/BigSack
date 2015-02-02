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


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import java.util.HashMap;

import com.neocoretechs.arieslogger.core.LogInstance;
import com.neocoretechs.arieslogger.core.StreamLogScan;
import com.neocoretechs.arieslogger.logrecords.Loggable;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;

/**

		Scan the the log which is implemented by a series of log files.n
		This log scan knows how to move across log file if it is positioned at
		the boundary of a log file and needs to getNextRecord.
		The instances in the log file correspond to the position of the instance record, not the size record
		This simplifies checks against position, they should flat be the same

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
	private static final boolean DEBUG = false;
	public static boolean multiTrans = false;

	private RandomAccessFile scan;		// an output stream to the log file
	private LogToFile logFactory; 		// log factory knows how to to skip
										// from log file to log file

	private long currentLogFileNumber; 	// the log file the scan is currently on

	private long currentLogFileLength;	// the size of the current log file
										// used only for FORWARD scan to determine when
										// to switch the next log file


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
	private long scannedEndInstance;	// end found with forward scan

	boolean firstRecord = true; // used to prevent check for zero length end marker
	/**
	    For backward scan, we expect a scan positioned at the end of the next log record.
		For forward scan, we expect a scan positioned at the beginning of the next log record.

		For forward flushed scan, we expect stopAt to be the instance for the
		   first not-flushed log record. Like any forward scan, we expect a scan
		   positioned at the beginning of the next log record.

		@exception IOException cannot access the log at the new position.
	*/
	public Scan(LogToFile logFactory, long startAt, LogInstance stopAt, byte direction) throws IOException
	{
		if (DEBUG)
			assert startAt == LogCounter.INVALID_LOG_INSTANCE : "cannot start scan on an invalid log instance";

		this.logFactory = logFactory;
		currentLogFileNumber = LogCounter.getLogFileNumber(startAt);
		currentLogFileLength = -1;
		currentInstance = LogCounter.INVALID_LOG_INSTANCE; // set at getNextRecord
		if (stopAt != null)
			this.stopAt = ((LogCounter) stopAt).getValueAsLong();
		else
			this.stopAt = LogCounter.INVALID_LOG_INSTANCE;

		scanDirection = direction;

		// NOTE: just get the length of the file without syncing.
		scan =  logFactory.getLogFileAtPosition(startAt);
		if (scan == null)
				throw new IOException("scan null at " + LogCounter.toDebugString(startAt));
		currentLogFileLength = scan.length();
		scan.seek(LogCounter.getLogFilePosition(startAt));
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
	public HashMap<LogInstance, LogRecord> getNextRecord(int groupmask) throws IOException
	{
		if (scan == null)
			return null;

		HashMap<LogInstance, LogRecord> lr = null;
		try
		{
			if (scanDirection == BACKWARD)
				lr = getNextRecordBackward(groupmask);
			else if (scanDirection == FORWARD)
				lr = getNextRecordForward(groupmask);

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
	private HashMap<LogInstance, LogRecord> getNextRecordBackward( int groupmask) throws IOException, ClassNotFoundException
	{
		if (DEBUG)
			assert scanDirection == BACKWARD : "can only called by backward scan";

		// scan is positioned just past the last byte of the record, or
		// right at the beginning of the file (end of the file header)
		// may need to switch log file

		boolean candidate;
		HashMap<LogInstance, LogRecord> lr = null;
		
		long curpos = scan.getFilePointer();
		// startAt positioned at zero marker EOF
		if( DEBUG )
			System.out.println("Scan.getNextRecordBackward curpos:"+curpos);
		if(curpos-4 == LogToFile.LOG_FILE_HEADER_SIZE) {
			// stopAt is valid and the stopAt number is the current log number and we are at header
			if (stopAt != LogCounter.INVALID_LOG_INSTANCE &&
				LogCounter.getLogFileNumber(stopAt) == currentLogFileNumber)
			{
				if (DEBUG) {
                        System.out.println( "stopping at " + currentLogFileNumber);
                }
				return null;  // no more log records
			}		
			// figure out where the last log record is in the previous
			// log file
			if(!moveBackwardFromHeader()) {
				return null;
			}
			// scan is located right past the last byte of the last log
			// record in the previous log file.  currentLogFileNumber is
			// set.  We asserted that the scan is not located right at the
			// end of the file header, in other words, there is at least
			// one log record in this log file.
			curpos = scan.getFilePointer();
		}
		
		// skip over the length and zero EOF
		//if( firstRecord )
			scan.seek(curpos - 8);
		//else
		//	scan.seek(curpos - 4); // just back up to previous length pointer
		int prevSize = scan.readInt(); // read size of last record positioned from forward scan
		if( DEBUG ) {
			System.out.println("Scan.getNextRecordBackward initial size:"+prevSize);
		}
		scan.seek(curpos-prevSize-16); // start of record, 16 plus record size
		curpos = scan.getFilePointer();
		long recordStartPosition = curpos;
		int recordLength = prevSize;
		if( DEBUG ) {
			System.out.println("Scan.getNextRecordBackward set up at "+curpos+" record size "+prevSize);
		}
		do
		{
			// this log record is a candidate unless proven otherwise
			candidate = true; 
			lr = null;
			// moving backward, we have hit the header in position yet may have more files
			// attempt previous file
	
			// read the current log instance - this is the currentInstance if we have not
			// exceeded the scan limit or some other horror
			currentInstance = scan.readLong();
	
			if (DEBUG) {
				// sanity check the current instance against the scan position
				// our records start at the instance for purposes of checks, not at the record size position
				if (LogCounter.getLogFileNumber(currentInstance) !=
					currentLogFileNumber ||
					LogCounter.getLogFilePosition(currentInstance) !=
					recordStartPosition-4)
					throw new IOException(
								 "Wrong LogInstance on log record " +
								LogCounter.toDebugString(currentInstance) + 
								 " version real position (" +
								 currentLogFileNumber + "," +
								 recordStartPosition + ")");
			}
			byte[] data =  new byte[recordLength];
			scan.readFully(data, 0, recordLength);
			// read the length after the log record and check it against the
			// length before the log record
			int checkLength = scan.readInt();
			if (!firstRecord && checkLength != recordLength)
			{
				long inst = LogCounter.makeLogInstanceAsLong(currentLogFileNumber, recordStartPosition);
				throw logFactory.markCorrupt(
                    new IOException("Corrupt:"+
                        new Long(checkLength)+" "+
                        new Long(recordLength)+" "+
                        new Long(inst)+" "+
                        new Long(currentLogFileNumber)));
			}
			firstRecord = false;
			// put the data to 'input'
			ByteBuffer input = ByteBuffer.wrap(data);
			
			if( DEBUG ) {
				System.out.println("Scan.getNextRecordBackward RecordLength:"+recordLength+" currentInstance:"+LogCounter.toDebugString(currentInstance));
			}
			// create the return struc
			if( lr == null )
				lr = new HashMap<LogInstance, LogRecord>();
			
			LogRecord lrec = (LogRecord)(GlobalDBIO.deserializeObject(input));
			
			// skip the checksum log records, there is no need to look at them 
			// during backward scans. Presumable they have been reviewd during forward scan during
			// roll forward or redo log scan recovery
			if( lrec.isChecksum() || (multiTrans && groupmask != 0 && (groupmask & lrec.group()) == 0) ) {
				candidate = false; 
			} else {
				lr.put(new LogCounter(currentInstance), lrec);
			}
			// go back to the start of the log record so that the next time
			// this method is called, it is positioned 2 records back
			// 
			// skip over the length
			recordStartPosition = LogCounter.getLogFilePosition(currentInstance);
			scan.seek(recordStartPosition-4); // this should be length of previous record
			prevSize = scan.readInt();
			if( DEBUG ) {
				System.out.println("Scan.getNextRecordBackward scanned size of previous record "+prevSize);
			}
			scan.seek(recordStartPosition-prevSize-12); // start of size of PREVIOUS record	
			//}
	
		} while (!candidate);

		return lr;

	}
	/**
	 * Move to the previous file number while positioned at header of current file
	 * currentLogFileNumber and scan are reset
	 * @return false for last file encountered
	 * @throws IOException
	 */
	private boolean moveBackwardFromHeader() throws IOException {
		if( currentLogFileNumber == 1 ) {
			if(DEBUG)
				System.out.println("Scan.moveBackwardFromHeader at log file 1, cannot move back, exiting");
			return false;
		}
		// figure out where the last log record is in the previous
		// log file
		scan.seek(LogToFile.LOG_FILE_HEADER_PREVIOUS_LOG_INSTANCE_OFFSET);
		long previousLogInstance = scan.readLong();
		scan.close();
		if(previousLogInstance == LogCounter.INVALID_LOG_INSTANCE)
			return false;
		//"scanning backward beyond the first log file");
		if (currentLogFileNumber != LogCounter.getLogFileNumber(previousLogInstance) + 1)
				throw new IOException("scanning backward but get incorrect log file number " + 
				 "expected " + (currentLogFileNumber -1) + "get " +
				 LogCounter.getLogFileNumber(previousLogInstance));

		assert(LogCounter.getLogFilePosition(previousLogInstance) > LogToFile.LOG_FILE_HEADER_SIZE);//,
							 //"scanning backward encounter completely empty log file");
		if(DEBUG)
			System.out.println("scanning backwards from log file " +
							currentLogFileNumber + ", switch to (" + 
							LogCounter.getLogFileNumber(previousLogInstance) + "," +
							LogCounter.getLogFilePosition(previousLogInstance) + ")"
							);
		// log file switch, set this.currentLogFileNumber
		currentLogFileNumber = LogCounter.getLogFileNumber(previousLogInstance);

		scan = logFactory.getLogFileAtPosition(previousLogInstance);
		// read to end of file to set up backward scan
		// go back 4 for length
		long scanPos = scan.getFilePointer();
		//scan.seek(scanPos-4);
		int recLen = scan.readInt();
		if( DEBUG )
			System.out.println("Scan.moveBackwardFromHeader record length of last instance "+recLen);
		// move FORWARD 12 + recLen to start, will move back 8 on entry to scan
		scan.seek(scanPos+recLen+20);
		if( DEBUG )
			System.out.println("Scan.moveBackwardFromHeader file position on exit "+scan.getFilePointer());
		return true;

	}
	/**
	 * currentLogFileNumber and scan are reset, assumes currentInstance on previous file record
	 * @throws IOException
	 */
	private boolean moveForwardToHeader() throws IOException {
		File newLogFile = logFactory.getLogFileName(currentLogFileNumber+1);
		if( !logFactory.privExists(newLogFile)) {
			if( DEBUG )
				System.out.println("Scan.moveForwardToHeader encountered last file "+newLogFile);
			return false;
		}
		long previousInstance = currentInstance;
		// BUMP the current log file number to advance
		LogCounter li = new LogCounter(currentLogFileNumber+1,LogToFile.LOG_FILE_HEADER_SIZE);
		if( DEBUG )
			System.out.println("Scan.moveForwardToHeader setting position to "+li+" in response to EOF with previous instance at "+LogCounter.toDebugString(previousInstance));
		resetPosition(li);
		if (scan == null) { // we have seen the last log file
			if( DEBUG )
				System.out.println("Scan.moveForwardToHeader "+li+" passed last file, returning null");
			return false;
		}
		if (DEBUG) {
			System.out.println("Scan.moveForwardToHeader:switched to next log file " + li+ " at position "+ scan.getFilePointer());
    	}
		// should have the correct header instance for previous record
		long checkInstance = logFactory.getHeaderLogInstance();
		if( previousInstance != checkInstance ) {
			throw new IOException("Scan.moveForwardToHeader instance mismatch in previous log file linkage "+
					LogCounter.toDebugString(previousInstance)+" "+
					LogCounter.toDebugString(checkInstance));
		}
		// we have successfully switched to the next log file.
		// scan is positioned just before the next log record

		System.out.println("Scan.moveForwardToHeader exiting with current log " +
							currentLogFileNumber + " header instance " + 
							LogCounter.toDebugString(checkInstance)
							);
		return true;

	}
	/**
		Read the next log record.
		Switching log to a previous log file if necessary, 
		Resize the input stream byte array if necessary.  
		@see StreamLogScan#getNextRecord

		Side effects include: 
				on a successful read, setting currentInstance,
				on a log file switch, setting currentLogFileNumber, currentLogFileLength.
				on detecting a fuzzy log end that needs clearing, it will call
				logFactory to clear the fuzzy log end.

		@return the next LogRecord, or null if the end of the
		scan has been reached.
	*/
	private HashMap<LogInstance, LogRecord> getNextRecordForward(int groupmask) throws IOException, ClassNotFoundException
	{
		if (DEBUG) {
			assert(scanDirection == FORWARD) : "can only called by forward scan";
			System.out.println("Scan.getNextRecordForward: entering with file pos:"+scan.getFilePointer());
		}
		HashMap<LogInstance, LogRecord> logBlock = null;
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

		boolean candidate;
		LogRecord lr;

		do {
			long recordStartPosition = scan.getFilePointer();
			// this log record is a candidate unless proven otherwise
			candidate = true;
			lr = null;
			//if( DEBUG ) {
			//	System.out.println("Scane.getNextRecordForward resordStartPosition:"+recordStartPosition+" currentLogFileLength:"+currentLogFileLength);
			//}
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
                            " greater than file length " + currentLogFileLength);
             
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
			//if( DEBUG )
			//	System.out.println("Scan.getNextRecordForward: record len from scan:"+recordLength+
			//			" recordStartPosition:"+recordStartPosition+" currentLogFileLength:"+currentLogFileLength);
	
			/**********************************************
			 * Increment the current log
			 * record 0, we have to move to next file
			 */
			if(recordLength == 0) {
				// set the last scanned position in case we finish
				scannedEndInstance = LogCounter.makeLogInstanceAsLong(currentLogFileNumber, scan.getFilePointer());
				if( moveForwardToHeader() ) {
					recordStartPosition = scan.getFilePointer();
					recordLength = scan.readInt();
				} else { // false indicates last file passed
					return logBlock;
				}
			} // recordLength == 0
			
			/****************************
			 * we know the entire log record is on this log file
			 * recordStartPosition should point to last record
			 */
			// read the current log instance
			currentInstance = scan.readLong();
			// currentInstance is coming from potential newly opened file

			// sanity check it 
			if (LogCounter.getLogFileNumber(currentInstance) != currentLogFileNumber ||
				LogCounter.getLogFilePosition(currentInstance) != recordStartPosition)
						throw new IOException("Wrong LogInstance on log record " +
								LogCounter.toDebugString(currentInstance) + 
								 " version real position (" +
								 currentLogFileNumber + "," +
								 recordStartPosition + ")");
			
			//if( DEBUG )
			//	System.out.println("Scan.getNextRecordForward: Current instance: "+
			//					 LogCounter.toDebugString(currentInstance));

			// read in the log record
			byte[] data = new byte[recordLength];

			// If the log is encrypted, we must do the filtering after
			// reading  the record.
	
			//if (groupmask == 0 && tranId == -1)
			//{
					// no filter, get the whole thing
			//if( DEBUG ) {
			//	System.out.println("Scan.getNextRecordForward reading "+recordLength+" @ "+scan.getFilePointer());
			//}
			
			scan.readFully(data, 0, recordLength);
			
			//if( DEBUG ) {
			//	System.out.println("Scan.getNextRecordForward read ended @"+scan.getFilePointer());
			//}
			// put the data to 'input'
			ByteBuffer input = ByteBuffer.wrap(data);
			
			//if( DEBUG ) {
			//	System.out.println("Scan.getNextRecordForward: put data, new buffer position:"+input.position()+
			//			" new file pos:"+scan.getFilePointer());
			//}
			
			lr = (LogRecord)(GlobalDBIO.deserializeObject(input));
			
			//if( DEBUG ) {
			//	System.out.println("Scan.getNextRecordForward RecordLength:"+recordLength+" rec:"+lr);
			//}
			// if multiTrans is active we are processing multiple users
			if(multiTrans && groupmask != 0 && (groupmask & lr.group()) == 0) {
					candidate = false; // no match, throw this log record out 
					continue;
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
			 *read the length after the log record and check it against the
			 *length before the log record, make sure we go to the correct
			 *place for skipped log record.
			 * 
			 */
			//if (!candidate)
			//	scan.seek(recordStartPosition - 4);
			int checkLength = scan.readInt();
			if( checkLength != recordLength ) {
				if( checkLength < recordLength ) {
					fuzzyLogEnd = true ;
					if( DEBUG ) {
						System.out.println("Scan.getNextRecordForward: fuzzy log end detected:"+checkLength+
								" expected: "+recordLength+" returning...");
					}
					return null;
				} else {				
					//If checklength > recordLength then it can be not be a partial write
					//probably it is corrupted for some reason , this should never
					//happen
					throw logFactory.markCorrupt
						(new IOException("Corrupt:"+
                            new Long(checkLength)+" "+
                            new Long(recordLength)+" "+
                            LogCounter.toDebugString(currentInstance)+" "+
                            new Long(currentLogFileNumber)));

				}
			}
			// next record start position is right after this record
			recordStartPosition = scan.getFilePointer();

			// the scan is now positioned just past this log record and right
			// at the beginning of the next log record
			/** if the current log record is a checksum log record then
			 * using the information available in this record validate
			 * that data in the log file by matching the checksum in 
			 * checksum log record and by recalculating the checksum for the 
			 * specified length of the data in the log file. checksum values
			 * should match unless the write was incomplete before the crash.
			 */
			if(lr.isChecksum())
			{
				// checksum log record should not be returned to the logger recovery redo
				// routines, it is just used to identify the incomplete log writes.
				Loggable op = lr.getLoggable(); 
				ChecksumOperation clop = (ChecksumOperation) op;
				int ckDataLength =  clop.getDataLength(); 
				//if (DEBUG) {
				//		System.out.println( "Scan.getNextRecordForward Log Record is Checksum:"+
				//							"scanned " + op + 
				//							" instance = " + 
				//							LogCounter.toDebugString(currentInstance) + 
				//							" record start position:"+
				//							recordStartPosition+" checksum data length:"+ckDataLength);
				//}
	
				// check if the expected number of bytes by the checksum log
				// record actually exist in the file and then verify if checksum
				// is valid to identify any incomplete out of order writes.
				if((recordStartPosition + ckDataLength + LogToFile.LOG_RECORD_OVERHEAD) <= scan.length()){
					//if( DEBUG ) {
					//	System.out.println("Scan.getNextRecordForward processing checksum payload start:"+recordStartPosition+" data len:"+ckDataLength+" file pos:"+scan.getFilePointer());
					//}
					if( data.length < ckDataLength )
						data = new byte[ckDataLength];
					// read the data into the buffer
					scan.skipBytes(4);
					currentInstance = scan.readLong();
					scan.readFully(data, 0, ckDataLength);
					scan.skipBytes(4);
					// verify the checksum 
					if(clop.isChecksumValid(data, 0 , ckDataLength)) {
						// 'data' should contain checksum block, deserialize the records
						// int record length, long instance
						// at end, int recordlength
						if(logBlock == null)
							logBlock = new HashMap<LogInstance, LogRecord>();
						ByteBuffer bbcb = ByteBuffer.wrap(data);
						LogRecord checkRec = (LogRecord) GlobalDBIO.deserializeObject(bbcb);
						logBlock.put(new LogCounter(currentInstance), checkRec);
						continue;
					} else {
						// declare that the end of the transaction log is fuzzy, checksum is invalid
						// only when the writes are incomplete; this can happen
						// only when writes at the end of the log were partially
						// written before the crash. 
						//if (DEBUG) {
						//	System.out.println("Scan.getNextRecordForward:"+
                        //        "detected fuzzy log end on log file while doing checksum checks " + 
						//		currentLogFileNumber + 
                        //        " checksum record start position " + recordStartPosition + 
                        //        " file length " + currentLogFileLength + 
						//		" checksumDataLength=" + ckDataLength);	
						//}
						fuzzyLogEnd = true;
						scan.close();
						scan = null;
						return null;
					}
				}
			} // lr.isChecksum()

		} while (!candidate);
		// if logrecord is not a checksum, and it checksum is valid, we have a good candidate
		return logBlock;
	}


	/**
	 * Works on 'scan' instance
	 * resets currentLogFileNumber, currentLogFileLength, currentKnownLogEnd, currentInstance
	 * Reset the scan to the given LogInstance.
	 * If stopAt is 'not invalid' check direction and instance against it
	 * If we cnnot locate the file then we return normally with instance of 'scan' null
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
				// If no file exists the n scan will be null and we may return
				if( scan == null )
					return;
				currentLogFileNumber= fnum;
			}
			// other purpose, need to sync access to the end of the log
			//
			currentLogFileLength = scan.length();
			// although this points to a random record possible, its the best we have until we can scan this
			// file for records. If we reposition to this file, we have no real way of determining relative log end
			// so dont completely trust this to be the actual end
			long fpos = ((LogCounter)instance).getLogFilePosition();
			scan.seek(fpos);
			//
			currentInstance = instance_long;
			if (DEBUG) {
                   System.out.println( "Scan.resetPosition EXIT reset to " + instance);  
			}
		}
	}
	/**
	 * Gets a record regardless of current log file instance etc.
	 * Used in recovery where we need to revisit log records for undo
	 * @param logFactory
	 * @param instance
	 * @return
	 * @throws IOException
	 */
	public static LogRecord getRecord(LogToFile logFactory, LogInstance instance) throws IOException
	{
		if (DEBUG) {
			assert(instance != null);
            System.out.println("Scan.resetPosition resetting to " + instance);
		}
		long instance_long = ((LogCounter)instance).getValueAsLong();
		// if passed instance invalid OR
		// if stopAt is valid, AND scan direction is forward instance > stopAt OR direction back and stopAt less
		// toss exception
		if(instance_long == LogCounter.INVALID_LOG_INSTANCE) 
		{
			throw new IOException("Invalid instance in Scan.getRecord "+ instance);
		}
		//long fnum = ((LogCounter)instance).getLogFileNumber();
		RandomAccessFile scan = logFactory.getLogFileAtPosition(instance_long);
		// If no file exists the n scan will be null and we may return
		if( scan == null )
				return null;
		// other purpose, need to sync access to the end of the log
		//
		// although this points to a random record possible, its the best we have until we can scan this
		// file for records. If we reposition to this file, we have no real way of determining relative log end
		// so dont completely trust this to be the actual end
		long fpos = ((LogCounter)instance).getLogFilePosition();
		scan.seek(fpos);
		//
		if (DEBUG) {
              System.out.println("Scan.getRecord retrieving " + instance);
		}
		int recordLength = scan.readInt();
		scan.skipBytes(8); // instance
		byte[] data = new byte[recordLength];
		scan.readFully(data, 0, recordLength);
		return (LogRecord) GlobalDBIO.deserializeObject(data);
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
		currentInstance = LogCounter.INVALID_LOG_INSTANCE;
		stopAt = LogCounter.INVALID_LOG_INSTANCE;
		scanDirection = 0;
	}

	public long getScannedEndInstance() { return scannedEndInstance; }
			
	@Override
	public long getLogRecordEnd() {
		return logFactory.endPosition;
	}

	@Override
	public long getLogInstanceAsLong() {
		return currentInstance;
	}

	/**
	 * Check for fuzzy log end, repair if possible by writing zeros after fuzz
	 * We assume scan is available to use and we have performed a forward scan of records to find
	 * the bogus end we are working on.
	 * 	 the end log marker is 4 bytes (of zeros)
		
		 if endPosition + 4 == logOut.length, we have a
         properly terminated log file
		
		 if endPosition + 4 is > logOut.length, there are 0,
         1, 2, or 3 bytes of 'fuzz' at the end of the log. We
         can ignore that because it is guaranteed to be
         overwritten by the next log record.
		
		 if endPosition + 4 is < logOut.length, we have a
         partial log record at the end of the log.
		
	 * @throws IOException 
	 */
	@Override
	public void checkFuzzyLogEnd() throws IOException {
		// We need to overwrite all of the incomplete log
        // record, because if we start logging but cannot
        // 'consume' all the bad log. If record size is wrong, deserializing the
		// log instance will cause an exception
		//
		//find out if log had incomplete log records at the end.
		if (isLogEndFuzzy()) {
			scan = logFactory.getLogFileAtPosition(getScannedEndInstance());
			long endPosition = scan.getFilePointer();
			long eof = scan.length();
			System.out.println("Fuzzy log end detected, best effort to zero fuzz starting at "+endPosition+" EOF "+eof);
			/* Write zeros from incomplete log record to end of file */
			long nWrites = (eof - endPosition)/logFactory.logBufferSize;
			int rBytes = (int)((eof - endPosition) % logFactory.logBufferSize);
			byte zeroBuf[]= new byte[logFactory.logBufferSize];
			System.out.println("Fuzzy EOF at:"+eof+" buffer size:"+logFactory.logBufferSize+" nWrites="+nWrites+" rBytes="+rBytes);
			//write the zeros to file
			while(nWrites-- > 0)
				scan.write(zeroBuf);
			if(rBytes !=0)
				scan.write(zeroBuf, 0, rBytes);
			logFactory.syncFile(scan);
			assert(scan.length() > endPosition) : "log end > log file length, bad scan";
			scan.close();
		} // fuzzy end
	}

}
