/*

    - Class com.neocoretechs.arieslogger.core.LogCounter

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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.neocoretechs.arieslogger.core.LogInstance;

/**
	A very simple log instance implementation.

	Within the stored log record a log counter is represented as a long,
	hence the getValueAsLong() method. Outside the LogFactory the instance
	is passed around as a LogCounter (through its LogInstance interface).

	The way the long is encoded is such that < == > correctly tells if
	one log instance is lessThan, equals or greater than another.

*/
public class LogCounter implements LogInstance, Externalizable {

	/********************************************************
	**
	**	This class implements Formatable. That means that it
	**	can write itself to and from a formatted stream. If
	**	you add more fields to this class, make sure that you
	**	also write/read them with the writeExternal()/readExternal()
	**	methods.
	**
	**	If, between releases, you add more fields to this class,
	**	then you should bump the version number emitted by the getTypeFormatId()
	**	method.
	**
	********************************************************/
	
	/** A well defined value of an invalid log instance. */
	public static final long INVALID_LOG_INSTANCE = 0;
	
	// max possible log file number is 2^31 -1
	public static final long MAX_LOGFILE_NUMBER	= (long)0x7FFFFFFFL; // 2147483647 
	// lower end of 32 bits in long type are used to store the log file position
	private static final long FILE_NUMBER_SHIFT	= 32;

	// reserve top 4 bits in log file size for future use
	public static final long MAX_LOGFILE_SIZE = (long)0x0FFFFFFFL; // 268435455
	// 32 bits are used to store the log file postion
	private static final long FILE_POSITION_MASK = (long)0x7FFFFFFFL;

	private static final boolean DEBUG = true;

	private static final int LOG_COUNTER = 0;

	private long fileNumber;
	private long filePosition;

	// contructors
	public LogCounter(long value) {
		fileNumber = getLogFileNumber(value);
		filePosition = getLogFilePosition(value);
	}

	public LogCounter(long fileNumber, long position) {

		if (DEBUG) {
			assert(fileNumber > 0) : "illegal fileNumber";
			assert(position > 0) : "illegal file position";

			assert(position < MAX_LOGFILE_SIZE) :
							 "log file position exceeded max log file size. log file position = " + position;
			assert(fileNumber < MAX_LOGFILE_NUMBER) :
							 "log file number exceeded max log file number. log file number = " + fileNumber;
		}

		this.fileNumber = fileNumber;
		this.filePosition = position;
	}

	/**
	 *
	 */
	public LogCounter() {}
	
	/** 
		Static functions that can only be used inside the RawStore's log
		factory which passes the log counter around encoded as a long
	*/

	// make a log instance from 2 longs and return a long which is the long
	// representation of a LogCounter
	static public final long makeLogInstanceAsLong(long filenum, long filepos)
	{
		if (DEBUG) {
			assert(filenum > 0) : "illegal fileNumber";
			assert(filepos > 0) : "illegal file position";

			assert(filepos < MAX_LOGFILE_SIZE) :
							 "log file position exceeded max log file size. log file position = " + filepos;
			assert(filenum < MAX_LOGFILE_NUMBER) :
							 "log file number exceeded max log file number. log file number = " + filenum;
		}

		return ((filenum << FILE_NUMBER_SHIFT) | filepos);
	}


	static public final long getLogFilePosition(long valueAsLong)
	{
		return valueAsLong & FILE_POSITION_MASK;
	}

	static public final long getLogFileNumber(long valueAsLong)
	{
		return valueAsLong >>> FILE_NUMBER_SHIFT;
	}

	/** LogScan methods */

	public boolean lessThan(Object other) {
		LogCounter compare = (LogCounter)other;
		return (fileNumber == compare.fileNumber) ? filePosition < compare.filePosition : fileNumber < compare.fileNumber;
	}

	public boolean equals(Object other) {
		if (this == other)
			return true;

		if (!(other instanceof LogCounter))
			return false;

		LogCounter compare = (LogCounter)other;

		return fileNumber == compare.fileNumber && filePosition == compare.filePosition;
	}

    public LogCounter next() {
        return new LogCounter( makeLogInstanceAsLong(fileNumber, filePosition) + 1);
    }
    
    public LogCounter prior() {
        return new LogCounter( makeLogInstanceAsLong(fileNumber, filePosition) - 1);
    }
    
	public int hashCode() {
		return (int) (filePosition ^ fileNumber);
	}

	public String toString() {
		return "(" + fileNumber + "," + filePosition + ")";
	}

	public static String toDebugString(long instance)
	{
			return "(" + getLogFileNumber(instance) + "," + getLogFilePosition(instance) + ")";
	}

	/**
		These following methods are only intended to be called by an
		implementation of a log factory. All other uses of this object should
		only see it as a log instance.
	*/
	public long getValueAsLong() {
		return makeLogInstanceAsLong(fileNumber, filePosition);
	}

	public long getLogFilePosition()
	{
		 return filePosition;
	}

	public long getLogFileNumber()
	{
		return fileNumber;
	}
	
	/*
	 * methods for the Formatable interface
	 */

	/**
	 * Read this in.
	 * @exception IOException error reading from log stream
	 * @exception ClassNotFoundException corrupted log stream
	 */
	public void readExternal(ObjectInput oi) throws IOException, ClassNotFoundException {
		fileNumber = oi.readLong();
		filePosition = oi.readLong();
	}
	
	/**
	 * Write this out.
	 * @exception IOException error writing to log stream
	 */
	public void writeExternal(ObjectOutput oo) throws IOException {
		oo.writeLong(fileNumber);
		oo.writeLong(filePosition);
	}
	
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public int getTypeFormatId()	{ return LOG_COUNTER; }

}
