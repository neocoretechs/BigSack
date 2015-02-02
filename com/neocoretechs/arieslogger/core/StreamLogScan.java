/*

    - Class com.neocoretechs.arieslogger.core.StreamLogScan

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

package com.neocoretechs.arieslogger.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

import com.neocoretechs.arieslogger.core.impl.LogRecord;

/**
	LogScan provides methods to read a log record and get its LogInstance
	in an already defined scan.  A logscan also needs to know how to advance to
	the next log record.
*/

public interface StreamLogScan extends LogScan {

	/**
		Get the next record in the scan and place its data in the passed in
		array.  The scan is advanced to the next log record.
	    If the input array is of insufficient size, getNextRecord must expand
		the array to accomodate the log record.  User can optionally pass in a
		group mask.  If provided, only log record that
		matches the group mask is returned.

		@param groupmask    if non-zero, only log record whose Loggable's group
                            value is included in the groupmask is returned.  
                            groupmask can be a bit wise OR of many Loggable 
                            groups.  If zero, log records are not filtered on 
                            the Loggable's group.

		@return an object that represents the log records by instance and record, return null if the
		scan has completed. 

		@exception IOException       Some I/O exception raised during reading 
                                     the log record.
	*/
	public HashMap<LogInstance, LogRecord> getNextRecord(int groupmask)  throws IOException;

	/**
		Get the instance of the record just retrieved with getNextRecord(). 
		@return INVALID_LOG_INSTANCE if no records have been returned yet or
		the scan has completed.
	*/
	public long getLogInstanceAsLong();

	/**
		Get the log instance that is right after the record just retrieved with
		getNextRecord().  Only valid for a forward scan and on a successful
		retrieval.

		@return INVALID_LOG_INSTANCE if this is not a FORWARD scan or, no
		record have been returned yet or the scan has completed.
	*/
	public long getLogRecordEnd();
	
	/**
	   @return true if  fuzzy log end found during forward scan, this happens
	   if there was a partially written log records before the crash.
	*/
	public boolean isLogEndFuzzy();

	/**
	    Get the LogInstance for the record just retrieved with getNextRecord().
		@return null if no records have been returned yet or the scan has
		completed.
		*/
	public LogInstance getLogInstance();

	/**
		Reset the scan to the given LogInstance so that getNextRecord get the
		log record AFTER the given LogInstance.

		@param instance the log instance to reset to

		@exception IOException       Some I/O exception raised when accessing 
                                     the log file
		@exception StandardException reset to illegal position or beyond the
		                             limit of the scan.
	*/
	public void resetPosition(LogInstance instance)  throws IOException;

	/**
		Close this log scan.
	*/
	public void close();

	public void checkFuzzyLogEnd() throws IOException;
}
