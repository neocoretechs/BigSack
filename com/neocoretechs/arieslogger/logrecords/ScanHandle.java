/*

    - Class com.neocoretechs.arieslogger.logrecords.ScanHandle

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

package com.neocoretechs.arieslogger.logrecords;

import java.io.IOException;

import com.neocoretechs.arieslogger.core.LogInstance;

/**
  Interface for scanning the log from outside.
  */
public interface ScanHandle
{
	/**
	  Position to the next log record. 
	  @return true if the log contains a next flushed log record and
	           false otherwise. If this returns false it is incorrect
			   to make any of the other calls on this interface.
	  @exception 
	  */
	public boolean next() throws IOException;

	/**
	  Get the group for the current log record.
	  @exception 
	  */
	public int getGroup() throws IOException;

	/**
	  Get the Loggable associated with the currentLogRecord
	  @exception
	  */
	public Loggable getLoggable() throws IOException;

	/**
	  Get the DatabaseInstant for the current log record.
	  @exception
	  */
    public LogInstance getInstance() throws IOException;
	/**
	  Get the TransactionId for the current log record.
	  @exception
	  */
	public Object getTransactionId() throws IOException;
	/**
	  Close this scan.
	  */
    public void close();
}
