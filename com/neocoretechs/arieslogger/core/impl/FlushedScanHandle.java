/*

    - Class com.neocoretechs.arieslogger.core.FlushedScanHandle

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
import com.neocoretechs.arieslogger.core.StreamLogScan;
import com.neocoretechs.arieslogger.logrecords.Loggable;
import com.neocoretechs.arieslogger.logrecords.ScanHandle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

public class FlushedScanHandle implements ScanHandle
{
	private static final boolean DEBUG = false;
	LogFactory lf;
	StreamLogScan fs;
	
	HashMap<LogInstance, LogRecord> lrx = null;
	LogRecord lr;
	boolean readOptionalData = false;
	int groupsIWant;
	
	ByteBuffer rawInput = ByteBuffer.allocate(4096);
	
	FlushedScanHandle(LogToFile lf, LogCounter start, int groupsIWant) throws IOException
	{
		this.lf = lf;
		fs = new FlushedScan(lf, start.getValueAsLong());
		this.groupsIWant = groupsIWant;
	}
	
	public boolean next() throws IOException
	{
		readOptionalData = false;
		lr = null;
		// filter the log stream so that only log records that belong to these
		// interesting groups will be returned
		try
		{
			lrx = fs.getNextRecord(rawInput,-1, groupsIWant);
			if (lrx == null) return false; //End of flushed log
			lr = lrx.get(0);
			if (DEBUG)
            {
                if ((groupsIWant & lr.group()) == 0)
                    throw new IOException(groupsIWant + "/" + lr.group());
            }

			return true;
		}
		catch (IOException ioe)
		{
			ioe.printStackTrace();
			fs.close();
			fs = null;
			throw lf.markCorrupt(ioe);
		}
	}

	/**
	  Get the group for the current log record.
	  @exception StandardException Oops
	  */
	public int getGroup() throws IOException
	{
		return lr.group();
	}

	/**
	  Get the Loggable associated with the currentLogRecord
	  @exception StandardException Oops
	  */
	public Loggable getLoggable() throws IOException
	{
		try {
			return lr.getLoggable();
		} catch (IOException ioe) {
			ioe.printStackTrace();
			fs.close();
			fs = null;
			throw lf.markCorrupt(ioe);
		} catch (ClassNotFoundException cnfe) {
			fs.close();
			fs = null;
			throw lf.markCorrupt(new IOException(cnfe));
		}
	}

	//This may be called only once per log record.
    public ByteBuffer getOptionalData() throws IOException
	{
		if (DEBUG) assert(!readOptionalData);
		if (lr == null) return null;
		int dataLength = rawInput.getInt();
		readOptionalData = true;
		//rawInput.setLimit(dataLength);
		return rawInput;
	}

    public LogInstance getInstance() throws IOException {
		return fs.getLogInstance();
	}

	public Object getTransactionId() throws IOException {  
		try
        {
			return lr.getTransactionId();
		}
		catch (IOException ioe)
		{
			ioe.printStackTrace();
			fs.close();
			fs = null;
			throw lf.markCorrupt(ioe);
		} catch (ClassNotFoundException cnfe) {
			fs.close();
			fs = null;
			throw lf.markCorrupt(new IOException( cnfe));
		}
	}

    public void close()
	{
		if (fs != null) fs.close();
		fs = null;
	}
}
