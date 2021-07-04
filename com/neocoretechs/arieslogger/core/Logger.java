/*

    - Class com.neocoretechs.arieslogger.core.impl.Logger

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

import com.neocoretechs.arieslogger.logrecords.Compensation;
import com.neocoretechs.arieslogger.logrecords.Loggable;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;


public interface Logger {

	/**
		Log the loggable operation under the context of the transaction and then
		apply the operation to the RawStore.

		<BR>
		Before you call this method, make sure that the Loggable's applyChange
		method will succeed.  This method will go ahead and send the log record
		to disk, and once it does that, then applyChange cannot fail or the system
		will be shut down and recovery may fail.  So it is <B> very important 
		</B> to make sure that every resource you need for the loggable's applyChange
		method, such as disk space, has be acquired or accounted for before
		calling logAndDo.

		@param xact		the transaction that is affecting the change
		@param operation	the loggable operation that describes the change
		@return LogInstance that is the LogInstance of the loggable operation 

	   */ 
	public LogInstance logAndDo(GlobalDBIO xact, Loggable operation) throws IOException; 

	/**
		Log the compensation operation under the context of the transaction 
        and then apply the undo to the RawStore.

		<BR>
		Before you call this method, make sure that the Compensation's applyChange
		method will succeed.  This method will go ahead and send the log record
		to disk, and once it does that, then applyChange cannot fail or the system
		will be shut down and recovery may fail.  So it is <B> very important 
		</B> to make sure that every resource you need for the Compensation's 
        applyChange method, such as disk space, has be acquired or accounted for before
		calling logAndUnDo.

		@param xact		the transaction that is affecting the undo
		@param operation	the compensation operation
		@param undoInstance	the logInstance of the change that is to be undone
		@param in			optional data

		@return LogInstance that is the LogInstance of the compensation operation

	   */ 
	public LogInstance logAndUndo(GlobalDBIO xact, Compensation operation, LogInstance undoInstance, Object in) throws IOException;

	/**
		Flush all unwritten log record up to the log instance indicated to disk.
		@param where flush log up to here
	*/
	public void flush() throws IOException;


    /**
     * During recovery re-prepare a transaction.
     * <p>
     * After redo() and undo(), this routine is called on all outstanding 
     * in-doubt (prepared) transactions.  This routine re-acquires all 
     * logical write locks for operations in the xact, and then modifies
     * the transaction table entry to make the transaction look as if it
     * had just been prepared following startup after recovery.
     * <p>
     *
     * @param t             is the transaction performing the re-prepare
     * @param undoId        is the transaction ID to be re-prepared
     * @param undoStopAt    is where the log instance (inclusive) where the 
     *                      re-prepare should stop.
     * @param undoStartAt   is the log instance (inclusive) where re-prepare 
     *                      should begin, this is normally the log instance of 
     *                      the last log record of the transaction that is to 
     *                      be re-prepare.  If null, then re-prepare starts 
     *                      from the end of the log.
     *
	 * @exception Standard exception policy.
     **/
	public void reprepare(long t, long undoId, LogInstance undoStopAt, LogInstance undoStartAt) throws IOException;

	/**
	  Undo transaction.

	  @param t is the transaction performing the rollback
	  @param undoId is the transaction ID to be rolled back
	  @param undoStopAt is where the log instance (inclusive) where 
				the rollback should stop.
	  @param undoStartAt is the log instance (inclusive) where rollback
				should begin, this is normally the log instance of 
				the last log record of the transaction that is 
				to be rolled back.  
				If null, then rollback starts from the end of the log.

		@exception
	  */
	public void undo(GlobalDBIO blockio, LogInstance undoStopAt, LogInstance undoStartAt) throws IOException;
}
