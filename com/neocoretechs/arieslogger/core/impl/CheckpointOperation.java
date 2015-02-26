/*

    - Class com.neocoretechs.arieslogger.core.CheckpointOperation

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
import com.neocoretechs.arieslogger.logrecords.Loggable;
import com.neocoretechs.bigsack.io.pooled.BlockDBIO;

import java.io.Externalizable;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.IOException;

/**
	A Log Operation that represents a checkpoint.
	@see Loggable
*/

public class CheckpointOperation implements Loggable, Externalizable
{
	private static final int LOGOP_CHECKPOINT = 0;
	static final boolean DEBUG = false;
	// redo LWM
	protected long	redoLWM;
	// undo LWM
	protected long	undoLWM;
	protected int tablespace;

	public CheckpointOperation(long redoLWM, long undoLWM, int tablespace)
	{
		this.redoLWM = redoLWM;
		this.undoLWM = undoLWM;
		this.tablespace = tablespace;
	}

	// no-arg constructor
	public CheckpointOperation() { super(); }

	public void writeExternal(ObjectOutput out) throws IOException 
	{
		out.writeLong(redoLWM);
		out.writeLong(undoLWM);

	}

	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
	{
		redoLWM = in.readLong();
		undoLWM = in.readLong();
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return LOGOP_CHECKPOINT;
	}

	/**
		Loggable methods
	*/

	/**
	 *	Nothing to do unless we are rollforward recovery;
	 *  Redoing of checkpoints during rollforward recovery allows us to restart
	 *  the roll-forward recovery from the last checkpoint redone during rollforward recovery, if
	 *  we happen to crash during the roll-forward recovery process.
	*/
	public void applyChange(BlockDBIO xact, LogInstance instance, Object in) throws IOException
	{
		//redo the checkpoint if we are in roll-forward recovery only
		if(inRollForwardRecovery(xact))
		{	
				checkpointInRollForwardRecovery(instance, redoLWM, undoLWM);
		}
		return;
	}
	/**
	 * Not implemented
	 * @param instance
	 * @param redoLWM2
	 * @param undoLWM2
	 */
	private void checkpointInRollForwardRecovery(LogInstance instance,long redoLWM2, long undoLWM2) {
	}
	/**
	 * Go back to blockIO and get the recoverylog instance and then get the logtofile, then see if in RFR from that
	 * @param xact
	 * @return
	 */
	private boolean inRollForwardRecovery(BlockDBIO xact) {
		return xact.getUlog().getLogToFile(tablespace).inRFR();
	}

	/**
		the default for prepared log is always null for all the operations
		that don't have optionalData.  If an operation has optional data,
		the operation need to prepare the optional data for this method.

		Checkpoint has no optional data to write out
	*/
	public byte[] getPreparedLog()
	{
		return  null;
	}

	/**
		Checkpoint does not need to be redone unless
		we are doing rollforward recovery.
	*/
	public boolean needsRedo(BlockDBIO xact)
	{
		return inRollForwardRecovery(xact);
	}


	/**
	  Checkpoint has not resource to release
	*/
	public void releaseResource(BlockDBIO xact) {}

	/**
		Checkpoint is a raw store operation
	*/
	public int group()
	{
		return Loggable.RAWSTORE;
	}

	/**
		Access attributes of the checkpoint record
	*/
	public long redoLWM() 
	{
		return redoLWM;
	}

	public long undoLWM() 
	{
		return undoLWM;
	}


	/**
	  DEBUG: Print self.
	*/
	public String toString()
	{
	
			LogCounter undolwm = new LogCounter(undoLWM);
			LogCounter redolwm = new LogCounter(redoLWM);

			StringBuffer str = new StringBuffer(1000)
				.append("Checkpoint : \tredoLWM ")
				.append(redolwm.toString())
				.append("\n\t\tundoLWM ").append(undolwm.toString());


			return str.toString();
	}
}












