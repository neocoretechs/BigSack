/*

    - Class com.neocoretechs.arieslogger.core.ChecksumOperation

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
import java.util.zip.Checksum;
import java.util.zip.CRC32;


/**
	A Log Operation that represents a checksum for a group of log records
	that are written to the transaction log file.

	<PRE>
	@.formatId	LOGOP_CHECKSUM
		the formatId is written by FormatIdOutputStream when this object is
		written out by writeObject
	@.purpose  checksum one or more log records while writing to disk
	@.upgrade
	@.diskLayout
		checksumAlgo(byte)  	the checksum algorithm 
		checksumValue(long)     the checksum value 
		dataLength(int)			number of bytes that the checksum is calculated
	@.endFormat
	</PRE>

	@see Loggable
*/

public class ChecksumOperation implements Loggable, Externalizable
{

	private  byte   checksumAlgo;
	private  long   checksumValue;   
	private  int	dataLength; 
	private transient Checksum checksum; 

	/*
	 * constant values for algorithm that are used to perform the checksum.
	 */
    public static final byte CRC32_ALGORITHM  = (byte) 0x1; //java.util.zip.CRC32
	
	private static final int LOGOP_CHECKSUM = 0;
	private static final boolean DEBUG = true;
	
	public void init()
	{
		this.checksumAlgo = CRC32_ALGORITHM;
		initializeChecksumAlgo();
		dataLength = 0;
	}

	// update the checksum
	protected void update(byte[] buf, int off, int len)
	{
		if( DEBUG ) {
			System.out.println("ChecksumOperation.update: buflen:"+buf.length+" offs:"+off+" len:"+len);
		}
		checksum.update(buf, off , len);
		dataLength += len;
	}
	
	// reset the checksum 
	protected void reset()
	{
		checksum.reset();
		dataLength = 0;
	}

	private void initializeChecksumAlgo()
	{
		if(checksumAlgo == CRC32_ALGORITHM)
			this.checksum = new CRC32();
	}

	/*
	 * Formatable methods
	 */

	// no-arg constructor, required by Formatable 
	public ChecksumOperation() { super();}

	public void writeExternal(ObjectOutput out) throws IOException 
	{	
		checksumValue = checksum.getValue();
		out.writeByte(checksumAlgo);
		out.writeInt(dataLength);
		out.writeLong(checksumValue);
	}

	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
	{
		checksumAlgo = (byte) in.readUnsignedByte();
		dataLength = in.readInt();
		checksumValue = in.readLong();
		initializeChecksumAlgo();
	}


	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return LOGOP_CHECKSUM;
	}

	/**
		Loggable methods
	*/

	/**
	 *	Nothing to do for the checksum log record because it does need to be
	 *  applied during redo. 
	 */
	public void applyChange(BlockDBIO xact, LogInstance instance, Object in) throws IOException
	{
	}

	/**
		the default for prepared log is always null for all the operations
		that don't have optionalData.  If an operation has optional data,
		the operation need to prepare the optional data for this method.
		Checksum has no optional data to write out	
	*/
	public byte[] getPreparedLog()
	{
		return  null;
	}

	/**
		Checksum does not need to be redone, it is used to just verify that
		log records are written completely.
	*/
	public boolean needsRedo(BlockDBIO xact)
	{
		return false;
	}

	/**
	  Checksum has no resources to release
	*/
	public void releaseResource(BlockDBIO xact){}

	/**
		Checksum is a raw store operation
	*/
	public int group()
	{
		return Loggable.RAWSTORE | Loggable.CHECKSUM;
	}

	/**
	 * Access attributes of the checksum log record
	 */

	protected int getDataLength() 
	{
		return dataLength;
	}

	protected boolean isChecksumValid(byte[] data, int off , int length)
	{
		checksum.reset();
		checksum.update(data , off , length);
		return checksum.getValue()== checksumValue;

	}

	/**
	  DEBUG: Print self.
	*/
	public String toString()
	{
			StringBuffer str = new StringBuffer(20)
				.append("Checksum Operation ")
				.append(" algorithm = ")
				.append(checksumAlgo)
				.append(" value = ")
				.append(checksumValue)
				.append(" data length= ").append(dataLength);

				return str.toString();
	}
}












