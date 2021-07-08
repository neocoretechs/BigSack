/*

    - Class com.neocoretechs.arieslogger.core.LogAccessFile

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
import java.io.SyncFailedException;
import java.nio.ByteBuffer;



/**
	Wraps a RandomAccessFile file to provide buffering
	on log writes. Only supports the write calls
	required for the log!

	MT - unsafe.  Caller of this class must provide synchronization.  The one
	exception is with the log file access, LogAccessFile will touch the log
	only inside synchronized block protected by the semaphore, which is
	defined by the creator of this object.
	
    Write to the log buffers are allowed when there are free buffers even
    when dirty buffers are being written(flushed) to the disk by a different
	thread. Only one flush writes to log file at a time, other wait for it to finish.

	Except for flushLogAccessFile , SyncAccessLogFile other function callers
	must provide synchronization that will allow only one of them to write to 
    the buffers. 

    Log Buffers are used in circular fashion, each buffer moves through following stages: 
	freeBuffers --> dirtyBuffers --> freeBuffers. Movement of buffers from one
    stage to another stage is synchronized using the object(this) of this class. 

	A Checksum log record that has the checksum value for the data that is
    being written to the disk is generated and written before the actual data. 
	Except for the large log records that do not fit into a single buffer, 
    checksum is calculated for a group of log records that are in the buffer 
	when buffers are switched. Checksum log records are written into the reserved
	space in the beginning of the buffer. 

    In case of a large log record that does not fit into a buffer, the
    checksum is written to the byte[] allocated for the big log
    record. 

	Checksum log records help in identifying the incomplete log disk writes during 
    recovery. This is done by recalculating the checksum value for the data on
    the disk and comparing it to the the value stored in the checksum log
    record. 

*/
public class LogAccessFile 
{

    /**
     * The fixed size of a log record is 16 bytes:
     *     int   length             : 4 bytes
     *     long  instance           : 8 bytes
     *     end length				: 4 bytes
     **/
    static final int            LOG_RECORD_FIXED_OVERHEAD_SIZE = 16;
	private static final boolean DEBUG = false;
	private boolean MEASURE = false;
	
	private LogAccessFileChecksum logChecksum;
	LogAccessFileBuffer currentBuffer; //current active buffer
	private boolean flushInProgress = false;
	private File logFile;
	private final RandomAccessFile  log;
	int checksumLogRecordSize;

	static int                      mon_numWritesToLog;
	static int                      mon_numBytesToLog;
		
	public LogAccessFile(File logFile, RandomAccessFile log, int bufferSize) throws IOException {
		this.logFile = logFile;
		this.log        = log;	
		currentBuffer = new LogAccessFileBuffer(bufferSize);
		logChecksum = new LogAccessFileChecksum();
		checksumLogRecordSize = logChecksum.getChecksumLogRecordSize();
		currentBuffer.init(0);
	}

	public LogAccessFileChecksum getLogAccessFileChecksum() { return logChecksum; }
	
	public long getFilePointer() throws IOException {
		return log.getFilePointer();
	}
	
	public void setLength(long len) throws IOException {
		log.setLength(len);
	}
	
	public RandomAccessFile getRandomAccessFile() { return log; }
	
	public boolean exists() { return logFile.exists(); }
	
    /**
     * Write a single log record to the stream.
     * Appends the log record to the bytebuffer and moves the checksum in
     * The log record written will always look the same as if the following
     * code had been executed:
     *     writeInt(length)
     *     writeLong(instance)
     *     write(data, data_offset, (length - optional_data_length) )
     *
     *     if (optional_data_length != 0)
     *         write(optional_data, optional_data_offset, optional_data_length)
     *
     *     writeInt(length)
     *
     * @param length                (data + optional_data) length bytes to write
     * @param instance               the log address of this log record.
     * @param data                  "from" array to copy "data" portion of rec
     * @param data_offset           offset in "data" to start copying from.
     * @param optional_data         "from" array to copy "optional data" from
     * @param optional_data_offset  offset in "optional_data" to start copy from
     * @param optional_data_length  length of optional data to copy.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void writeLogRecord (
    		int     length,
    		long    instance,
    		byte[]  data,
    		int     data_offset,
    		byte[]  optional_data,
    		int     optional_data_offset,
    		int     optional_data_length) throws IOException 
    {
        if( DEBUG )
        	System.out.println("LogAccessFile.writeLogRecord instance:"+LogCounter.toDebugString(instance)+" len:"+length);
        	
    	assert(currentBuffer.buffer.limit() > 0 ) : "free bytes less than or equal to zero";
    	assert(instance != LogCounter.INVALID_LOG_INSTANCE) : "LogAcessFile.writeToLog instance invalid "+LogCounter.toDebugString(instance);
    	
        // now set up checksum in its buffer based on contents
        logChecksum.getChecksumLogOperation().reset();     
        logChecksum.getChecksumLogOperation().update(data, data_offset, length);
        // writes it to internal bytebuffer
        int fileNum = (int) LogCounter.getLogFileNumber(instance);
        assert(fileNum != 0 && log.getFilePointer() != 0) : "LogAccessFile.writeLogRecord checksum instance invalid "+fileNum+" "+log.getFilePointer();
        logChecksum.setChecksumInstance(LogCounter.makeLogInstanceAsLong(fileNum, log.getFilePointer()));
        // make sure to call updateChecksum last, before processing, as it sets up entire checksum buffer for writing
        int lenCs = logChecksum.updateChecksum();
        // write checksum, its ready to go
        writeToLog(logChecksum.checksumBuffer.array(), 0, lenCs);
        //syncLogAccessFile();
        // write currentBuffer to log immediately after checksum record
        int totalLogRecordLength = length + LOG_RECORD_FIXED_OVERHEAD_SIZE;
        // add another log overhead for checksum log record
   
        if( DEBUG ) {
        	System.out.println("LogAccessFile.writeLogRecord: BIG buffer reclen/bytes free:"+totalLogRecordLength+"/"+currentBuffer.buffer.remaining());
        } 
        if( currentBuffer.buffer.capacity() > totalLogRecordLength ) {
        	currentBuffer.buffer.clear();
        } else {
        	currentBuffer.buffer = ByteBuffer.allocate(totalLogRecordLength);
        }
        // set the greatest instance to the next record to be written
        currentBuffer.greatestInstance = LogCounter.makeLogInstanceAsLong(fileNum, log.getFilePointer());
        // append log record
        appendLogRecordToBuffer(currentBuffer.buffer, 
       		 						0,
                                   length, 
                                   currentBuffer.greatestInstance, 
                                   data, 
                                   data_offset,
                                   optional_data,
                                   optional_data_offset,
                                   optional_data_length);
 
    }

    /**
     * Append a log record to a byte[]. Typically, the byte[] will be
     * currentBuffer, but if a log record that is too big to fit in a
     * buffer is added, buff will be a newly allocated byte[].
     *
     * @param buffer The byte[] the log record is appended to
     * @param pos The position in buff where the method will start to
     * append to, unless -1, then position is left at last write
     * @param length (data + optional_data) length bytes to write
     * @param instance the log address of this log record.
     * @param data "from" array to copy "data" portion of rec
     * @param data_offset offset in "data" to start copying from.
     * @param optional_data "from" array to copy "optional data" from
     * @param optional_data_offset offset in "optional_data" to start copy from
     * @param optional_data_length length of optional data to copy.
     *
     * @see LogAccessFile#writeLogRecord
     */
    private int appendLogRecordToBuffer(ByteBuffer buffer, int pos,
                                        int length,
                                        long instance,
                                        byte[] data,
                                        int data_offset,
                                        byte[] optional_data,
                                        int optional_data_offset,
                                        int optional_data_length) {
        if( DEBUG ) {
        	System.out.println("LogAccessFile.appendLogRecordToBuffer1:"+pos+" len:"+length);//+" "+new String(data));
        }
        // if pos > -1 set position
        if( pos != -1)
        	buffer.position(pos);
        buffer.putInt(length);
        buffer.putLong(instance);

        int data_length = length - optional_data_length;
        buffer.put(data, data_offset, data_length);

        if (optional_data_length != 0) {
        	buffer.put(optional_data, optional_data_offset, optional_data_length);
        }
        // write ending length used in reverse scan
        buffer.putInt(length);
        if( DEBUG ) {
        	System.out.println("LogAccessFile.appendLogRecordToBuffer2:"+LogCounter.toDebugString(instance));//+" "+new String(buffer.array()));
        }
        return buffer.position();
    }

    /**
     * Write data from all dirty buffers into the log file.
     * <p>
     * A call for clients of LogAccessFile to insure that all privately buffered
     * data has been written to the file - so that reads on the file using one
     * of the various scan classes will see
     * all the data which has been written to this point.
     * <p>
     * Note that this routine only "writes" the data to the file, full flush
     * is to call syncLogAccessFile() after this call 
	 * 
	 * <p>
	 * MT-Safe : parallel threads can call this function, only one threads does
	 * the flush and the other threads waits for the one that is doing the flush to finish.
	 * Currently there are two possible threads that can call this function in parallel 
	 * 1) A Thread that is doing the commit
	 * 2) A Thread that is writing to the log and log buffers are full or
	 * a log records does not fit in a buffer. (Log Buffers
	 * full(switchLogBuffer() or a log record size that is greater than
	 * logbuffer size has to be written through writeToLog call directly)
	 * Note: writeToLog() is not synchronized on the semaphore
	 * that is used to do  buffer management to allow writes 
	 * to the free buffers when flush is in progress.  
     **/
	protected synchronized void flushBuffers() throws IOException 
    {
		try {
			// wait out any current flushers
			while(flushInProgress) {
				try {
						wait();
				} catch (InterruptedException ie) {}
			}		
			flushInProgress = true;
			byte[] b = currentBuffer.buffer.array();
			//writeToLog(currentBuffer.buffer.array(), 0, currentBuffer.buffer.limit(), currentBuffer.greatest_instance);
			writeToLog(b, 0, currentBuffer.buffer.position());
			// now check to see if we need to switch to next log
		} finally {
				flushInProgress = false;
				currentBuffer.buffer.clear();
				notifyAll();
		}
	}

    /**
     * Guarantee all writes up to the last call to flushLogAccessFile on disk.
     * <p>
     * A call for clients of LogAccessFile to insure that all data written
     * up to the last call to flushLogAccessFile() are written to disk.
     * This call will not return until those writes have hit disk.
     * <p>
     * Note that this routine may block waiting for I/O to complete so 
     * callers should limit the number of resource held locked while this
     * operation is called.  It is expected that the caller
     * Note that this routine only "writes" the data to the file, this does not
     * mean that the data has been synced to disk.  The only way to insure that
     * is to first call switchLogBuffer() and then follow by a call of sync().
     *
     **/
    public synchronized void syncLogAccessFile() throws IOException
    {
        for( int i=0; ; )
        {
            // 3311: JVM sync call sometimes fails under high load against NFS 
            // mounted disk.  We re-try to do this 20 times.
            try
            {
                log.getFD().sync();
                // the sync succeed, so return
                break;
            }
            catch( SyncFailedException sfe )
            {
                i++;
                try
                {
                    // wait for .2 of a second, hopefully I/O is done by now
                    // we wait a max of 4 seconds before we give up
                    Thread.sleep( 200 ); 
                }
                catch( InterruptedException ie ) { }

                if( i > 20 )
                    throw new IOException(sfe);
            }
        }
    }

	/**
		The database is being marked corrupted, get rid of file pointer without
		writing out anything more.
	 */
	public synchronized void corrupt() throws IOException
	{
			System.out.println("CORRUPTION detected, log file closing.."+log);
			if (log != null)
				log.close();
	}
	/**
	 * Flush the log via flushLogAccessFile, then call close in log if not null
	 * @throws IOException
	 */
	public synchronized void close() throws IOException
    {
		if (DEBUG) 
        {
			if( !currentBuffer.isBufferEmpty() )
				throw new IOException("Attempt to close log file while data still buffered " + 
                currentBuffer.buffer.position() +  " " + currentBuffer.buffer.remaining());
			System.out.println("LogAccessFile.Close file being flushed/closed "+log);
        }
		if (log != null)
				log.close();	
	}


	/* write to the log file */
	//private void writeToLog(byte b[], int off, int len, long highestInstance) throws IOException {
	private void writeToLog(byte[] b, int off, int len) throws IOException {
		if( DEBUG ) {
			System.out.println("LogAccessFile.writeToLog "+log+" offs:"+off+" len:"+len+" current file pointer:"+log.getFilePointer());
		}
        assert(log != null);
        log.write(b, off, len);
   
		if(MEASURE) {
			mon_numWritesToLog++;
			mon_numBytesToLog += len;
		}
	}


	public void write(byte b) {
		currentBuffer.buffer.put(b);	
	}

	public void writeInt(int value) {
		currentBuffer.buffer.putInt(value);
	}

	public void writeLong(long value) {
		currentBuffer.buffer.putLong(value);
	}

	public void write(byte[] data, int offset, int bytesToWrite) {
		currentBuffer.buffer.put(data, offset, bytesToWrite);
	}
	
}

