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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.SyncFailedException;
import java.nio.ByteBuffer;
import java.util.LinkedList;

import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;

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
    stage to 	another stage is synchronized using	the object(this) of this class. 

	A Checksum log record that has the checksum value for the data that is
    being written to the disk is generated and written 	before the actual data. 
	Except for the large log records that does not fit into a single buffer, 
    checksum is calculated for a group of log records that are in the buffer 
	when buffers is switched. Checksum log record is written into the reserved
	space in the beginning buffer. 

    In case of a large log record that does not fit into a buffer, the
    checksum is written to the byte[] allocated for the big log
    record. 

	Checksum log records helps in identifying the incomplete log disk writes during 
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
    private static final int            LOG_RECORD_FIXED_OVERHEAD_SIZE = 16;
	private static final int            LOG_RECORD_HEADER_SIZE = 12; //(length + instance)
    private static final int            LOG_NUMBER_LOG_BUFFERS = 3;
	private static final boolean DEBUG = true;
	private static final int STORE_MAJOR_VERSION_1 = 1;
	private static final int STORE_MINOR_VERSION_0 = 0;


	private LinkedList    freeBuffers;  //list of free buffers
	private LinkedList    dirtyBuffers; //list of dirty buffers to flush
	private  LogAccessFileBuffer currentBuffer; //current active buffer
	private boolean flushInProgress = false;
	
	private final RandomAccessFile  log;

	// log can be touched only inside synchronized block protected by
	// logFileSemaphore.
	private final Object            logFileSemaphore;

	static int                      mon_numWritesToLog;
	static int                      mon_numBytesToLog;

	private long checksumInstance = -1;
	private int checksumLength;
	private int checksumLogRecordSize;      //checksumLength + LOG_RECORD_FIXED_OVERHEAD_SIZE
	private ChecksumOperation checksumLogOperation;
	private LogRecord checksumLogRecord;
		
	public LogAccessFile(RandomAccessFile log, int bufferSize) throws IOException 
    {

		this.log            = log;
		logFileSemaphore    = log;

		if (DEBUG)
            assert(LOG_NUMBER_LOG_BUFFERS >= 1);
				
		//initialize buffers lists
		freeBuffers = new LinkedList();
		dirtyBuffers = new LinkedList();


		//add all buffers to free list
        for (int i = 0; i < LOG_NUMBER_LOG_BUFFERS; i++)
        {
            LogAccessFileBuffer b = new LogAccessFileBuffer(bufferSize);
            freeBuffers.addLast(b);
        }

		currentBuffer = (LogAccessFileBuffer) freeBuffers.removeFirst();
		
		/**
		 * setup structures that are required to write the checksum log records
		 * for a group of log records are being written to the disk. 
		 */
		checksumLogOperation = new ChecksumOperation();
		checksumLogOperation.init();
		checksumLogRecord = new LogRecord();

		// Note: Checksum log records are not related any particular transaction, 
		// they are written to store a checksum to identify
		// incomplete log record writes. No transacton id is set for this
		// log record. That is why a null argument is passed below 
		// setValue(..) call. 
		checksumLogRecord.setValue(-1, checksumLogOperation);
		checksumLogRecordSize = checksumLogRecord.getRecordSize();
		if( DEBUG ) {
			System.out.println("LogAccessFile: checksum log record len:"+checksumLogRecordSize);
		}

		/** initialize the buffer with space reserved for checksum log record in
		 * the beginning of the log buffer; checksum record is written into
		 * this space when buffer is switched
		 */
		currentBuffer.init(checksumLogRecordSize + LOG_RECORD_FIXED_OVERHEAD_SIZE );
	}


    /**
     * Write a single log record to the stream.
     * <p>
     * For performance pass all parameters rather into a specialized routine
     * rather than maintaining the writeInt, writeLong, and write interfaces
     * that this class provides as a standard OutputStream.  It will make it
     * harder to use other OutputStream implementations, but makes for less
     * function calls and allows optimizations knowing when to switch buffers.
     * <p>
     * This routine handles all log records which are smaller than one log
     * buffer.  If a log record is bigger than a log buffer it calls
     * writeUnbufferedLogRecord().
     * <p>
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
    public void writeLogRecord(
    int     length,
    long    instance,
    byte[]  data,
    int     data_offset,
    byte[]  optional_data,
    int     optional_data_offset,
    int     optional_data_length) throws IOException 
    {
        if( DEBUG )
        	System.out.println("LogAccessFile.writeLogRecord instance:"+instance+" len:"+length);
        	
    	assert(currentBuffer.buffer.limit() > 0 ) : "free bytes less than zero";
    	
        int total_log_record_length = length + LOG_RECORD_FIXED_OVERHEAD_SIZE;

        if (total_log_record_length <= currentBuffer.buffer.remaining()) {
            //if( DEBUG ) {
            //	System.out.println("LogAccessFile.writeLogRecord: main buffer reclen/bytes free:"+total_log_record_length+"/"+currentBuffer.buffer.remaining()+" -- "+new String(data)+" len:"+length);
            //}
            int newpos = appendLogRecordToBuffer(currentBuffer.buffer,
                                                 currentBuffer.buffer.position(),
                                                 length, 
                                                 instance, 
                                                 data, 
                                                 data_offset,
                                                 optional_data,
                                                 optional_data_offset,
                                                 optional_data_length);
 
            //currentBuffer.buffer.bytes_free -= total_log_record_length;
            currentBuffer.greatest_instance = instance;
            if (DEBUG) {
                int normalizedPosition = newpos;
                normalizedPosition -= checksumLogRecordSize;
                assert(currentBuffer.buffer.remaining() + normalizedPosition == currentBuffer.length) :
                    "free_bytes and position do not add up to the total length of the buffer";
            }

        } else {
            /* The current log record will never fit in a single
             * buffer. The reason is that reserveSpaceForChecksum is
             * always called before writeLogRecord (see
             * LogToFile#appendLogRecord). When we reach this point,
             * reserveSpaceForChecksum has already found out that the
             * previous buffer did not have enough free bytes to store
             * this log record, and therefore switched to a fresh
             * buffer. Hence, currentBuffer is empty now, and
             * switching to the next free buffer will not help. Since
             * there is no way for this log record to fit into a
             * buffer, it is written to a new, big enough, byte[] and
             * then written to log file instead of writing it to
             * buffer.
             */

            // allocate a byte[] that is big enough to contain the
            // giant log record:
            if( DEBUG ) {
            	System.out.println("LogAccessFile.writeLogRecord: BIG buffer reclen/bytes free:"+total_log_record_length+"/"+currentBuffer.buffer.remaining()+" -- "+new String(data)+" len:"+length);
            }
            // add another log overhead for checksum log record
            int bigBufferLength = checksumLogRecordSize + LOG_RECORD_FIXED_OVERHEAD_SIZE + total_log_record_length;
            
            ByteBuffer bigbuffer = ByteBuffer.allocate(bigBufferLength);
            appendLogRecordToBuffer(bigbuffer, checksumLogRecordSize,
                                    length, 
                                    instance, 
                                    data, 
                                    data_offset,
                                    optional_data,
                                    optional_data_offset,
                                    optional_data_length);

    
            checksumLogOperation.reset();
            checksumLogOperation.update(bigbuffer.array(), checksumLogRecordSize + LOG_RECORD_FIXED_OVERHEAD_SIZE,
                                            total_log_record_length);

            writeChecksumLogRecord(bigbuffer);

            // flush all buffers before writing the bigbuffer to the
            // log file.
            flushLogAccessFile();

            // Note:No Special Synchronization required here , There
            // will be nothing to write by flushDirtyBuffers that can
            // run in parallel to the threads that is executing this
            // code. Above flush call should have written all the
            // buffers and NO new log will get added until the
            // following direct log to file call finishes.

			// write the log record directly to the log file.
            //if( DEBUG ) {
            //	System.out.println("LogAccessFile.writeLogRecord: len:"+bigBufferLength+" dat:"+new String(bigbuffer.array()));
            //}
            writeToLog(bigbuffer.array(), 0, bigBufferLength, instance);
        }
    }

    /**
     * Append a log record to a byte[]. Typically, the byte[] will be
     * currentBuffer, but if a log record that is too big to fit in a
     * buffer is added, buff will be a newly allocated byte[].
     *
     * @param buffer The byte[] the log record is appended to
     * @param pos The position in buff where the method will start to
     * append to
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
        if( buffer == currentBuffer.buffer ) {
        	currentBuffer.length = buffer.position();
        	if( DEBUG ) {
        		System.out.println("LogAccessFile.appendLogRecordToBuffer3: Setting main current buffer to length:"+currentBuffer.length);
        	}
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
     * Note that this routine only "writes" the data to the file, this does not
     * mean that the data has been synched to disk unless file was opened in
	 * WRITE SYNC mode(rws/rwd).  The only way to insure that is by calling
     * is to call syncLogAccessFile() after this call in Non-WRITE sync mode(rw)
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
	protected void flushDirtyBuffers() throws IOException 
    {
		if( DEBUG )
			System.out.println("Flush "+dirtyBuffers.size()+" dirty buffers "+flushInProgress);
        LogAccessFileBuffer buf = null;
		int noOfBuffers;
		int nFlushed= 0;
		try{
			synchronized(this)
			{
				/**if some one else flushing wait, otherwise it is possible 
				 * different threads will get different buffers and order can 
				 * not be determined.
				 * 
				 **/
				while(flushInProgress)
				{
					try{
						wait();
					}catch (InterruptedException ie) 
					{
                       
					}
				}
		
				noOfBuffers = dirtyBuffers.size();
				if(noOfBuffers > 0)
					buf = (LogAccessFileBuffer) dirtyBuffers.removeFirst();
				
				flushInProgress = true;
			}
			
			while(nFlushed < noOfBuffers)
			{
				if (buf.length != 0) {
					if( DEBUG ) {
						System.out.println("LogAccessFile.flushDirtyBuffers: len:"+buf.length+" top inst:"+LogCounter.toDebugString(buf.greatest_instance));
					}
					writeToLog(buf.buffer.array(), 0, buf.length, buf.greatest_instance);
				}

				nFlushed++;
				synchronized(this)
				{
					//add the buffer that was written previously to the free list
					freeBuffers.addLast(buf);
					if(nFlushed < noOfBuffers)
						buf = (LogAccessFileBuffer) dirtyBuffers.removeFirst();
					else
					{
						//see if we can flush more, that came when we are at it.
						//don't flush more than the total number of buffers,
						//that might lead to starvation of the current thread.
						int size = dirtyBuffers.size();
						if(size > 0 && nFlushed <= LOG_NUMBER_LOG_BUFFERS)
						{
							noOfBuffers += size;
							buf = (LogAccessFileBuffer) dirtyBuffers.removeFirst();
						}
					}
				}
			}

				
		} finally {
			synchronized(this)
			{
				flushInProgress = false;
				notifyAll();
			}
		}
	}


	//flush all the the dirty buffers to disk
	public void flushLogAccessFile() throws IOException 
	{
		switchLogBuffer();
		flushDirtyBuffers();
	}

		
	/**
	 * Appends the current Buffer to the dirty Buffer list and assigns a free
	 * buffer to be the currrent active buffer . Flushing of the buffer
	 * to disk is delayed if there is a free buffer available. 
	 * dirty buffers will be  flushed to the disk   
	 * when  flushDirtyBuffers() is invoked by  a commit call 
	 * or when no more free buffers are available. 
	 */
	public void switchLogBuffer() throws IOException  
    {
		
		if( DEBUG ) System.out.println("LogAccessFile.switchLogBuffer");
		synchronized(this)
		{
			// ignore empty buffer switch requests
			if( currentBuffer.isBufferEmpty() ) {
				if( DEBUG ) {
					System.out.println("LogAccessFile.switchLogBuffer: returning without switch buffer pos:"+currentBuffer.buffer.position()+" checksum len:"+checksumLogRecordSize);
				}
				return;
			}
			// calculate the checksum for the current log buffer 
			// and write the record to the space reserved in 
			// the beginning of the buffer. 

			checksumLogOperation.reset();
			checksumLogOperation.update(currentBuffer.buffer.array(), 
						checksumLogRecordSize + LOG_RECORD_FIXED_OVERHEAD_SIZE, 
						currentBuffer.length - (checksumLogRecordSize + LOG_RECORD_FIXED_OVERHEAD_SIZE) );
			writeChecksumLogRecord(currentBuffer.buffer);

			//add the current buffer to the flush buffer list
			dirtyBuffers.addLast(currentBuffer);

			//if there is No free buffer, flush the buffers to get a free one 
			if(freeBuffers.size() == 0) 
			{
				flushDirtyBuffers();
				//after the flush call there should be a free buffer
				//because this is only methods removes items from 
				//free buffers and removal is in synchronized block. 
			}


			// there should be free buffer available at this point.
			if (DEBUG)
				assert(freeBuffers.size() > 0);

			//switch over to the next log buffer, let someone else write it.
			currentBuffer = (LogAccessFileBuffer) freeBuffers.removeFirst();
			currentBuffer.init(checksumLogRecordSize + LOG_RECORD_FIXED_OVERHEAD_SIZE);

			if (DEBUG)
			{
				assert(currentBuffer.buffer.position()+1 == checksumLogRecordSize+LOG_RECORD_FIXED_OVERHEAD_SIZE);
                assert(currentBuffer.buffer.remaining() > 0);
			}
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
    public void syncLogAccessFile() throws IOException
    {
        for( int i=0; ; )
        {
            // 3311: JVM sync call sometimes fails under high load against NFS 
            // mounted disk.  We re-try to do this 20 times.
            try
            {
                synchronized( this)
                {
                    log.getFD().sync();
                }

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
                catch( InterruptedException ie )
                {
                    
                }

                if( i > 20 )
                    throw new IOException(sfe);
            }
        }
    }

	/**
		The database is being marked corrupted, get rid of file pointer without
		writing out anything more.
	 */
	public void corrupt() throws IOException
	{
		synchronized(logFileSemaphore)
		{
			if (log != null)
				log.close();
		}
	}

	public void close() throws IOException
    {
		if (DEBUG) 
        {
			if( !currentBuffer.isBufferEmpty() )
				throw new IOException(
				"Log file being closed with data still buffered " + 
                currentBuffer.buffer.position() +  " " + currentBuffer.buffer.remaining());
		}

		flushLogAccessFile();

		synchronized(logFileSemaphore)
		{
			if (log != null)
				log.close();
		}
	}


	/* write to the log file */
	private void writeToLog(byte b[], int off, int len, long highestInstance) throws IOException {
		if( DEBUG ) {
			System.out.println("LogAccessFile.writeToLog "+log+" offs:"+off+" len:"+len+" hiInst:"+LogCounter.toDebugString(highestInstance));
		}
		synchronized(logFileSemaphore)
		{
            if (log != null)
            {
                // Try to handle case where user application is throwing
                // random interrupts at  threads, retry in the case
                // of IO exceptions 5 times.  After that hope that it is 
                // a real disk problem - an IO error in a write to the log file
                // is going to take down the whole system, so seems worthwhile
                // to retry.
                for (int i = 0; ;i++)
                {
                    try 
                    {
                		if( DEBUG ) {
                			System.out.println("LogAccessFile.writeToLog writing @ file position:"+log.getFilePointer());
                		}
                        log.write(b, off, len);
                        break;
                    }
                    catch (IOException ioe)
                    {
                        // just fall through and retry the log write 1st 5 times.

                        if (i >= 5)
                            throw ioe;
                    }
                }
            }
		}

		if (DEBUG) 
        {
			mon_numWritesToLog++;
			mon_numBytesToLog += len;
		}
	}

	/**
	 * reserve the space for the checksum log record in the log file. 
     *
	 * @param  length           the length of the log record to be written
	 * @param  logFileNumber    current log file number 
	 * @param  currentPosition  current position in the log file. 
     *
	 * @return the space that is needed to write a checksum log record.
	 */
	protected long reserveSpaceForChecksum(int length, long logFileNumber, long currentPosition ) throws IOException
	{

		int total_log_record_length = length + LOG_RECORD_FIXED_OVERHEAD_SIZE;
		
		/* checksum log record is calculated for a group of log 
		 * records that can fit in to a single buffer or for 
		 * a single record when it does not fit into 
		 * a fit into a buffer at all. When a new buffer 
		 * is required to write a log record, log space 
		 * has to be reserved before writing the log record
		 * because checksum is written in the before the 
		 * log records that are being checksummed. 
		 * What it also means is a real log instance has to be 
		 * reserved for writing the checksum log record in addition 
		 * to the log buffer space.
		 */
		
		if (total_log_record_length > currentBuffer.buffer.remaining())
		{
				// the log record that is going to be written is not 
				// going to fit in the current buffer, switch the 
				// log buffer to create buffer space for it. 
				switchLogBuffer();
				// reserve space if log checksum feature is enabled. 
		}
		if (DEBUG) {
				// Previously reserved real checksum instance should have been
				// used, before another one is generated. 
				assert(checksumInstance == -1) : "CHECKSUM INSTANCE OVERWRITE";
		}	
		checksumInstance = LogCounter.makeLogInstanceAsLong(logFileNumber, currentPosition);
		if( DEBUG ) {
				System.out.println("Reserved checksum size:"+checksumLogRecordSize+" instance:"+LogCounter.toDebugString(checksumInstance));
		}
		return checksumLogRecordSize;
	}


	/**
	 * Generate the checskum log record and write it into the log
	 * buffer. The checksum applies to all bytes from this checksum
	 * log record to the next one. 
     * @param bigbuffer The byte[] the checksum is written to. The
     * checksum is always written at the beginning of buffer.
	 */
	private void writeChecksumLogRecord(ByteBuffer bigbuffer) throws IOException {
		
		int p = 0; //checksum is written in the beginning of the buffer
		bigbuffer.position(p);
		bigbuffer.putInt(checksumLogRecordSize);
		bigbuffer.putLong(checksumInstance);
		if( DEBUG )
			System.out.println("writing checksum log record of size "+checksumLogRecordSize);
		//write the checksum log operation  
		byte[] checkObj = GlobalDBIO.getObjectAsBytes(checksumLogRecord);
		if( DEBUG ) {
			System.out.println("checksum log record len:"+checkObj.length+" "+checksumLogRecord.toString());
			assert( checksumLogRecordSize == checkObj.length) : "Assumed checksum log record size unequal to deserilized byte count";
		} 
		bigbuffer.put(checkObj);
		bigbuffer.putInt(checksumLogRecordSize);
	
		//p = LOG_RECORD_HEADER_SIZE + checksumLength ;

		if (DEBUG)
		{
			System.out.println(
									"LogAccessFile.WriteChecksumLogRecord: "  +
									" instance: " + LogCounter.toDebugString(checksumInstance) + " pos: " +bigbuffer.position()+
									" op:" + checksumLogOperation);
			checksumInstance = -1; 
		}

	}

    /** Return the length of a checksum record */
    public  int getChecksumLogRecordSize() { return checksumLogRecordSize; }


	protected void writeEndMarker(int marker) throws IOException 
	{
		//flush all the buffers and then write the end marker.
		flushLogAccessFile();
		
		//end is written in the beginning of the buffer, no
		//need to checksum a int write.
		int pos = currentBuffer.buffer.position();
		currentBuffer.buffer.putInt(marker);
		writeToLog(currentBuffer.buffer.array(), pos, 4, -1); //end marker has no instance
		currentBuffer.buffer.position(pos);
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








