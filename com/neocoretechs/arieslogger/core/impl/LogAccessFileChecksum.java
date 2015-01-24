package com.neocoretechs.arieslogger.core.impl;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.neocoretechs.arieslogger.core.LogInstance;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
/**
 * Maintains the checksum for a given record. All records checksummed and flushed to durable store for logs
 * make sure to call updateChecksum last, before processing, as it sets up entire checksum buffer for writing
 * @author jg
 *
 */
public class LogAccessFileChecksum {
	private long checksumInstance = LogCounter.makeLogInstanceAsLong(1, LogToFile.LOG_FILE_HEADER_SIZE);
	private int checksumLogRecordSize;      //checksumLength + LOG_RECORD_FIXED_OVERHEAD_SIZE
	private ChecksumOperation checksumLogOperation;
	private LogRecord checksumLogRecord;
	private boolean DEBUG = true;
	ByteBuffer checksumBuffer;
	public LogAccessFileChecksum() {
		/**
		 * setup structures that are required to write the checksum log records
		 * for a group of log records are being written to the disk. 
		 */
		checksumLogOperation = new ChecksumOperation();
		checksumLogOperation.init();
		checksumLogRecord = new LogRecord();

		// Note: Checksum log records are not related any particular transaction, 
		// they are written to store a checksum to identify
		// incomplete log record writes. No transaction id is set for this
		// log record. That is why a null argument is passed below 
		// setValue(..) call. 
		checksumLogRecord.setValue(-1, checksumLogOperation);
		checksumLogRecordSize = checksumLogRecord.getRecordSize();
		if( DEBUG ) {
			System.out.println("LogAccessFileChecksum: checksum log record len:"+checksumLogRecordSize);
		}
		checksumBuffer = ByteBuffer.allocate(checksumLogRecordSize+LogAccessFile.LOG_RECORD_FIXED_OVERHEAD_SIZE);
	}
	
	public long getChecksumInstance() {
		return checksumInstance;
	}
	public void setChecksumInstance(long checksumInstance) {
		this.checksumInstance = checksumInstance;
	}
	public int getChecksumLogRecordSize() {
		return checksumLogRecordSize;
	}
	public void setChecksumLogRecordSize(int checksumLogRecordSize) {
		this.checksumLogRecordSize = checksumLogRecordSize;
	}
	public ChecksumOperation getChecksumLogOperation() {
		return checksumLogOperation;
	}
	public void setChecksumLogOperation(ChecksumOperation checksumLogOperation) {
		this.checksumLogOperation = checksumLogOperation;
	}
	public LogRecord getChecksumLogRecord() {
		return checksumLogRecord;
	}
	public void setChecksumLogRecord(LogRecord checksumLogRecord) {
		this.checksumLogRecord = checksumLogRecord;
	}
	/**
	 * Generate the checskum log record and write it into the log
	 * buffer. The checksum applies to all bytes from this checksum
	 * log record to the next one. 
	 * NOTE:make sure to call updateChecksum last, before processing, as it sets up entire checksum buffer for writing
     * @param bigbuffer The byte[] the checksum is written to. The
     * checksum is always written at the beginning of buffer.
	 */
	public int updateChecksum() throws IOException {
    	// calculate the checksum for the current log buffer 
		// and write the record to the space reserved in 
		// the beginning of the buffer. 
		//checksumLogOperation.reset();
		//checksumLogOperation.update(currentBuffer.buffer.array(), 
		//				checksumLogRecordSize + LOG_RECORD_FIXED_OVERHEAD_SIZE, 
		//				currentBuffer.length - (checksumLogRecordSize + LOG_RECORD_FIXED_OVERHEAD_SIZE) );
		checksumBuffer.clear();
		checksumBuffer.putInt(checksumLogRecordSize);
		assert(checksumInstance != LogInstance.INVALID_LOG_INSTANCE);
		checksumBuffer.putLong(checksumInstance);
		if( DEBUG )
			System.out.println("writing checksum log record of size "+checksumLogRecordSize);
		//write the checksum log operation  
		byte[] checkObj = GlobalDBIO.getObjectAsBytes(checksumLogRecord);
		if( DEBUG ) {
			System.out.println("checksum log record len:"+checkObj.length+" "+checksumLogRecord.toString());
			assert( checksumLogRecordSize == checkObj.length) : "Assumed checksum log record size unequal to deserilized byte count";
		} 
		checksumBuffer.put(checkObj);
		checksumBuffer.putInt(checksumLogRecordSize);
		int pos = checksumBuffer.position(); // total length
		checksumBuffer.flip();

		if (DEBUG)
		{
			System.out.println(
									"LogAccessFileChecksum.updateChecksum: "  +
									" instance: " + LogCounter.toDebugString(checksumInstance) +
									" op:" + checksumLogOperation);
		}
		return pos;
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
	protected long setChecksumInstance(int logFileNumber, long currentPosition ) throws IOException
	{
		/* checksum log record is calculated for a log 
		 * record  When a new buffer 
		 * is required to write a log record, log space 
		 * has to be reserved before writing the log record
		 * because checksum is written in the before the 
		 * log records that are being checksummed. 
		 * What it also means is a real log instance has to be 
		 * reserved for writing the checksum log record in addition 
		 * to the log buffer space.
		 */
		checksumInstance = LogCounter.makeLogInstanceAsLong(logFileNumber, currentPosition);
		if( DEBUG ) {
				System.out.println("LogAccessFileChecksum.reserveSpaceForChecksum Reserved checksum size:"+checksumLogRecordSize+" instance:"+LogCounter.toDebugString(checksumInstance));
		}
		return checksumLogRecordSize;
	}

}
