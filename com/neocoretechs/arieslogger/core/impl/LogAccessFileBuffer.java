
package com.neocoretechs.arieslogger.core.impl;

import java.nio.ByteBuffer;


/**
* A single buffer of data. this class encapsulates a NIO ByteBuffer
* It functions as a linked list as it contains next and prev pointers of the same type.
* Frequently, we will initialize it with space reserved for the checksum
* record at the beginning, hence the need for the reserved space calculation.
**/

final class LogAccessFileBuffer
{

    private static final boolean DEBUG = false;
	protected ByteBuffer    buffer;
    protected long      greatest_instance;
    protected int length;
    private int reservedLength;

    LogAccessFileBuffer next;
    LogAccessFileBuffer prev;

    /**
     * Allocate size bytes to the NIO ByteBuffer, set the next and prev values to null
     * @param size
     */
    public LogAccessFileBuffer(int size)
    {
        buffer      = ByteBuffer.allocate(size);
        prev        = null;
        next        = null;

        init(0);
    }
    /**
     * Check to see if reservedLength = length, if so , buffer empty. A bit counterintuitive
     * but realize that we nned to allocate empty space above where we wish to initially write and
     * the buffer position and length will only increment from there, so if they are equal, the buffer is 'empty'
     * @return
     */
    public boolean isBufferEmpty() {
    	return reservedLength == length;
    }
 
    /**
     * Sets the length to 'reserve', the reservedLength to 'reserve', buffer.position to 'reserve' and
     * greatest_instance to -1
     * @param reserve
     */
    public void init(int reserve)
    {
		length = reserve;
		reservedLength = reserve;
		if( DEBUG ) {
			System.out.println("LogAccessFileBuffer.init len:"+length+" cap:"+buffer.capacity()+" res:"+reserve);
		}
		buffer.position(reserve);
        greatest_instance = -1;
        assert(length > 0) : "initialization to less than zero bytes";
    }

}
