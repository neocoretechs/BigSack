
package com.neocoretechs.arieslogger.core.impl;

import java.nio.ByteBuffer;


/**
* A single buffer of data.
**/

final class LogAccessFileBuffer
{

    private static final boolean DEBUG = true;
	protected ByteBuffer    buffer;
    protected long      greatest_instance;
    protected int length;
    private int reservedLength;

    LogAccessFileBuffer next;
    LogAccessFileBuffer prev;

    public LogAccessFileBuffer(int size)
    {
        buffer      = ByteBuffer.allocate(size);
        prev        = null;
        next        = null;

        init(0);
    }
    public boolean isBufferEmpty() {
    	return reservedLength == length;
    }
 
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
