package com.neocoretechs.bigsack.io;
import java.io.*;
import java.nio.*;
import java.nio.channels.*;
/*
* Copyright (c) 2003, NeoCoreTechs
* All rights reserved.
* Redistribution and use in source and binary forms, with or without modification, 
* are permitted provided that the following conditions are met:
*
* Redistributions of source code must retain the above copyright notice, this list of
* conditions and the following disclaimer. 
* Redistributions in binary form must reproduce the above copyright notice, 
* this list of conditions and the following disclaimer in the documentation and/or
* other materials provided with the distribution. 
* Neither the name of NeoCoreTechs nor the names of its contributors may be 
* used to endorse or promote products derived from this software without specific prior written permission. 
* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED 
* WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A 
* PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR 
* ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
* TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
* HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT 
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED 
* OF THE POSSIBILITY OF SUCH DAMAGE.
*
*/
/**
* Memory mapped file I/O.
* We can only map 2 gig at a time due to mmap, so we keep track of ranges
* currently mapped and remap when necessary.
* For pool, there are one of these per tablespace and pointers. Use the
* first 3 bits for tablespace so our theoretical max per tablespace is
* 2,305,843,009,213,693,952 bytes * 8 tablespaces.
* A MappedByteBuffer is the underlying core object, and thread synchronization is on that.
* @see IoInterface
* @author Groff
*/
final class LinkedMappedByteBuffer {
	private static boolean DEBUG = false;
	private FileChannel FC;
	private MappedByteBuffer bb;
	private static int rangeSize = Integer.MAX_VALUE;
	private long rangeMin = 0L;
	private long rangeMax = (long) rangeSize;
	/**
	* @param tFC The FileChannel to map
	* @param tiSize The initial size to map (can be > than file size to extend)
	* and must be less than Integer.MAX_SIZE
	*/
	LinkedMappedByteBuffer(FileChannel tFC, long tiSize)
		throws IOException {
		FC = tFC;
		setRange(0L, tiSize);
	}
	/**
	* @param tFC The FileChannel to map
	* @param tiSize The initial size to map (can be > than file size to extend)
	* and must be less than Integer.MAX_SIZE
	* @param rPos The position to set to
	*/
	LinkedMappedByteBuffer(FileChannel tFC, long tiSize, long rPos) throws IOException {
		FC = tFC;
		setRange(rPos, tiSize);
	}
	
	MappedByteBuffer force() {
		synchronized(bb) {
			return bb.force();
		}
	}
	
	boolean isLoaded() {
		synchronized(bb) {
			return bb.isLoaded();
		}
	}
	
	MappedByteBuffer load() {
		synchronized(bb) {
			return bb.load();
		}
	}
	
	long position() throws IOException {
		synchronized(bb) {
			return rangeMin + bb.position();
		}
	}
	
	void position(long offset) throws IOException {
		checkRange(offset);
	}
	
	long capacity() throws IOException {
		synchronized(bb) {
			return FC.size();
		}
	}
	/**
	* Compute the range we are determined to map<br>
	* It will only ever be maxxed at FileChannel size, so
	* you must extend it via external mechanisim using Filechannel, it
	* is not automatically extended!<br>
	* Only chunks up to rangeSize are mapped
	* @param rangeTarg The target that our range must contain
	* @exception IOException if filechannel position or size ops fail
	*/
	private void checkRange(long rangeTarg) throws IOException {
		synchronized(bb) {
			if (rangeTarg >= rangeMin && rangeTarg <= rangeMax) {
				// within range but exceeding capacity of buffer
				// we must have extended the file so remap if necessary
				int rPos = (int) (rangeTarg - rangeMin);
				if (rPos < bb.capacity()) {
					if( DEBUG ) {
						System.out.println("LinkedMappedByteBuffer.checkRange 1 range:"+rangeTarg+" min:"+rangeMin+" max:"+rangeMax+" try pos:"+rPos+" max:"+bb.capacity());
					}
					bb.position(rPos);
					return;
				}
			}
			long r1 = rangeTarg / rangeSize;
			rangeMin = r1 * rangeSize;
			rangeMax = (rangeMin + rangeSize) - 1L;
			// remap new range
			bb.force();
			bb = null;
			System.gc();
			long iSize = FC.size();
			if (iSize > rangeSize)
				iSize = rangeSize;
			bb = FC.map(FileChannel.MapMode.READ_WRITE, rangeMin, iSize);
			if( DEBUG ) {
				System.out.println("LinkedMappedByteBuffer.checkRange 2 range:"+rangeTarg+" min:"+rangeMin+" max:"+rangeMax+" try pos:"+(rangeTarg-rangeMin)+" max:"+bb.capacity());
			}
			bb.position((int) (rangeTarg - rangeMin));
		}
	}
	/**
	* Set the range we are determined to map<br>
	* This method, called from c'tor can extend the file
	* Only chunks up to rangesize are mapped and this method is used
	* to set initial position other than default on startup
	* @param rangeTarg The target that our range must contain
	* @param iSize The desired size to map
	* @exception IOException if FileChannel.map fails, or position fails
	*/
	private void setRange(long rangeTarg, long iSize) throws IOException {

		long r1 = rangeTarg / rangeSize;
		rangeMin = r1 * rangeSize;
		rangeMax = (rangeMin + rangeSize) - 1L;
		// map new range
		if (iSize > rangeSize)
			iSize = rangeSize;
		bb = FC.map(FileChannel.MapMode.READ_WRITE, rangeMin, iSize);
		synchronized(bb) {
			bb.position((int) (rangeTarg - rangeMin));
		}
	}
	
	// writing..
	void put(byte[] buf) throws IOException {
		put(buf, 0, buf.length);
	}
	
	void put(byte[] buf, int ioffs, int numbyte) throws IOException {
		synchronized(bb) {
			int i = ioffs, runcount = numbyte, blkbytes;
			// assume our position is set and we have space
			if (bb.position() == (rangeSize - 1))
				checkRange(bb.position() + 1);
			//
			for (;;) {
				blkbytes = (rangeSize - 1) - bb.position();
				if (runcount > blkbytes) {
					runcount -= blkbytes;
					bb.put(buf, i, blkbytes);
					i += blkbytes;
					checkRange(bb.position() + 1);
				} else {
					bb.put(buf, i, runcount);
					return;
				}
			}
			//bb.put(obuf, 0, osiz);
		}
	}
	
	void putInt(int obuf) throws IOException {
		ByteBuffer tbb = ByteBuffer.allocate(4);
		tbb.putInt(obuf);
		put(tbb.array());
	}
	
	void putLong(long obuf) throws IOException {
		ByteBuffer tbb = ByteBuffer.allocate(8);
		tbb.putLong(obuf);
		put(tbb.array());
	}
	
	void putShort(short obuf) throws IOException {
		ByteBuffer tbb = ByteBuffer.allocate(2);
		tbb.putShort(obuf);
		put(tbb.array());
	}
	
	void putByte(byte obuf) throws IOException {
		ByteBuffer tbb = ByteBuffer.allocate(1);
		tbb.put(obuf);
		put(tbb.array());		
	}
	
	// reading...
	int get(byte[] buf, int ioffs, int numbyte) throws IOException {
		synchronized(bb) {
			int i = ioffs, runcount = numbyte, blkbytes;
			// assume our position is set and we have space
			if (bb.position() == (rangeSize - 1L))
				checkRange(bb.position() + 1);
			//
			for (;;) {
				blkbytes = (rangeSize - 1) - bb.position();
				if (runcount > blkbytes) {
					runcount -= blkbytes;
					bb.get(buf, i, blkbytes);
					i += blkbytes;
					checkRange(bb.position() + 1);
				} else {
					bb.get(buf, i, runcount);
					i += runcount;
					break;
				}
			}
			//bb.get(b, 0, osiz);
			return i;
		}
	}
	
	int get(byte[] b) throws IOException {
		return get(b, 0, b.length);
	}
	
	int getInt() throws IOException {
		byte[] b = new byte[4];
		get(b);
		ByteBuffer tbb = ByteBuffer.wrap(b);
		return tbb.getInt();
	}
	
	long getLong() throws IOException {
		byte[] b = new byte[8];
		get(b);
		ByteBuffer tbb = ByteBuffer.wrap(b);
		return tbb.getLong();
	}
	
	short getShort() throws IOException {
		byte[] b = new byte[2];
		get(b);
		ByteBuffer tbb = ByteBuffer.wrap(b);
		return tbb.getShort();
	}
	
	byte get() throws IOException {
		byte[] b = new byte[1];
		get(b);
		return b[0];
	}

}
