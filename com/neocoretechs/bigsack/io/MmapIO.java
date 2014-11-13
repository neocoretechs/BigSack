package com.neocoretechs.bigsack.io;
import java.io.*;
import java.nio.*;
import java.nio.channels.*;

import com.neocoretechs.bigsack.DBPhysicalConstants;
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
* Memory mapped file I/O.  Somewhat bound to our block-oriented tables
* by the Fextend, which guarantees a block<br>
* For pool, there are one of these per tablespace and pointers use the
* first 3 bits for tablespace so our theoretical max per tablespace is
* 2,305,843,009,213,693,952 bytes * 8 tablespaces
* @see IoInterface
* @author Groff
*/
public final class MmapIO implements IoInterface {
	private boolean fisopen, fisnew;
	private File WO;
	private FileOutputStream FO;
	private FileChannel FC;
	private LinkedMappedByteBuffer linkedMappedByteBuff;
	private ByteBuffer bPageBuff =
		ByteBuffer.allocate(DBPhysicalConstants.DBLOCKSIZ);
	private RandomAccessFile RA;

	public MmapIO() {
	}
	
	public MmapIO(String fname, boolean create) throws IOException {
		Fopen(fname, create);
	}
	/** create is true for 'create if not existing' */
	public boolean Fopen(String fname, boolean create) throws IOException {
		WO = new File(fname);
		if (!WO.exists()) {
			if (create) {
				// create if not existing
				FO = new FileOutputStream(WO);
				FO.close();
				RA = new RandomAccessFile(WO, "rw");
				fisnew = true;
			} else {
				fisopen = false;
				return false;
			}
		} else { // exists?
			//
			RA = new RandomAccessFile(WO, "rw");
			fisnew = false;
		}
		FC = RA.getChannel();
		long iSize;
		if (FC.size() == 0L)
			iSize =
				DBPhysicalConstants.DBLOCKSIZ * DBPhysicalConstants.DBUCKETS;
		else
			iSize = FC.size();
		// map the mem
		//linkedMappedByteBuff = FC.map(FileChannel.MapMode.READ_WRITE, 0, iSize);
		linkedMappedByteBuff = new LinkedMappedByteBuffer(FC, iSize);
		fisopen = true;
		return true;
	}
	// re-open file
	public void Fopen() throws IOException {
		RA = new RandomAccessFile(WO, "rw");
		FC = RA.getChannel();
		long iSize;
		if (FC.size() == 0L) {
			iSize =
				DBPhysicalConstants.DBLOCKSIZ * DBPhysicalConstants.DBUCKETS;
			fisnew = true;
		} else {
			iSize = FC.size();
			fisnew = false;
		}
		//linkedMappedByteBuff = FC.map(FileChannel.MapMode.READ_WRITE, 0, iSize);
		linkedMappedByteBuff = new LinkedMappedByteBuffer(FC, iSize);
		fisopen = true;
	}
	/**
	 * Invoke about every flush of every associated buffer imaginable
	 * lots of opinions but this way just seems to have to work
	 */
	public void Fclose() throws IOException {
		if (fisopen) {
			Fforce();
			FC.close();
			RA.close();
			fisopen = false;
		}
	}
	
	public long Ftell() throws IOException {
		return linkedMappedByteBuff.position();
	}
	
	public void Fseek(long offset) throws IOException {
		linkedMappedByteBuff.position((int) offset);
	}
	
	public long Fsize() throws IOException {
		return linkedMappedByteBuff.capacity();
	}
	
	public void Fset_length(long newlen) throws IOException {
		if (newlen < FC.size()) {
			linkedMappedByteBuff.force();
			linkedMappedByteBuff = null;
			System.gc();
			FC.truncate(newlen);
			FC.force(false);
			//linkedMappedByteBuff = FC.map(FileChannel.MapMode.READ_WRITE, 0, newlen);
			linkedMappedByteBuff = new LinkedMappedByteBuffer(FC, newlen);
		} else if (newlen > FC.size())
			Fextend(newlen);
	}
	
	public void Fforce() throws IOException {
		linkedMappedByteBuff.force();
		FC.force(true);
		RA.getFD().sync();
	}
	
	private void Fextend(long newSize) throws IOException {
		FC.position(newSize - DBPhysicalConstants.DBLOCKSIZ);
		FC.write(bPageBuff);
		bPageBuff.rewind();
		FC.force(false);
	}

	@SuppressWarnings("unused")
	private void Fextend() throws IOException {
		long fPos = linkedMappedByteBuff.position();
		if (fPos == linkedMappedByteBuff.capacity()) {
			FC.write(bPageBuff);
			bPageBuff.rewind();
			FC.force(false);
		}
	}
	// writing..
	public void Fwrite(byte[] obuf) throws IOException {
		try {
			linkedMappedByteBuff.put(obuf);
		} catch (Exception bue) {
			throw new IOException(bue.toString());
		}
	}
	
	public void Fwrite(byte[] obuf, int osiz) throws IOException {
		try {
			linkedMappedByteBuff.put(obuf, 0, osiz);
		} catch (Exception bue) {
			throw new IOException(bue.toString());
		}
	}
	
	public void Fwrite_int(int obuf) throws IOException {
		try {
			linkedMappedByteBuff.putInt(obuf);
		} catch (Exception bue) {
			throw new IOException(bue.toString());
		}
	}
	
	public void Fwrite_long(long obuf) throws IOException {
		try {
			linkedMappedByteBuff.putLong(obuf);
		} catch (Exception bue) {
			throw new IOException(bue.toString());
		}
	}
	
	public void Fwrite_short(short obuf) throws IOException {
		try {
			linkedMappedByteBuff.putShort(obuf);
		} catch (Exception bue) {
			throw new IOException(bue.toString());
		}
	}
	
	// reading...
	public int Fread(byte[] b, int osiz) throws IOException {
		try {
			linkedMappedByteBuff.get(b, 0, osiz);
			return osiz;
		} catch (Exception bue) {
			throw new IOException(bue.toString());
		}
	}
	
	public int Fread(byte[] b) throws IOException {
		try {
			linkedMappedByteBuff.get(b);
			return b.length;
		} catch (Exception bue) {
			throw new IOException(bue.toString());
		}
	}
	
	public int Fread_int() throws IOException {
		try {
			return linkedMappedByteBuff.getInt();
		} catch (Exception bue) {
			throw new IOException(bue.toString());
		}
	}
	
	public long Fread_long() throws IOException {
		try {
			return linkedMappedByteBuff.getLong();
		} catch (Exception bue) {
			throw new IOException(bue.toString());
		}
	}
	
	public short Fread_short() throws IOException {
		try {
			return linkedMappedByteBuff.getShort();
		} catch (Exception bue) {
			throw new IOException(bue.toString());
		}
	}
	
	public String FTread() throws IOException {
		return RA.readLine();
	}
	
	public void FTwrite(String ins) throws IOException {
		RA.writeBytes(ins);
	}
	
	public void Fdelete() {
		WO.delete();
	}
	
	public String Fname() {
		return WO.getName();
	}
	
	public boolean isopen() {
		return fisopen;
	}
	
	public boolean iswriteable() {
		return true;
	}
	
	public boolean isnew() {
		return fisnew;
	}
	
	public Channel getChannel() {
		return FC;
	}
}
