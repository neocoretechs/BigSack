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
	private static boolean DEBUG = false;
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
	public synchronized boolean Fopen(String fname, boolean create) throws IOException {
		if( DEBUG )
			System.out.println("MMapIO.Fopen "+fname+" "+create);
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
	public synchronized void Fopen() throws IOException {
		if( DEBUG )
			System.out.println("MMapIO.Fopen "+WO);
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
	public synchronized void Fclose() throws IOException {
		if( DEBUG )
			System.out.println("MMapIO.Fclose "+WO);
		if (fisopen) {
			Fforce();
			FC.close();
			RA.close();
			fisopen = false;
		}
	}
	
	public synchronized long Ftell() throws IOException {
		if( DEBUG )
			System.out.println("MMapIO.Ftell "+linkedMappedByteBuff.position());
		return linkedMappedByteBuff.position();
	}
	
	public synchronized void Fseek(long offset) throws IOException {
		if( DEBUG )
			System.out.println("MMapIO.Fseek "+offset+" from pos:"+linkedMappedByteBuff.position());
		linkedMappedByteBuff.position((int) offset);
	}
	
	public synchronized long Fsize() throws IOException {
		if( DEBUG )
			System.out.println("MMapIO.Fsize "+linkedMappedByteBuff.capacity());
		return linkedMappedByteBuff.capacity();
	}
	
	public synchronized void Fset_length(long newlen) throws IOException {
		if( DEBUG )
			System.out.println("MMapIO.Fset_length NEW LENGTH:"+newlen);
		if (newlen < FC.size()) {
			linkedMappedByteBuff.force();
			linkedMappedByteBuff = null;
			System.gc();
			FC.truncate(newlen);
			FC.force(true);
			//linkedMappedByteBuff = FC.map(FileChannel.MapMode.READ_WRITE, 0, newlen);
			linkedMappedByteBuff = new LinkedMappedByteBuffer(FC, newlen);
		} else if (newlen > FC.size())
			Fextend(newlen);
	}
	
	public synchronized void Fforce() throws IOException {
		if( DEBUG )
			System.out.println("MMapIO.Fforce ATTEMPTING FORCE");
		linkedMappedByteBuff.force();
		FC.force(true);
		RA.getFD().sync();
	}
	/**
	 * Position file at newSize - 1 block, and write blank block
	 * @param newSize
	 * @throws IOException
	 */
	private synchronized void Fextend(long newSize) throws IOException {
		if( DEBUG )
			System.out.println("MMapIO.Fextend "+newSize);
		FC.position(newSize - DBPhysicalConstants.DBLOCKSIZ);
		FC.write(bPageBuff);
		bPageBuff.rewind();
		FC.force(false);
	}
	/**
	 * Extend by 1 block by writing page buffer if position is at capacity value
	 * @throws IOException
	 */
	@SuppressWarnings("unused")
	private synchronized void Fextend() throws IOException {
		if( DEBUG )
			System.out.println("MMapIO.Fextend ");
		long fPos = linkedMappedByteBuff.position();
		if (fPos == linkedMappedByteBuff.capacity()) {
			FC.write(bPageBuff);
			bPageBuff.rewind();
			FC.force(false);
		}
	}
	// writing..
	public synchronized void Fwrite(byte[] obuf) throws IOException {
		if( DEBUG )
			System.out.println("MMapIO.Fwrite @"+linkedMappedByteBuff.position()+" bytes:"+obuf.length);
		linkedMappedByteBuff.put(obuf);
	}
	
	public synchronized void Fwrite(byte[] obuf, int osiz) throws IOException {
		if( DEBUG )
			System.out.println("MMapIO.Fwrite @"+linkedMappedByteBuff.position()+" bytes:"+obuf.length+" size:"+osiz);
		linkedMappedByteBuff.put(obuf, 0, osiz);
	}
	
	public synchronized void Fwrite_int(int obuf) throws IOException {
		if( DEBUG )
			System.out.println("MMapIO.Fwrite_int @"+linkedMappedByteBuff.position()+" val:"+obuf);	
		linkedMappedByteBuff.putInt(obuf);
	}
	
	public synchronized void Fwrite_long(long obuf) throws IOException {
		if( DEBUG )
			System.out.println("MMapIO.Fwrite_long @"+linkedMappedByteBuff.position()+" val:"+obuf);
		linkedMappedByteBuff.putLong(obuf);
	}
	
	public synchronized void Fwrite_short(short obuf) throws IOException {
		if( DEBUG )
			System.out.println("MMapIO.Fwrite_short @"+linkedMappedByteBuff.position()+" val:"+obuf);
		linkedMappedByteBuff.putShort(obuf);
	}
	
	public synchronized void Fwrite_byte(byte obuf) throws IOException {
		if( DEBUG )
			System.out.println("MMapIO.Fwrite_short @"+linkedMappedByteBuff.position()+" val:"+obuf);
		linkedMappedByteBuff.putByte(obuf);	
	}
	//
	// reading...
	//
	public synchronized int Fread(byte[] b, int osiz) throws IOException {
		if( DEBUG )
			System.out.println("MMapIO.Fread @"+linkedMappedByteBuff.position()+" buf:"+b.length+" size:"+osiz);
		linkedMappedByteBuff.get(b, 0, osiz);
		return osiz;
	}
	
	public synchronized int Fread(byte[] b) throws IOException {
		if( DEBUG )
			System.out.println("MMapIO.Fread @"+linkedMappedByteBuff.position()+" buf length:"+b.length);
		linkedMappedByteBuff.get(b);
		return b.length;
	}
	
	public synchronized int Fread_int() throws IOException {
		if( DEBUG ) {	
			int i = linkedMappedByteBuff.getInt();
			System.out.println("MMapIO.Fread_int @"+linkedMappedByteBuff.position()+" value:"+i);
			return i;
		}
		return linkedMappedByteBuff.getInt();	
	}
	
	public synchronized long Fread_long() throws IOException {
		if( DEBUG ) {
			long l = linkedMappedByteBuff.getLong();
			System.out.println("MMapIO.Fread_long @"+linkedMappedByteBuff.position()+" value:"+l);
			return l;
		}
		return linkedMappedByteBuff.getLong();
	}
	
	public synchronized short Fread_short() throws IOException {
		if( DEBUG ) {
			short s = linkedMappedByteBuff.getShort();
			System.out.println("MMapIO.Fread_short @"+linkedMappedByteBuff.position()+" value:"+s);
			return s;
		}
		return linkedMappedByteBuff.getShort();
	}

	public synchronized byte Fread_byte() throws IOException {
		if( DEBUG ) {
			byte s = linkedMappedByteBuff.get();
			System.out.println("MMapIO.Fread_byte @"+linkedMappedByteBuff.position()+" value:"+s);
			return s;
		}
		return linkedMappedByteBuff.get();
	}
	
	public synchronized String FTread() throws IOException {
		if( DEBUG )
			System.out.println("MMapIO.FTread @"+linkedMappedByteBuff.position());
		return RA.readLine();
	}
	
	public synchronized void FTwrite(String ins) throws IOException {
		if( DEBUG )
			System.out.println("MMapIO.FTwrite @"+linkedMappedByteBuff.position()+" len:"+ins.length());
		RA.writeBytes(ins);
	}
	
	public synchronized void Fdelete() {
		if( DEBUG )
			System.out.println("MMapIO.FDelete");
		WO.delete();
	}
	
	public synchronized String Fname() {
		return WO.getName();
	}
	
	public synchronized boolean isopen() {
		return fisopen;
	}
	
	public synchronized boolean iswriteable() {
		return true;
	}
	
	public synchronized boolean isnew() {
		return fisnew;
	}
	
	public synchronized Channel getChannel() {
		if( DEBUG )
			System.out.println("MMapIO.getChannel:"+FC);
		return FC;
	}

}
