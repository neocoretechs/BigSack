package com.neocoretechs.bigsack.io;
import java.nio.channels.*;
import java.io.*;
/*
* Copyright (c) 1997,2003, NeoCoreTechs
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
* Generalized file I/O encapsulating a random access file.
* @see IoInterface
* Copyright NeoCoreTechs 2003,2014
* @author Groff
*/
public final class FileIO implements IoInterface {
	private boolean fisopen, fisnew;
	private File WO;
	private FileOutputStream FO;
	private RandomAccessFile RA;
	
	public FileIO(String fname, boolean create) throws IOException {
		Fopen(fname, create);
	}
	public FileIO() {}
	
	/** 
	 * create is true for 'create if not existing' 
	 * Open the file, performing the proper initialization on creation
	 */
	public synchronized boolean Fopen(String fname, boolean create) throws IOException {
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
		fisopen = true;
		return true;
	}
	
	/**
	 * Open the random access file encapsulated by constructor
	 * mode is rw
	 */
	public synchronized void Fopen() throws IOException {
		RA = new RandomAccessFile(WO, "rw");
		fisnew = false;
		fisopen = true;
	}
	/**
	 * Close the encapsulated random access file
	 */
	public synchronized void Fclose() throws IOException {
		if (fisopen) {
			fisopen = false;
			Fforce();
			RA.close();
		}
	}
	/**
	 * Return the result of getFilePointer on encapsulated randomaccessfile
	 */
	public synchronized long Ftell() throws IOException {
		return RA.getFilePointer();
	}
	/**
	 * See the designated position in encapsulated random access file
	 */
	public synchronized void Fseek(long offset) throws IOException {
		RA.seek(offset);
	}
	public synchronized long Fsize() throws IOException {
		return RA.length();
	}
	/**
	 * Set the length of the encapsulated RandomAccessFile
	 */
	public synchronized void Fset_length(long newlen) throws IOException {
		RA.setLength(newlen);
	}
	/**
	 * Get the file descriptor of the encapsulated RandomAccessFile and perform a 'sync' upon it.
	 * The will guarantee a flush of the filesystem buffers
	 */
	public synchronized void Fforce() throws IOException {
		RA.getFD().sync();
	}

	/**
	 * Write the byte buffer to the encapsulated random access file
	 */
	public synchronized void Fwrite(byte[] obuf) throws IOException {
		RA.write(obuf);
	}
	public synchronized void Fwrite(byte[] obuf, int osiz) throws IOException {
		RA.write(obuf, 0, osiz);
	}
	public synchronized void Fwrite_int(int obuf) throws IOException {
		RA.writeInt(obuf);
	}
	public synchronized void Fwrite_long(long obuf) throws IOException {
		RA.writeLong(obuf);
	}
	public synchronized void Fwrite_short(short obuf) throws IOException {
		RA.writeShort(obuf);
	}
	/**
	 * Read from the encap random file
	 */
	public synchronized int Fread(byte[] b, int osiz) throws IOException {
		return RA.read(b, 0, osiz);
	}
	public synchronized int Fread(byte[] b) throws IOException {
		return RA.read(b);
	}
	public synchronized int Fread_int() throws IOException {
		return RA.readInt();
	}
	public synchronized long Fread_long() throws IOException {
		return RA.readLong();
	}
	public synchronized short Fread_short() throws IOException {
		return RA.readShort();
	}
	public synchronized String FTread() throws IOException {
		return RA.readLine();
	}
	public synchronized void FTwrite(String ins) throws IOException {
		RA.writeBytes(ins);
	}
	public synchronized void Fdelete() {
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
		return RA.getChannel();
	}
}
