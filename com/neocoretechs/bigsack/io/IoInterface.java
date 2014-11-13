package com.neocoretechs.bigsack.io;
import java.io.*;
import java.nio.channels.*;
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
* Pool I/O interface; allows us to plug in various I/O modules by
* implementing this interface.  Some methods may have no context for a
* given implementation (such as writing a URL)
* @author Groff
*/
public interface IoInterface {
	/**
	* @param fname The fully qualified name of file
	* @param create true for 'create if not existing'
	* @exception IOException if open/create fails
	*/
	public boolean Fopen(String fname, boolean create) throws IOException;
	/**
	* re-open file/stream
	* @return true if success
	* @exception IOException if open fails
	*/
	public void Fopen() throws IOException;
	/**
	* close file
	* @exception IOException if close fails
	*/
	public void Fclose() throws IOException;
	/**
	* @return current byte location
	* @exception IOException if ftell fails
	*/
	public long Ftell() throws IOException;
	/**
	* @param offset the offset into file/stream
	* @return true if can do
	* @exception IOException if seek fails
	*/
	public void Fseek(long offset) throws IOException;
	/**
	* @return size of data set in bytes
	* @exception IOException if seek fails
	*/
	public long Fsize() throws IOException;
	/**
	* Set the file length
	* @param newlen The new file length
	* @exception IOException if length set fails
	*/
	public void Fset_length(long newlen) throws IOException;
	/**
	* For modes that need forced write to flush buffers
	* @exception IOException if flush fails
	*/
	public void Fforce() throws IOException;
	/**
	* writing buffer
	* @param obuf the byte buffer to write
	* @exception IOException if write fails
	*/
	public void Fwrite(byte[] obuf) throws IOException;
	/**
	* writing buffer
	* @param obuf the byte buffer to write
	* @param osiz number to write
	* @exception IOException if write fails
	*/
	public void Fwrite(byte[] obuf, int osiz) throws IOException;
	/**
	* write an int value
	* @exception IOException if write fails
	*/
	public void Fwrite_int(int obuf) throws IOException;
	/**
	* write a long value
	* @exception IOException if write fails
	*/
	public void Fwrite_long(long obuf) throws IOException;
	/**
	* write a short value
	* @exception IOException if write fails
	*/
	public void Fwrite_short(short obuf) throws IOException;
	/**
	* read buffer
	* @param b the buffer to fill
	* @param osiz number bytes
	* @exception IOException if read fails
	*/
	public int Fread(byte[] b, int osiz) throws IOException;
	/**
	* read buffer
	* @param b the buffer to fill
	* @exception IOException if read fails
	*/
	public int Fread(byte[] b) throws IOException;
	/**
	* read a long
	* @exception IOException if read fails
	*/
	public long Fread_long() throws IOException;
	/**
	* read an int
	* @exception IOException if read fails
	*/
	public int Fread_int() throws IOException;
	/**
	* read a short
	* @exception IOException if read fails
	*/
	public short Fread_short() throws IOException;
	/**
	* text read; reads until EOL
	* @exception IOException if read fails
	*/
	public String FTread() throws IOException;
	/**
	* text write
	* @exception IOException if write fails
	*/
	public void FTwrite(String ins) throws IOException;
	/**
	* delete the file
	*/
	public void Fdelete();
	/**
	* @return proper name
	*/
	public String Fname();
	/**
	* @return true if open
	*/
	public boolean isopen();
	/**
	* @return true if writeable
	*/
	public boolean iswriteable();

	public boolean isnew();

	public Channel getChannel();
}
