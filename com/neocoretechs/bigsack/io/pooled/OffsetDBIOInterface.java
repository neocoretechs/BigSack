package com.neocoretechs.bigsack.io.pooled;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.neocoretechs.bigsack.io.Optr;
/*
* Copyright (c) 2002,2003, NeoCoreTechs
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
* Interface for process database IO.
* @author Groff
*/
public interface OffsetDBIOInterface {

	public short getByteindex();
	public void setByteindex(short tindex);
	/**
	* objseek - seek to offset within block
	* @param adr low level OID to seek to
	* @exception IOException thrown if seek header
	* @see Optr
	*/
	public void objseek(Optr adr) throws IOException;
	public void objseek(long blockNum) throws IOException;
	/**
	* seek_fwd - big seek forward from current spot
	* @param offset offset from current
	* @exception IOException if can't acquire next block
	*/
	public boolean seek_fwd(long offset) throws IOException;
	/**
	* seek_fwd - little seek forward from cur
	* @param offset offset from current
	* @exception IOException if can't acquire next block
	*/
	public boolean seek_fwd(short offset) throws IOException;
	/**
	* readi - read 1 byte from object / directory.
	* This method designed to be called from DBInput.
	* @return the byte as integer for InputStream
	* @exception IOException if can't acquire next block
	*/
	public int readi() throws IOException;
	public int readn(ByteBuffer bb, int numbyte) throws IOException;
	/**
	* writei -  write 1 byte to object / directory
	* This method designed to be called from DBOutput
	* Will overwrite to next blk if necessary.
	* @param byte to write
	* @exception IOException if can't acquire new block
	*/
	public void writei(int tbyte) throws IOException;
	public int writen(ByteBuffer bb, int numbyte)  throws IOException;

}
