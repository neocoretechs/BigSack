package com.neocoretechs.bigsack.io;
import java.io.Serializable;

import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
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
* Optr - low level object db pointer where a combination of file block and offset determine position<br>
* Block and offset locate Datablock and byte offset within that Datablock
* @author Groff
*/
public final class Optr implements Serializable {
	private static final long serialVersionUID = 513878730827370903L;
	static final short ZERO = 0;
	public static Optr valueOf(long blk, short offset) { return new Optr(blk, offset); }
	public static Optr valueOf(long blk) { return new Optr(blk, (short) 0); }
    private long block;// first blk of object
    private short offset; // byte offset in block
    //
    public Optr() {}
    public Optr(long tblk, short toff) { this.block = tblk; this.offset = toff; }

	public short getOffset() {
		return offset;
	}
	public void setOffset(short offset) {
		this.offset = offset;
	}
	public long getBlock() {
		return block;
	}
	public void setBlock(long block) {
		this.block = block;
	}
	public static Optr getEmptyPointer() { return new Optr(-1, (short) -1); }
	
	public boolean isEmptyPointer() { return ( this.block == -1 && this.offset == -1); }
	
	@Override
	public boolean equals(Object opt) {
		if( ((Optr)opt).getBlock() == this.block && ((Optr)opt).getOffset() == this.offset )
			return true;
		return false;
	}
	@Override
	public String toString() {
		return this.isEmptyPointer() ? "Empty" : (GlobalDBIO.valueOf(this.block)+","+String.valueOf(this.offset));
	}
}

