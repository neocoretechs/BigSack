package com.neocoretechs.bigsack.io.stream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.ref.SoftReference;

import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.MappedBlockBuffer;
/*
* Copyright (c) 1998,2003, NeoCoreTechs
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
* OutputStream writing directly from the DB blocks
* obviates need for intermediate byte array
* @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
*/
public final class DBOutputStream extends OutputStream {
	MappedBlockBuffer blockBuffer;
	BlockAccessIndex lbai;
	public DBOutputStream(BlockAccessIndex tlbai, MappedBlockBuffer tsdbio) {
		lbai =  tlbai;
		//tlbai.getBlk().setIncore(true);
		blockBuffer = tsdbio;
	}
	/**
	 * Allows us to replace the underlying deep store blocks for data that spans multiple blocks
	 * To the underlying stream it should appear as one continuous stream
	 * @param tlbai
	 * @param tsdbio
	 */
	public void replaceSource(BlockAccessIndex tlbai, MappedBlockBuffer tsdbio) {
		lbai = tlbai;
		blockBuffer = tsdbio;
	}
	
	@Override
	public void write(byte[] b) throws IOException {
		blockBuffer.writen(lbai, b, b.length);
	}
		
	//Reads bytes from this byte-input stream into the specified byte array, starting at the given offset.
	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		for(int i = off; i < len; i++)
			blockBuffer.writei(lbai, b[i]);
	}
		
	@Override
	public void write(int b) throws IOException {
		blockBuffer.writei(lbai, b);
	}
	
}
