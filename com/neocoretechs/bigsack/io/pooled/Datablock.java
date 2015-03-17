package com.neocoretechs.bigsack.io.pooled;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.IoInterface;

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
* Datablock - Class for doubly linked list DB block (page)
* composed of header and data payload whose total size is the constant DBLOCKSIZ
* The usual pattern is to have these methods call back through an IoInterface to perform
* specific low level record writes. IoInterface is accessed through a request that has been queued
* and is being serviced, thus, direct calls back to the file store are appropriate.
* Copyright (C) NeoCoreTechs 1997,2014
* @author Groff
*/
public final class Datablock implements Externalizable {
	private static boolean DEBUG = false;
	private long prevblk = -1L; // offset to prev blk in chain
	private long nextblk = -1L; // offset of next blk in chain
	private short bytesused; // bytes used this blk-highwater mark
	private short bytesinuse; // actual # of bytes in use
	private long pageLSN = -1L; // pageLSN number of this block
	byte data[]; // data section of blk
	private boolean incore = false; // is it modified?
	private boolean inlog = false; // written to log since incore?
	private static final long serialVersionUID = 1L;
	//
	private int datasize;
	//
	public Datablock() {
		this(DBPhysicalConstants.DATASIZE);
	}
	
	public Datablock(int tdatasize) {
		datasize = tdatasize;
		data = new byte[datasize];
	}
	/**
	* Write the header and data portion to IoInterface implementor.
	* Primarily for freechain create as we used writeUsed otherwise
	* @param fobj the IoInterface
	* @exception IOException error writing field
	*/
	public synchronized void  write(IoInterface fobj) throws IOException {
		//synchronized(fobj) {
			fobj.Fwrite_long(getPrevblk());
			fobj.Fwrite_long(getNextblk());
			fobj.Fwrite_short(getBytesused());
			fobj.Fwrite_short(getBytesinuse());
			fobj.Fwrite_long(getPageLSN());
			fobj.Fwrite(data);
		//}
	}
	/**
	* write the header and used data portion to IoInterface implementor
	* @param fobj the IoInterface
	* @exception IOException error writing field
	*/
	public synchronized void writeUsed(IoInterface fobj) throws IOException {
		//synchronized(fobj) {
			fobj.Fwrite_long(getPrevblk());
			fobj.Fwrite_long(getNextblk());
			fobj.Fwrite_short(getBytesused());
			fobj.Fwrite_short(getBytesinuse());
			fobj.Fwrite_long(getPageLSN());
			if (getBytesused() == datasize)
				fobj.Fwrite(data);
			else
				fobj.Fwrite(data, getBytesused());
		//}
	}

	
	public synchronized void resetBlock() {
		if( DEBUG )
		System.out.println("Datablock,resetBlock "+this);
		prevblk = -1L;
		nextblk = -1L;
		bytesused = 0;
		bytesinuse = 0;
		pageLSN = -1;
		incore = false;
		inlog = false;
	}
	/**
	* write the header and data portion to IoInterface implementor
	* in compressed form
	* @param fobj the IoInterface
	* @exception IOException error writing field
	*/
	/*        protected synchronized void writeCompressed(IoInterface fobj) throws IOException
	        {
	                fobj.Fwrite_long(prevblk);
	                fobj.Fwrite_long(nextblk);
	                fobj.Fwrite_short(bytesused);
	                fobj.Fwrite_short(bytesinuse);
	                fobj.Fwrite_long(pageLSN);
	                ByteArrayOutputStream baos = new ByteArrayOutputStream();
	                DeflaterOutputStream dfo = new DeflaterOutputStream(baos);
	                dfo.write(data,0,data.length);
	                dfo.close();
	                baos.close();
	                byte[] bx = baos.toByteArray();
	                fobj.Fwrite(bx);
	        }
	*/
	/**
	* read the header and data portion from IoInterface implementor
	* @param fobj the IoInterface
	* @exception IOException error reading field
	*/
	public synchronized void read(IoInterface fobj) throws IOException {
		//synchronized(fobj) {
			setPrevblk(fobj.Fread_long());
			setNextblk(fobj.Fread_long());
			setBytesused(fobj.Fread_short());
			setBytesinuse(fobj.Fread_short());
			setPageLSN(fobj.Fread_long());
			if (fobj.Fread(data, datasize) != datasize) {
				throw new IOException(
						"Datablock read size invalid " + this.toString());
			}
		//}
	}
	/**
	* read the header and used data portion from IoInterface implementor
	* @param fobj the IoInterface
	* @exception IOException error reading field
	*/
	public synchronized void readUsed(IoInterface fobj) throws IOException {
		//synchronized(fobj) {
			setPrevblk(fobj.Fread_long());
			setNextblk(fobj.Fread_long());
			setBytesused(fobj.Fread_short());
			setBytesinuse(fobj.Fread_short());
			setPageLSN(fobj.Fread_long());
			if (getBytesused() > datasize) {
				throw new IOException("block inconsistency " + this.toString());
			}
			if (getBytesused() == datasize) {
				if (fobj.Fread(data) != datasize) {
					throw new IOException(
							"Datablock read size invalid " + this.toString());
				}
			} else {
				if (fobj.Fread(data, getBytesused()) != getBytesused()) { 
					throw new IOException(
					"Datablock read error bytesused="
						+ String.valueOf(getBytesused())
						+ " bytesinuse="
						+ String.valueOf(getBytesinuse()));
				}
			}
		//}
	}
	
	/**
	Write this out.
	@exception IOException error writing to log stream
	 */
	public synchronized void writeExternal(ObjectOutput out) throws IOException
	{
		out.writeLong(getPrevblk());
		out.writeLong(getNextblk());
		out.writeShort(getBytesused());
		out.writeShort(getBytesinuse());
		out.writeLong(getPageLSN());
		if (getBytesused() == datasize)
			out.write(data);
		else
			out.write(data, 0, getBytesused());
	}

	/**
	Read this in
	@exception IOException error reading from log stream
	@exception ClassNotFoundException corrupted log stream
	 */
	public synchronized void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
	{
		setPrevblk(in.readLong());
		setNextblk(in.readLong());
		setBytesused(in.readShort());
		setBytesinuse(in.readShort());
		setPageLSN(in.readLong());
		in.read(data);
		//if (in.read(data) != datasize) {
		//	throw new IOException(
		//		"Datablock read size invalid " + this.toString());
		//}
	}
	
	/** for debugging, write block info */
	public synchronized String blockdump() {
		int nzero=0;
		for(int i =0;i<datasize;i++) {
		        if(data[i] != 0) {
		               ++nzero;
		        }
		}
		return "Blockdump "+this.toString()+" "+(nzero == 0 ? "NO Non-Zero elements found" : nzero+" non-zero elements found");
		
	}

	/**
	* deep copy
	* @return The clone of this block
	*/
	synchronized Datablock doClone() {
		Datablock d = new Datablock(DBPhysicalConstants.DATASIZE);
		d.setPrevblk(prevblk);
		d.setNextblk(nextblk);
		d.setBytesused(bytesused);
		d.setBytesinuse(bytesinuse);
		System.arraycopy(data, 0, d.data, 0, getBytesused());
		d.setIncore(true);
		return d;
	}
	/**
	* deep copy
	* @param d The block whose values are set from 'this' instance
	*/
	public synchronized void doClone(Datablock d) {
		d.setPrevblk(prevblk);
		d.setNextblk(nextblk);
		d.setBytesused(bytesused);
		d.setBytesinuse(bytesinuse);
		d.setPageLSN(pageLSN);
		System.arraycopy(data, 0, d.data, 0, getBytesused());
		d.setIncore(incore);
		d.setInlog(inlog);
	}
	
	public synchronized String toString() {
		//String o = new String("Elems all 0");
		//for(int i =0;i<datasize;i++) {
		//        if(data[i] != 0) {
		//                o = new String("Some elems not 0");
		//                break;
		//        }
		//} o+=
		//String o =
		return	"DBLK: prev = "
				+ prevblk
				+ " next = "
				+ nextblk
				+ " bytesused = "
				+ bytesused
				+ " bytesinuse = "
				+ bytesinuse
				+ " pageLSN: "
				+ pageLSN
				+ " incore "
				+ incore;
		//return o;
	}
	public synchronized short getBytesinuse() {
		return bytesinuse;
	}
	public synchronized void setBytesinuse(short bytesinuse) {
		this.bytesinuse = bytesinuse;
	}
	
	public synchronized String toBriefString() {
		return ( prevblk !=-1 || nextblk !=-1 || bytesused != 0 || bytesinuse != 0 || 
				 pageLSN != -1 || incore) ?
				"DBLK prev = "
					+ GlobalDBIO.valueOf(getPrevblk())
					+ " next = "
					+ GlobalDBIO.valueOf(getNextblk())
					+ " bytesused = "
					+ bytesused
					+ " bytesinuse = "
					+ bytesinuse
					+ " pageLSN: "
					+ pageLSN
					+ " incore "
					+ incore
			:  "[[ Block Empty ]]";
	}
	public synchronized boolean isIncore() {
		return incore;
	}
	public synchronized void setIncore(boolean incore) {
		this.incore = incore;
	}
	public synchronized long getPrevblk() {
		return prevblk;
	}
	public synchronized void setPrevblk(long prevblk) {
		this.prevblk = prevblk;
	}
	public synchronized long getNextblk() {
		return nextblk;
	}
	public synchronized void setNextblk(long nextblk) {
		this.nextblk = nextblk;
	}
	public synchronized short getBytesused() {
		return bytesused;
	}
	public synchronized void setBytesused(short bytesused) {
		this.bytesused = bytesused;
	}
	public synchronized long getPageLSN() {
		return pageLSN;
	}
	public synchronized void setPageLSN(long version) {
		this.pageLSN = version;
	}
	public synchronized boolean isInlog() {
		return inlog;
	}
	public synchronized void setInlog(boolean inlog) {
		this.inlog = inlog;
	}
	public synchronized byte[] getData() { return data; }

}
