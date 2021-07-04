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
* 
* @author Jonathan Groff Copyright (C) NeoCoreTechs 1997,2014,2021
*/
public final class Datablock implements Externalizable {
	private static boolean DEBUG = false;
	public static final int DATABLOCKHEADERSIZE = 21;
	private long prevblk = -1L; // offset to prev blk in chain
	private long nextblk = -1L; // offset of next blk in chain
	private short bytesused; // bytes used this blk-highwater mark
	private short bytesinuse; // actual # of bytes in use
	private byte inlog = 0; // written to log since incore?
	byte data[]; // data section of blk
	private boolean incore = false; // is it modified?
	private static final long serialVersionUID = 1L;
	//
	private int datasize;
	/**
	 * Initialize the datablock with default DBPhysicalConstants.DATASIZE 
	 */
	public Datablock() {
		this(DBPhysicalConstants.DATASIZE);
	}
	/**
	 * Initialize the datablock with the given size
	 * @param tdatasize The size to initialize the block
	 */
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
	public synchronized void write(IoInterface fobj) throws IOException {
			fobj.Fwrite_long(getPrevblk());
			fobj.Fwrite_long(getNextblk());
			fobj.Fwrite_short(getBytesused());
			fobj.Fwrite_short(getBytesinuse());
			fobj.Fwrite_byte(inlog);
			fobj.Fwrite(data);
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
			fobj.Fwrite_byte(inlog);
			if (getBytesused() == datasize)
				fobj.Fwrite(data);
			else
				fobj.Fwrite(data, getBytesused());
		//}
	}

	/**
	* Write the header portion to IoInterface implementor.
	* Primarily for log related operations.
	* @param fobj the IoInterface
	* @exception IOException error writing field
	*/
	public synchronized void writeHeader(IoInterface fobj) throws IOException {
			fobj.Fwrite_long(getPrevblk());
			fobj.Fwrite_long(getNextblk());
			fobj.Fwrite_short(getBytesused());
			fobj.Fwrite_short(getBytesinuse());
			fobj.Fwrite_byte(inlog);
	}
	
	/**
	 * Sets up default header. prevblk = -1L, nextblk = -1L, bytesused, bytesinuse = 0 <p/>
	 * isKeypage = 0, incore = false, inlog = false
	 */
	public synchronized void resetBlock() {
		if( DEBUG )
			System.out.println("Datablock,resetBlock "+this);
		prevblk = -1L;
		nextblk = -1L;
		bytesused = 0;
		bytesinuse = 0;
		incore = false;
		inlog = 0;
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
			setInLog(fobj.Fread_byte());
			if (fobj.Fread(data, datasize) != datasize) {
				throw new IOException(
						"Datablock read size invalid " + this.toString());
			}
		//}
	}
	
	/**
	* Read the header portion from IoInterface implementor
	* @param fobj the IoInterface
	* @exception IOException error reading field
	*/
	public synchronized void readHeader(IoInterface fobj) throws IOException {
			setPrevblk(fobj.Fread_long());
			setNextblk(fobj.Fread_long());
			setBytesused(fobj.Fread_short());
			setBytesinuse(fobj.Fread_short());
			setInLog(fobj.Fread_byte());
	}
	
	public void setInLog(byte fbyte) {
		setInlog((fbyte == 0 ? false : true));	
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
			setInLog(fobj.Fread_byte());
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
	* Write this out.
	* @throws IOException error writing to log stream
	*/
	public synchronized void writeExternal(ObjectOutput out) throws IOException
	{
		out.writeLong(getPrevblk());
		out.writeLong(getNextblk());
		out.writeShort(getBytesused());
		out.writeShort(getBytesinuse());
		out.writeByte(inlog);
		if (getBytesused() == datasize)
			out.write(data);
		else
			out.write(data, 0, getBytesused());
	}

	/**
	* Read this in
	* @throws IOException error reading from log stream
	* @throws ClassNotFoundException corrupted log stream
	*/
	public synchronized void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
	{
		setPrevblk(in.readLong());
		setNextblk(in.readLong());
		setBytesused(in.readShort());
		setBytesinuse(in.readShort());
		setInLog(in.readByte());
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
		return "Blockdump "+this.toString()+" "+(nzero == 0 ? "NO Non-Zero bytes found" : nzero+" non-zero bytes found");
		
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
		d.setInLog(inlog);
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
		System.arraycopy(data, 0, d.data, 0, getBytesused());
		d.setIncore(incore);
		d.setInLog(inlog);
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
		StringBuilder sb = new StringBuilder("DBLK: prev = ");
		sb.append(GlobalDBIO.valueOf(getPrevblk()));
		sb.append(" next = ");
		sb.append(GlobalDBIO.valueOf(getNextblk()));
		sb.append(" bytesused = ");
		sb.append(bytesused);
		sb.append(" bytesinuse = ");
		sb.append(bytesinuse);
		sb.append( " incore ");
		sb.append( incore);
		sb.append(" inlog ");
		sb.append(inlog);
		return sb.toString();		
		//return o;
	}
	/**
	 * Actual # of bytes in use
	 * @return The actual number of bytes in use in this block.
	 */
	public synchronized short getBytesinuse() {
		return bytesinuse;
	}
	/**
	 * Set the number of bytes in use for this block
	 * @param bytesinuse the number of bytes in use in any location in the block
	 */
	public synchronized void setBytesinuse(short bytesinuse) {
		this.bytesinuse = bytesinuse;
	}
	/**
	 * Return a non-verbose rendition of this data block.<p/>
	 * <li>
	 * previous<dd/>
	 * next<dd/>
	 * bytesused<dd/>
	 * bytes in use <dd/>
	 * page # <dd/>
	 * is key page?<dd/>
	 * is in core?<dd/>
	 * is block empty?<dd/>
	 * </li>
	 * @return
	 */
	public synchronized String toBriefString() {
		return ( prevblk !=-1 || nextblk !=-1 || bytesused != 0 || bytesinuse != 0 || incore || inlog != 0) ?
				String.format("DBLK prev = %s next = %s bytesused =%d bytesinuse =%d incore =%b inlog =%b",
					GlobalDBIO.valueOf(getPrevblk())
					,GlobalDBIO.valueOf(getNextblk())
					,bytesused
					,bytesinuse
					,incore
					,(inlog != 0 ? true : false))
			:  "[[ Block Empty ]]";
	}
	/**
	 * Return whether this block is in core
	 * @return boolean true if block is active, being written or accessed
	 */
	public synchronized boolean isIncore() {
		return incore;
	}
	/**
	 * Set whether this block is active, written, aceesed, etc.
	 * @param incore boolean true if active
	 */
	public synchronized void setIncore(boolean incore) {
		this.incore = incore;
	}
	/**
	 * Previous block
	 * @return the long value of previous block or -1L if none
	 */
	public synchronized long getPrevblk() {
		return prevblk;
	}
	/**
	 * Set previous block
	 * @param prevblk the long value of previous block
	 */
	public synchronized void setPrevblk(long prevblk) {
		this.prevblk = prevblk;
	}
	/**
	 * Get next block
	 * @return the long value of next block or -1L if none
	 */
	public synchronized long getNextblk() {
		return nextblk;
	}
	/**
	 * Set next block
	 * @param nextblk Long value of next block or -1L if none.
	 */
	public synchronized void setNextblk(long nextblk) {
		this.nextblk = nextblk;
	}
	/**
	 * Return the number of bytes used in this block, the high water mark.
	 * @return short value of number of bytes used high water mark this block.
	 */
	public synchronized short getBytesused() {
		return bytesused;
	}
	/**
	 * Set high water mark bytes used this block.
	 * @param bytesused short value of bytes used high water mark.
	 */
	public synchronized void setBytesused(short bytesused) {
		this.bytesused = bytesused;
	}

	/**
	 * Is page in log?
	 * @return true if page is in log.
	 */
	public synchronized boolean isInlog() {
		return inlog != 0;
	}
	/**
	 * Set whether page is in log.
	 * @param inlog true if page is in log.
	 */
	public synchronized void setInlog(boolean inlog) {
		this.inlog = (byte) (inlog ? 1 : 0);
	}
	/**
	 * Get the data payload of this page.
	 * @return The byte array holding the data payload of this page.
	 */
	public synchronized byte[] getData() { return data; }
	
	/**
	 * Is this block effectively empty? i.e. not linked to anything (prev and nextblk == -1L) and
	 * contains no data bytesinuse = 0.
	 * @return boolean value of result of empty block check, true if empty.
	 */
	public boolean isEmpty() {
		return (getPrevblk() == -1L && getNextblk() == -1L && getBytesinuse() == 0);	
	}

}
