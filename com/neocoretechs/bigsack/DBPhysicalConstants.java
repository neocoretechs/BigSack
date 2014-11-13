package com.neocoretechs.bigsack;
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
* Defines physical constants for tablespaces, buckets, etc
* @author Groff
*/
public interface DBPhysicalConstants {
	//
	//block header size is diff of DBLOCKSIZ - DATASIZE
	//public static final short DATASIZE = 988; //size of data portion of block
	//public static final short DBLOCKSIZ = 1024;  // total block size
	//public static final int DBUCKETS = 4096;
	/**
	 * The size of payload data in a block (page).  Determined by BlockSize in properties file
	 * - the size of the header portion
	 */
	public static final short DATASIZE =
		(short) (Props.toInt("BlockSize") - 36);
	//If total 4096 then 4060 is size of data portion of block
	/**
	 * The total block (page) size.  Determined by BlockSize in properties file
	 */
	public static final short DBLOCKSIZ = (short) Props.toInt("BlockSize");
	//4096 total block size typical
	/**
	 * The number of blocks (pages) per tablespace.  Determined by Buckets in properties file
	 */
	public static final int DBUCKETS = Props.toInt("Buckets");
	//4096 number of table buckets typical
	/**
	 * Number of tablespaces per DB.  Cannot be easily altered.
	 */
	public static final int DTABLESPACES = 8; //Can't really mess with this


}
