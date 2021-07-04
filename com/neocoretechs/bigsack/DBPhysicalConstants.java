package com.neocoretechs.bigsack;

import com.neocoretechs.bigsack.io.pooled.Datablock;

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
* block header size is diff of DBLOCKSIZ - DATASIZE
* @author Jonathan Groff Copyright (C) NeoCoreTechs 1997,2003,2021
*/
public interface DBPhysicalConstants {
	/**
	 * Number of tablespaces per DB.  Cannot be altered.
	 */
	public static final int DTABLESPACES = 8; //Can't really mess with this
	
	/**
	 * The total block (page) size.
	 */
	public static final short DBLOCKSIZ = (short)8192;
	
	/**
	 * The size of payload data in a block (page). 
	 * - the size of the header portion
	 */
	public static final short DATASIZE = (short) (DBLOCKSIZ - Datablock.DATABLOCKHEADERSIZE);
	
	/**
	 * We can change the following constants dynamically, after DB creation, if necessary.
	 * 
	 * The number of blocks (pages) per tablespace. 
	 */
	public static int DBUCKETS = 1024;
	/**
	 * The backing store type "File", "MMap" etc, as supported in GlobalDBIO
	 */
	public static String BACKINGSTORE = "MMap";

}
