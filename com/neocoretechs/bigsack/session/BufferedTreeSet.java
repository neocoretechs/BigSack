package com.neocoretechs.bigsack.session;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.stream.Stream;

import com.neocoretechs.bigsack.keyvaluepages.KeySearchResult;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
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
* BufferedTreeSet. We use the BigSackSession object here. Recovery is supported transparently
* and transactions are transparent to the user.
* The user is not concerned with semantics of recovery when using this construct. The commit
* operations are performed after each insert and recovery takes place if a failure occurs during
* runtime writes. If transparency with existing code is paramount this class is a good choice.
* Thread safety is with the session object using session.getMutexObject().
* Java TreeSet backed by pooled serialized objects.<br>
* @author Jonathan Groff (C) NeoCoreTechs 2003, 2017, 2021
*/
public class BufferedTreeSet extends BufferedSet {
	/**
	 * 
	 * @param dbname
	 * @param backingStore
	 * @param poolBlocks
	 * @throws IOException
	 * @throws IllegalAccessException
	 */
	public BufferedTreeSet(String dbname, String backingStore, int poolBlocks) throws IOException, IllegalAccessException {
		super(dbname, "BTree", backingStore, poolBlocks);
	}
	

}
