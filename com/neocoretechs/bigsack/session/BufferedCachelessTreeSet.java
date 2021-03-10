package com.neocoretechs.bigsack.session;
import java.io.IOException;
import java.util.Iterator;
import java.util.stream.Stream;

import com.neocoretechs.bigsack.btree.TreeSearchResult;
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
* Wrapper for BufferedTreeSet.
* Java TreeSet backed by pooled serialized objects.  
* This class can be used for debugging and benchmarking, or if you need to 
* support multiple instances for writing for some reason.
* It uses no level 1 cache; no deserialized instance in-memory cache
* @author Jonathan Groff (C) NeoCoreTechs 2020,2021
*/
public class BufferedCachelessTreeSet {
	protected BigSackSession session;
	/**
	* Get instance of BigSack session.
	* Each new instance of this will connect to the same backing store
	* @param tdbname The database name
	* @exception IOException if global IO problem
	* @exception IllegalAccessException if the database has been put offline by admin
	*/
	public BufferedCachelessTreeSet(String tdbname, int tobjectCacheSize)
		throws IOException, IllegalAccessException {
		session = SessionManager.Connect(tdbname, null, true);
	}
	
	public BufferedCachelessTreeSet(String tdbname, String tremotename, int tobjectCacheSize)
			throws IOException, IllegalAccessException {
			session = SessionManager.Connect(tdbname, tremotename, true);
		}
	/**
	* Put an object to persistent collection.
	* @param tvalue The value for the object
	* @exception IOException if put to backing store fails
	*/
	@SuppressWarnings("rawtypes")
	public synchronized void add(Comparable tvalue) throws IOException {
			session.put(tvalue);
	}
	
	@SuppressWarnings("rawtypes")
	public synchronized TreeSearchResult locate(Comparable tvalue) throws IOException {
		return session.locate(tvalue);
	}
	
	/**
	* Returns true if key is in collection
	* @param tkey The key to match
	* @return true or false if in
	* @exception IOException If backing store fails
	*/
	@SuppressWarnings("rawtypes")
	public synchronized boolean contains(Comparable tkey) throws IOException {
			return session.contains(tkey);
	}
	/**
	* Remove object from backing store
	* @param tkey the value to match
	* @return true if value was present
	* @exception IOException If backing store fails
	*/
	@SuppressWarnings("rawtypes")
	public synchronized boolean remove(Comparable tkey) throws IOException {
			return session.remove(tkey) != null;
	}
	/**
	* Return the number of elements in the backing store
	* @return A long value of number of elements
	* @exception IOException If backing store retrieval failure
	*/
	public synchronized long size() throws IOException {
			return session.size();
	}
	/**
	* Return the last element.
	* @return The last element
	* @exception IOException If backing store retrieval failure
	*/
	public synchronized Object last() throws IOException {
			return session.last();
	}
	/**
	* Return the first element
	* @return The first element
	* @exception IOException If backing store retrieval failure
	*/
	public synchronized Object first() throws IOException {
			return session.first();
	}
	/**
	* Return the subset of elements
	* @return An Iterator of subset, not a real Set
	* @exception IOException If backing store retrieval failure
	*/
	@SuppressWarnings("rawtypes")
	public synchronized Iterator<?> subSet( Comparable fkey, Comparable tkey) throws IOException {
			return session.subSet(fkey, tkey);
	}
	
	@SuppressWarnings("rawtypes")
	public synchronized Stream<?> subSetStream( Comparable fkey, Comparable tkey) throws IOException {
			return session.subSetStream(fkey, tkey);
	}
	/**
	* Return boolean value indicating whether the set is empty
	* @return true if empty
	* @exception IOException If backing store retrieval failure
	*/
	public synchronized boolean isEmpty() throws IOException {
			return session.isEmpty();
	}
	
	public String getDBName() {
		return session.getDBname();
	}
}
