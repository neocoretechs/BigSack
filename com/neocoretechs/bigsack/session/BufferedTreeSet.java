package com.neocoretechs.bigsack.session;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;
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
* BufferedTreeSet. We use the BigSackSession object here and add a L3 cache java TreeSet.
* The user is not concerned with semantics of recovery when using this construct. The commit
* operations are performed after each insert and recovery takes place if a failure occurs during
* runtime writes. If transparency with existing code is paramount this class is a good choice.
* Thread safety is with the session object using session.getMutexObject().
* Java TreeSet backed by pooled serialized objects.<br>
* @author Groff (C) NeoCoreTechs 2003, 2017
*/
public class BufferedTreeSet {
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected TreeSet<Comparable<?>> table = new TreeSet();
	protected BigSackSession session;
	public BigSackSession getSession() {
		return session;
	}

	protected int objectCacheSize;
	/**
	* Get instance of BigSack session.
	* Each new instance of this will connect to a backing store
	* to provide an in-mem cache. 
	* @param tdbname The database name
	* @param tobjectCacheSize The maximum size of in-mem cache , then backing store hits go up
	* @exception IOException if global IO problem
	* @exception IllegalAccessException if the database has been put offline
	*/
	public BufferedTreeSet(String tdbname, int tobjectCacheSize)
		throws IOException, IllegalAccessException {
		session = SessionManager.Connect(tdbname, null, true);
		objectCacheSize = tobjectCacheSize;
	}
	
	public BufferedTreeSet(String tdbname, String tremotedbname, int tobjectCacheSize)
			throws IOException, IllegalAccessException {
			session = SessionManager.Connect(tdbname, tremotedbname, true);
			objectCacheSize = tobjectCacheSize;
		}
	/**
	* Put an object to main cache and pool.  We may
	* toss out an old one when cache size surpasses objectCacheSize
	* @param tvalue The value for the object
	* @exception IOException if put to backing store fails
	*/
	@SuppressWarnings("rawtypes")
	public void add(Comparable tvalue) throws IOException {
		synchronized (session.getMutexObject()) {
				if (table.size() >= objectCacheSize) {
					// throw one out
					Iterator<Comparable<?>> et = table.iterator();
					//Object remo = 
					et.next();
					et.remove();
				}
				// now put new
				session.put(tvalue);
				session.Commit();
				table.add(tvalue);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public synchronized TreeSearchResult locate(Comparable tvalue) throws IOException {
		return session.locate(tvalue);
	}
	
	/**
	* Returns true if value in table
	* @param tvalue the value to match
	* @return true or false if in
	* @exception IOException If backing store fails
	*/
	@SuppressWarnings("rawtypes")
	public boolean contains(Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
				boolean isin = table.contains(tkey);
				if (!isin) {
					isin = session.contains(tkey);
				}
				session.Commit();
				return isin;
		}
	}
	/**
	* Remove object from cache and backing store
	* @param tkey the value to match
	* @return previous value or null
	* @exception IOException If backing store fails
	*/
	@SuppressWarnings("rawtypes")
	public Object remove(Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
				table.remove(tkey);
				Object o = session.remove(tkey);
				session.Commit();
				return o;
		}
	}
	/**
	* Return the number of elements in the backing store
	* @return A long value of number of elements
	* @exception IOException If backing store retrieval failure
	*/
	public long size() throws IOException {
		synchronized (session.getMutexObject()) {
				long siz = session.size();
				session.Commit();
				return siz;
		}
	}
	/**
	* Return the last element, we have to bypass cache for this because
	* of our random throwouts
	* @return A long value of number of elements
	* @exception IOException If backing store retrieval failure
	*/
	public Object last() throws IOException {
		synchronized (session.getMutexObject()) {
				Object o = session.lastKey();
				session.Commit();
				return o;
		}
	}
	/**
	* Return the first element, we have to bypass cache for this because
	* of our random throwouts
	* @return A long value of number of elements
	* @exception IOException If backing store retrieval failure
	*/
	public Object first() throws IOException {
		synchronized (session.getMutexObject()) {
				Object o = session.firstKey();
				session.Commit();
				return o;
		}
	
	}
	/**
	* Return the headset of elements, we have to bypass cache for this because
	* of our random throwouts
	* @param tkey return from head to strictly less than tkey
	* @return An Iterator of subset, not a real Set
	* @exception IOException If backing store retrieval failure
	*/
	@SuppressWarnings("rawtypes")
	public Iterator<?> headSet(Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
				return session.headSet(tkey);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public Stream<?> headSetStream(Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
				return session.headSetStream(tkey);
		}
	}
	/**
	* Return the tailset of elements, we have to bypass cache for this because
	* of our random throwouts
	* @param fkey return from this value to end
	* @return An Iterator of subset, not a real Set
	* @exception IOException If backing store retrieval failure
	*/
	@SuppressWarnings("rawtypes")
	public Iterator<?> tailSet(Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
				return session.tailSet(tkey);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public Stream<?> tailSetStream(Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
				return session.tailSetStream(tkey);
		}
	}
	/**
	* Return the subset of elements, we have to bypass cache for this because
	* of our random throwouts
	* @return A subset iterator, not a real Set
	* @exception IOException If backing store retrieval failure
	*/
	@SuppressWarnings("rawtypes")
	public Iterator<?> subSet(Comparable fkey, Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
				return session.subSet(fkey, tkey);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public Stream<?> subSetStream(Comparable fkey, Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
				return session.subSetStream(fkey, tkey);
		}
	}
	/**
	* Return boolean value indicating whether the set is empty
	* @return true if empty
	* @exception IOException If backing store retrieval failure
	*/
	public boolean isEmpty() throws IOException {
		synchronized (session.getMutexObject()) {
				boolean ret = session.isEmpty();
				session.Commit();
				return ret;
		}
	}

	
	public String getDBName() {
		return session.getDBname();
	}
}
