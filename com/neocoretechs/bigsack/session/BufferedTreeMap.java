package com.neocoretechs.bigsack.session;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

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
* BufferedTreeMap.We use the BigSackSession object here and add a L3 cache java TreeMap.
* The user is not concerned with semantics of recovery when using this construct. The commit
* operations are performed after each insert and recovery takes place if a failure occurs during
* runtime writes. If transparency with existing code is paramount this class is a good choice.
* Thread safety is with the session object using session.getMutexObject().
* Java TreeMap backed by pooled serialized objects.
* @author Groff (C) NeoCoreTechs 2003, 2017
*/
public class BufferedTreeMap {
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected TreeMap<Comparable<?>, Object> table = new TreeMap();
	protected BigSackSession session;
	protected int objectCacheSize;
	/**
	* Get instance of BigSack session.
	* Each new instance of this will connect to the same backing store
	* with a different in-mem cache.
	* @param tdbname The database name
	* @param tobjectCacheSize The maximum size of in-mem cache , then backing store hits go up
	* @exception IOException if global IO problem
	* @exception IllegalAccessException if the database has been put offline
	*/
	public BufferedTreeMap(String tdbname, int tobjectCacheSize)
		throws IOException, IllegalAccessException {
		session = SessionManager.Connect(tdbname, null, true);
		objectCacheSize = tobjectCacheSize;
	}
	
	public BufferedTreeMap(String tdbname, String tremotename, int tobjectCacheSize)
			throws IOException, IllegalAccessException {
			session = SessionManager.Connect(tdbname, tremotename, true);
			objectCacheSize = tobjectCacheSize;
	}
	/**
	* Put a  key/value pair to main cache and pool.  We may
	* toss out an old one when cache size surpasses objectCacheSize
	* @param tkey The key for the pair
	* @param tvalue The value for the pair
	* @exception IOException if put to backing store fails
	*/
	@SuppressWarnings("rawtypes")
	public void put(Comparable tkey, Object tvalue) throws IOException {
		synchronized (session.getMutexObject()) {
				if (table.size() >= objectCacheSize) {
					// throw one out
					Iterator et = table.keySet().iterator();
					//Object remo = 
					et.next();
					et.remove();
				}
				// now put new
				session.put(tkey, tvalue);
				session.Commit();
				table.put(tkey, tvalue);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public synchronized TreeSearchResult locate(Comparable tvalue) throws IOException {
		return session.locate(tvalue);
	}
	
	/**
	* Get a value from backing store if not in cache.
	* We may toss out one to make room if size surpasses objectCacheSize
	* @param tkey The key for the value
	* @return The value for the key
	* @exception IOException if get from backing store fails
	*/
	@SuppressWarnings("rawtypes")
	public Object get(Comparable tkey) throws IOException {
		Object c = null;
		synchronized (session.getMutexObject()) {
				c = session.get(tkey);
				session.Commit();
		}
		return c;
	}
	
	/**
	* Get a value from backing store if not in cache.
	* We may toss out one to make room if size surpasses objectCacheSize
	* @param tkey The key for the value
	* @return The value for the key
	* @exception IOException if get from backing store fails
	*/
	@SuppressWarnings("rawtypes")
	public Object getValue(Object tkey) throws IOException {
		synchronized (session.getMutexObject()) {
				Object kvp = session.getValue(tkey);
				session.Commit();
				if (kvp == null)
					return null;
				return kvp;
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
	* Obtain iterator over the entrySet. Retrieve from backing store if not in cache.
	* @return The Iterator for all elements
	* @exception IOException if get from backing store fails
	*/
	public Iterator<?> entrySet() throws IOException {
		synchronized (session.getMutexObject()) {
				return session.entrySet();
		}
	}
	/**
	* Get a keySet iterator. Get from backing store if not in cache.
	* @return The iterator for the keys
	* @exception IOException if get from backing store fails
	*/
	public Iterator<?> keySet() throws IOException {
		synchronized (session.getMutexObject()) {
				return session.keySet();
		}
	}
	/**
	* Returns true if the collection contains the given key
	* @param tkey The key to match
	* @return true or false if in
	* @exception IOException If backing store fails
	*/
	@SuppressWarnings("rawtypes")
	public boolean containsKey(Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
			boolean ret = session.contains(tkey);
			session.Commit();
			return ret;
		}
	}
	/**
	* Remove object from cache and backing store.
	* @param tkey The key to match
	* @return The removed object
	* @exception IOException If backing store fails
	*/
	@SuppressWarnings("rawtypes")
	public Object remove(Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
			Object o = session.remove(tkey);
			session.Commit();
			return o;
		}
	}
	/**
	* @return First key in set
	* @exception IOException If backing store retrieval failure
	*/
	public Object firstKey() throws IOException {
		synchronized (session.getMutexObject()) {
			Object ret = session.firstKey();
			session.Commit();
			return ret;
		}
	}
	/**
	* @return Last key in set
	* @exception IOException If backing store retrieval failure
	*/
	public Object lastKey() throws IOException {
		synchronized (session.getMutexObject()) {
			Object ret = session.lastKey();
			session.Commit();
			return ret;
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
				Object ret = session.last();
				session.Commit();
				return ret;
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
			Object ret = session.first();
			session.Commit();
			return ret;
		}
	}
	/**
	* @param tkey Strictly less than 'to' this element
	* @return Iterator of first to tkey
	* @exception IOException If backing store retrieval failure
	*/
	@SuppressWarnings("rawtypes")
	public Iterator<?> headMap(Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.headSet(tkey);
		}
	}
	/**
	* @param tkey Strictly less than 'to' this element
	* @return Iterator of first to tkey returning KeyValuePairs
	* @exception IOException If backing store retrieval failure
	*/
	@SuppressWarnings("rawtypes")
	public Iterator<?> headMapKV(Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.headSetKV(tkey);
		}
	}
	/**
	* @param fkey Greater or equal to 'from' element
	* @return Iterator of objects from fkey to end
	* @exception IOException If backing store retrieval failure
	*/
	@SuppressWarnings("rawtypes")
	public Iterator<?> tailMap(Comparable fkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.tailSet(fkey);
		}
	}
	/**
	* @param fkey Greater or equal to 'from' element
	* @return Iterator of objects from fkey to end which are KeyValuePairs
	* @exception IOException If backing store retrieval failure
	*/
	@SuppressWarnings("rawtypes")
	public Iterator<?> tailMapKV(Comparable fkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.tailSetKV(fkey);
		}
	}
	/**
	* @param fkey 'from' element inclusive 
	* @param tkey 'to' element exclusive
	* @return Iterator of objects in subset from fkey to tkey
	* @exception IOException If backing store retrieval failure
	*/
	@SuppressWarnings("rawtypes")
	public Iterator<?> subMap(Comparable fkey, Comparable tkey)
		throws IOException {
		synchronized (session.getMutexObject()) {
			return session.subSet(fkey, tkey);
		}
	}
	/**
	* @param fkey 'from' element inclusive 
	* @param tkey 'to' element exclusive
	* @return Iterator of objects in subset from fkey to tkey composed of KeyValuePairs
	* @exception IOException If backing store retrieval failure
	*/
	@SuppressWarnings("rawtypes")
	public Iterator<?> subMapKV(Comparable fkey, Comparable tkey)
		throws IOException {
		synchronized (session.getMutexObject()) {
			return session.subSetKV(fkey, tkey);
		}
	}
	/**
	* Return boolean value indicating whether the map is empty
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
