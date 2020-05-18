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
* TransactionalTreeMap. The same underlying session objects are used here but the user has access to the transactional
* Semantics underlying the ARIES recovery protocol. Thread safety is enforced on the session at this level.
* Java TreeMap backed by pooled serialized objects. It is the users responsibility to commit/rollback/checkpoint.
* @author Groff (C) NeoCoreTechs 2003,2014,2017
*/
public class TransactionalTreeMap {
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
	public TransactionalTreeMap(String tdbname, int tobjectCacheSize)
		throws IOException, IllegalAccessException {
		session = SessionManager.Connect(tdbname, null, true);
		objectCacheSize = tobjectCacheSize;
	}
	
	public TransactionalTreeMap(String tdbname, String tremotedbname, int tobjectCacheSize)
			throws IOException, IllegalAccessException {
			session = SessionManager.Connect(tdbname, tremotedbname, true);
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
				// commit later
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
		synchronized (session.getMutexObject()) {
			return session.get(tkey);
		}
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
				return session.getValue(tkey);
		}
	}	

	/**
	* Return the number of elements in the backing store
 	* @return A long value of number of elements
	* @exception IOException If backing store retrieval failure
	*/
	public long size() throws IOException {
		synchronized (session.getMutexObject()) {
				return session.size();
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
	* @return true if in, or false if absent
	* @exception IOException If backing store fails
	*/
	@SuppressWarnings("rawtypes")
	public boolean containsKey(Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return  session.contains(tkey);
		}
	}
	/**
	* Returns true if the collection contains the given value object
	* @param value The value to match
	* @return true if in, false if absent
	* @exception IOException If backing store fails
	*/
	@SuppressWarnings("rawtypes")
	public boolean containsValue(Object value) throws IOException {
		synchronized (session.getMutexObject()) {
			return  session.containsValue(value);
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
			return session.remove(tkey);
		}
	}
	/**
	* @return First key in set
	* @exception IOException If backing store retrieval failure
	*/
	public Object firstKey() throws IOException {
		synchronized (session.getMutexObject()) {
			return session.firstKey();
		}
	}
	/**
	* @return Last key in set
	* @exception IOException If backing store retrieval failure
	*/
	public Object lastKey() throws IOException {
		synchronized (session.getMutexObject()) {
			return session.lastKey();
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
				return session.last();
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
			return session.first();
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
				return session.isEmpty();
		}
	}

	/**
	 * Commit the outstanding transaction
	 * @throws IOException
	 */
	void commit() throws IOException {
		session.Commit();
	}
	/**
	 * Checkpoint the current database transaction state for roll forward recovery in event of crash
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	void checkpoint() throws IllegalAccessException, IOException {
		session.Checkpoint();
	}
	/**
	 * Roll back the outstanding transactions
	 * @throws IOException
	 */
	void rollback() throws IOException {
		session.Rollback();
	}
	public String getDBName() {
		return session.getDBname();
	}

}
