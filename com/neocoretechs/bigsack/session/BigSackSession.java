package com.neocoretechs.bigsack.session;
import java.io.IOException;
import java.util.Iterator;
import java.util.Stack;
import java.util.stream.Stream;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.pooled.Datablock;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.iterator.EntrySetIterator;
import com.neocoretechs.bigsack.iterator.HeadSetIterator;
import com.neocoretechs.bigsack.iterator.HeadSetKVIterator;
import com.neocoretechs.bigsack.iterator.KeySetIterator;
import com.neocoretechs.bigsack.iterator.SubSetIterator;
import com.neocoretechs.bigsack.iterator.SubSetKVIterator;
import com.neocoretechs.bigsack.iterator.TailSetIterator;
import com.neocoretechs.bigsack.iterator.TailSetKVIterator;
import com.neocoretechs.bigsack.keyvaluepages.KeySearchResult;
import com.neocoretechs.bigsack.keyvaluepages.KeyValue;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
import com.neocoretechs.bigsack.keyvaluepages.TraversalStackElement;
import com.neocoretechs.bigsack.stream.EntrySetStream;
import com.neocoretechs.bigsack.stream.HeadSetKVStream;
import com.neocoretechs.bigsack.stream.HeadSetStream;
import com.neocoretechs.bigsack.stream.KeySetStream;
import com.neocoretechs.bigsack.stream.SubSetKVStream;
import com.neocoretechs.bigsack.stream.SubSetStream;
import com.neocoretechs.bigsack.stream.TailSetKVStream;
import com.neocoretechs.bigsack.stream.TailSetStream;
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
* Session object. Returned by SessionManager.Connect().
* Responsible for providing access to Deep Store key/value interface implementations
* such as BTree and HMap..  Operations include
* handing out iterators, inserting and deleting objects, size, navigation, clearing,
* and handling commit and rollback.
* @author Jonathan Groff (C) NeoCoreTechs 2003, 2017, 2021
*/
final class BigSackSession implements TransactionInterface {
	private boolean DEBUG = true;
	private int uid;
	private int gid;
	private KeyValueMainInterface kvStore;
	private GlobalDBIO globalIO;
	/**
	* Create a new session
	* @param kvMain The {@link KeyValueMainInterface} Main object than handles the key pages indexing the objects in the deep store.
	* @param tuid The user
	* @param tgis The group
	* @exception IOException If global IO problem
	*/
	protected BigSackSession(GlobalDBIO globalIO, int uid, int gid)  {
		this.globalIO = globalIO;
		this.kvStore = this.globalIO.getKeyValueMain();
		this.uid = uid;
		this.gid = gid;
		if( DEBUG )
			System.out.println("BigSackSession constructed with db:"+getDBPath());
	}

	public long getTransactionId() { return kvStore.getIO().getTransId(); }
	
	protected String getDBname() {
		return kvStore.getIO().getDBName();
	}
	
	protected String getDBPath() {
		return kvStore.getIO().getDBPath();
	}

	@Override
	public String toString() {
		return "BigSackSession using DB:"+getDBname()+" path:"+getDBPath();
	}
	
	@Override
	public int hashCode() {
		return Integer.hashCode((int)getTransactionId());
	}
	
	@Override
	public boolean equals(Object o) {
		return( getTransactionId() == ((Long)o).longValue());
	}
	
	protected int getUid() {
		return uid;
	}
	protected int getGid() {
		return gid;
	}

	protected Object getMutexObject() {
		return kvStore;
	}

	@SuppressWarnings("rawtypes")
	protected boolean put(Comparable o) throws IOException {
		return (kvStore.add(o) == 0 ? false : true);
	}
	
	/**
	 * Call the add method of KeyValueMainInterface.
	 * @param key The key value to attempt add
	 * @param o The value for the key to add
	 * @return true if the key existed and was not added
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected boolean put(Comparable key, Object o) throws IOException {
		return (kvStore.add(key, o) == 0 ? false : true);
	}
	/**
	 * Cause the KvStore to seekKey for the Comparable type.
	 * @param o the Comparable object to seek.
	 * @return the Key/Value object from the retrieved node
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected Object get(Comparable o) throws IOException {
		Stack stack = new Stack();
		KeySearchResult tsr = kvStore.seekKey(o, stack);
		if(tsr != null && tsr.atKey)
			//return tsr.page.getKeyValueArray(tsr.insertPoint);
			return tsr.getKeyValue();
		return null;
	}
	/**
	 * Retrieve an object with this value for first key found to have it.
	 * @param o the object value to seek
	 * @return The Map.Entry derived BigSack iterator {@link Entry} element for the key, null if not found
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected Object getValue(Object o) throws IOException {
		return kvStore.seekObject(o);
	}
	/**
	 * Locate a TreeSearchResult for a given key.
	 * @param key The Comparable key to search
	 * @return The TreeSearchResult, which has atKey true if key was actually located, the page, and index on that page. if insertPoint > 0 then insertPoint-1 point to key that immediately precedes target key.
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected KeySearchResult locate(Comparable key, Stack stack) throws IOException {
		KeySearchResult tsr = kvStore.locate(key, stack);
		return tsr;
	}
	
	/**
	* Not a real subset, returns iterator vs set.
	* 'from' element inclusive, 'to' element exclusive
	* @param fkey Return from fkey
	* @param tkey Return from fkey to strictly less than tkey
	* @return The Iterator over the subSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> subSet(Comparable fkey, Comparable tkey) throws IOException {
		return new SubSetIterator(fkey, tkey, kvStore);
	}
	/**
	 * Return a Stream that delivers the subset of fkey to tkey
	 * @param fkey the from key
	 * @param tkey the to key
	 * @return the stream from which the lambda expression can be utilized
	 * @throws IOException
	 */
	protected Stream<?> subSetStream(Comparable fkey, Comparable tkey) throws IOException {
		return new SubSetStream(new SubSetIterator(fkey, tkey, kvStore));
	}
	
	/**
	* Not a real subset, returns iterator vs set.
	* 'from' element inclusive, 'to' element exclusive
	* @param fkey Return from fkey
	* @param tkey Return from fkey to strictly less than tkey
	* @return The KeyValuePair Iterator over the subSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> subSetKV(Comparable fkey, Comparable tkey) throws IOException {
		return new SubSetKVIterator(fkey, tkey, kvStore);
	}
	/**
	 * Return a Streamof key/value pairs that delivers the subset of fkey to tkey
	 * @param fkey the from key
	 * @param tkey the to key
	 * @return the stream from which the lambda expression can be utilized
	 * @throws IOException
	 */
	protected Stream<?> subSetKVStream(Comparable fkey, Comparable tkey) throws IOException {
			return new SubSetKVStream(fkey, tkey, kvStore);
	}
	
	/**
	* Not a real subset, returns iterator
	* @return The Iterator over the entrySet
	* @exception IOException If we cannot obtain the iterator
	*/
	protected Iterator<?> entrySet() throws IOException {
		return new EntrySetIterator(kvStore);
	}
	/**
	 * Get a stream of entry set
	 * @return
	 * @throws IOException
	 */
	protected Stream<?> entrySetStream() throws IOException {
		return new EntrySetStream(kvStore);
	}
	/**
	* Not a real subset, returns Iterator
	* @param tkey return from head to strictly less than tkey
	* @return The Iterator over the headSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> headSet(Comparable tkey) throws IOException {
		return new HeadSetIterator(tkey, kvStore);
	}
	/**
	 * Get a stream of headset
	 * @param tkey
	 * @return
	 * @throws IOException
	 */
	protected Stream<?> headSetStream(Comparable tkey) throws IOException {
		return new HeadSetStream(tkey, kvStore);
	}
	/**
	* Not a real subset, returns Iterator
	* @param tkey return from head to strictly less than tkey
	* @return The KeyValuePair Iterator over the headSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> headSetKV(Comparable tkey) throws IOException {
		return new HeadSetKVIterator(tkey, kvStore);
	}
	/**
	 * Get a stream of head set
	 * @param tkey
	 * @return
	 * @throws IOException
	 */
	protected Stream<?> headSetKVStream(Comparable tkey) throws IOException {
		return new HeadSetKVStream(tkey, kvStore);
	}
	/**
	* Return the keyset Iterator over all elements
	* @return The Iterator over the keySet
	* @exception IOException If we cannot obtain the iterator
	*/
	protected Iterator<?> keySet() throws IOException {
		return new KeySetIterator(kvStore);
	}
	/**
	 * Get a keyset stream
	 * @return
	 * @throws IOException
	 */
	protected Stream<?> keySetStream() throws IOException {
		return new KeySetStream(kvStore);
	}
	/**
	* Not a real subset, returns Iterator
	* @param fkey return from value to end
	* @return The Iterator over the tailSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> tailSet(Comparable fkey) throws IOException {
		return new TailSetIterator(fkey, kvStore);
	}
	/**
	 * Return a tail set stream
	 * @param fkey
	 * @return
	 * @throws IOException
	 */
	protected Stream<?> tailSetStream(Comparable fkey) throws IOException {
		return new TailSetStream(fkey, kvStore);
	}
	/**
	* Not a real subset, returns Iterator
	* @param fkey return from value to end
	* @return The KeyValuePair Iterator over the tailSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> tailSetKV(Comparable fkey) throws IOException {
		return new TailSetKVIterator(fkey, kvStore);
	}
	/**
	 * Return a tail set key/value stream
	 * @param fkey from key of tailset
	 * @return the stream from which the lambda can be utilized
	 * @throws IOException
	 */
	protected Stream<?> tailSetKVStream(Comparable fkey) throws IOException {
		return new TailSetKVStream(fkey, kvStore);
	}
	/**
	 * Contains a value object
	 * @param o
	 * @return boolean if the value object is found
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected boolean containsValue(Object o) throws IOException {
		Object obj = kvStore.seekObject(o);
		if( obj != null ) {
			return true;
		}
		return false;
	}
	
	/**
	 * Contains a value object
	 * @param o
	 * @return boolean if the value object is found
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected boolean contains(Comparable o) throws IOException {
		// return TreeSearchResult
		Stack s = new Stack();
		KeySearchResult tsr = kvStore.seekKey(o, s);
		return tsr.atKey;
	}
	
	/**
	* Remove the key and value of the parameter.
	* @return null or previous object
	*/
	@SuppressWarnings("rawtypes")
	protected Object remove(Comparable o) throws IOException {
		kvStore.delete(o);
		return o; //fluent interface style
	}
	/**
	 * Get the value of the object associated with first key
	 * @return Object from first key
	 * @throws IOException
	 */
	protected Object first(TraversalStackElement tse, Stack stack) throws IOException {
		KeyValue current = kvStore.rewind(tse, stack);
		if(current == null)
			return null;
		Object retVal = current.getmValue();
		return retVal;
	}
	/**
	 * Get the first key
	 * @return The Comparable first key in the KVStore
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected Comparable firstKey(TraversalStackElement tse, Stack stack) throws IOException {
		if(DEBUG)
			System.out.printf("%s.firstKey for kvStore %s%n", this.getClass().getName(),kvStore);
		KeyValue current = kvStore.rewind(tse, stack);
		if(current == null)
			return null;
		Comparable retVal = current.getmKey();
		return retVal;
	}
	/**
	 * Get the last object associated with greatest valued key in the KVStore
	 * @return The Object of the greatest key
	 * @throws IOException
	 */
	protected Object last(TraversalStackElement tse, Stack stack) throws IOException {
		KeyValue current = kvStore.toEnd(tse, stack);
		if(current == null)
			return null;
		Object retVal = current.getmValue();
		return retVal;
	}
	/**
	 * Get the last key in the KVStore
	 * @return The last, greatest valued key in the KVStore.
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected Comparable lastKey(TraversalStackElement tse, Stack stack) throws IOException {
		KeyValue current = kvStore.toEnd(tse, stack);
		if(current == null)
			return null;
		Comparable retVal = current.getmKey();
		return retVal;
	}
	/**
	 * Get the number of keys total.
	 * @return The size of the KVStore.
	 * @throws IOException
	 */
	protected long size() throws IOException {
		return kvStore.count();
	}
	/**
	 * Is the KVStore empty?
	 * @return true if it is empty.
	 * @throws IOException
	 */
	protected boolean isEmpty() throws IOException {
		return kvStore.isEmpty();
	}

	/**
	* Close this session.
	* @param rollback true to roll back, false to commit
	* @exception IOException For low level failure
	*/
	public void Close(boolean rollback) throws IOException {
		rollupSession(rollback);
	}
	/**
	 * Open the files associated with the BTree for the instances of class
	 * @throws IOException
	 */
	protected void Open() throws IOException {
		kvStore.getIO().getIOManager().Fopen();
	}
	
	/**
	* @exception IOException for low level failure
	*/
	public void Rollback() throws IOException {
		kvStore.getIO().deallocOutstandingRollback();
	}
	
	/**
	* Commit the blocks.
	* @exception IOException For low level failure
	*/
	public void Commit() throws IOException {
		kvStore.getIO().deallocOutstandingCommit();
	}
	/**
	 * Checkpoint the current transaction
	 * @throws IOException 
	 * @throws IllegalAccessException 
	 */
	public void Checkpoint() throws IllegalAccessException, IOException {
			kvStore.getIO().checkpointBufferFlush();
	}
	/**
	* Generic session roll up.  Data is committed based on rollback param.
	* We deallocate the outstanding block
	* We iterate the tablespaces for each db removing obsolete log files.
	* Remove the WORKER threads from KeyValueMain, then remove this session from the SessionManager
	* @param rollback true to roll back, false to commit
	* @exception IOException For low level failure
	*/
	public void rollupSession(boolean rollback) throws IOException {
		if (rollback) {
			kvStore.getIO().deallocOutstandingRollback();
		} else {
			// calls commitbufferflush
			kvStore.getIO().deallocOutstandingCommit();
		}
		SessionManager.releaseSession(this);
	}
	
	/**
	* This forces a close with rollback.
	* for offlining of db's
	* @exception IOException if low level error occurs
	*/
	protected void forceClose() throws IOException {
		Close(true);
	}

	protected KeyValueMainInterface getKVStore() { return kvStore; }
	
	/**
	 * Scans through tablespaces and analyzes space utilization
	 * BEWARE - doing this under multithreading, well, youre asking for it..
	 * @exception Exception
	 */
	public void analyze(boolean verbose) throws Exception {
		System.out.println("Begin BigSack analysis verbose:"+verbose);
		float totutil = 0;
		Datablock db = new Datablock(DBPhysicalConstants.DBLOCKSIZ);
		int numberBlks = 1;
		long minBlock = 0;
		int itab = 0;
		do {
			int tnumberBlks = 1;
			float ttotutil = 0;
			int zeroBlocks = 0;
			long fsiz = kvStore.getIO().getIOManager().Fsize(itab)-DBPhysicalConstants.DBLOCKSIZ;
			minBlock =0;
			//long maxBlock = GlobalDBIO.makeVblock(itab, fsiz);
			
			System.out.println("<<BigSack Analysis|Tablespace number "+itab+" File size bytes: " + fsiz+">>");
			while (minBlock < fsiz) {
				kvStore.getIO().getIOManager().FseekAndReadFully(GlobalDBIO.makeVblock(itab, minBlock), db);
				if( db.getBytesused() == 0 || db.getBytesinuse() == 0 ) {		
					++zeroBlocks;
				} else {
					//if(db.getData()[0] == 0xAC && db.getData()[1] == 0xED && db.getData()[2] == 0x00 && db.getData()[3] == 0x05) 
					//	System.out.println("Detected object stream header for tablespace"+itab+" pos:"+minBlock);
				}
				//if( Props.DEBUG ) System.out.println("Block:" + xblk + ": " + db.toBriefString());
				if( verbose ) {
						System.out.println("BigSack Block tablespace "+itab+". Current zero blocks:"+zeroBlocks+". Pos:"+minBlock+". Data:"+db.blockdump());
				}
				totutil += ((float) (db.getBytesinuse()));
				ttotutil += ((float) (db.getBytesinuse()));
				minBlock += DBPhysicalConstants.DBLOCKSIZ;
	
				++numberBlks;
				++tnumberBlks;
				//if( ((float)(tnumberBlks))/1000.0f == (float)(tnumberBlks/1000) ) System.out.print(tnumberBlks+"\r");
			}
			int ttTotal = DBPhysicalConstants.DBLOCKSIZ * tnumberBlks; // bytes total theoretical
			int taTotal = (int) ((ttotutil / (float)ttTotal) * 100.0); // ratio of total to used
			System.out.println("BigSack Tablespace "+itab+" utilization: " + (int)ttotutil + " bytes in "+tnumberBlks+" blocks");
			System.out.println("Maximum possible utilization is "+ttTotal+" bytes, making data Utilization "+taTotal+"%");
		} while( (minBlock = GlobalDBIO.nextTableSpace(itab++)) != 0);
		System.out.println("Total BigSack utilization: " + (int)totutil + " bytes in "+numberBlks+" blocks");
		int tTotal = DBPhysicalConstants.DBLOCKSIZ * numberBlks; // bytes total theoretical
		int aTotal = (int) ((totutil / (float)tTotal) * 100.0); // ratio of total to used
		System.out.println("Maximum possible utilization is "+tTotal+" bytes, making data Utilization "+aTotal+"%");
	}

}
