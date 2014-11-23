package com.neocoretechs.bigsack.session;
import java.io.IOException;
import java.util.Iterator;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.Props;
import com.neocoretechs.bigsack.btree.BTreeMain;
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
* Responsible for providing generic access to BTreeMain from which other
* specific collection types can obtain their functionality.  Operations include
* handing out iterators, inserting and deleting objects, size, navigation, clearing,
* and handling commit and rollback.
* @author Groff
*/
public final class BigSackSession {
	public static final boolean COMMIT = false;
	public static final boolean ROLLBACK = true;
	private int uid;
	private int gid;
	private BTreeMain bTree;
	/**
	* Create new session
	* @param tuid The user
	* @param tgis The group
	* @exception IOException If global IO problem
	*/
	protected BigSackSession(BTreeMain bTree, int tuid, int tgid)  {
		this.bTree = bTree;
		uid = tuid;
		gid = tgid;
	}

	public long getTransactionId() { return bTree.getIO().getTransId(); }
	
	protected String getDBname() {
		return bTree.getIO().getDBName();
	}
	protected int getUid() {
		return uid;
	}
	protected int getGid() {
		return gid;
	}

	public Object getMutexObject() {
		return bTree;
	}

	@SuppressWarnings("rawtypes")
	public void put(Comparable o) throws IOException {
		//bTree.add(o, new String());
		bTree.add(o);
	}

	@SuppressWarnings("rawtypes")
	public void put(Comparable key, Object o) throws IOException {
		bTree.add(key, o);
	}

	@SuppressWarnings("rawtypes")
	public Object get(Comparable o) throws IOException {
		return bTree.seek(o);
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
	public Iterator<?> subSet(Comparable fkey, Comparable tkey)
		throws IOException {
		return new SubSetIterator(fkey, tkey, bTree);
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
	public Iterator<?> subSetKV(Comparable fkey, Comparable tkey)
		throws IOException {
		return new SubSetKVIterator(fkey, tkey, bTree);
	}
	/**
	* Not a real subset, returns iterator
	* @return The Iterator over the entrySet
	* @exception IOException If we cannot obtain the iterator
	*/
	public Iterator<?> entrySet() throws IOException {
		return new EntrySetIterator(bTree);
	}
	/**
	* Not a real subset, returns Iterator
	* @param tkey return from head to strictly less than tkey
	* @return The Iterator over the headSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	public Iterator<?> headSet(Comparable tkey) throws IOException {
		return new HeadSetIterator(tkey, bTree);
	}
	/**
	* Not a real subset, returns Iterator
	* @param tkey return from head to strictly less than tkey
	* @return The KeyValuePair Iterator over the headSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	public Iterator<?> headSetKV(Comparable tkey) throws IOException {
		return new HeadSetKVIterator(tkey, bTree);
	}
	/**
	* Return the keyset Iterator over all elements
	* @return The Iterator over the keySet
	* @exception IOException If we cannot obtain the iterator
	*/
	public Iterator<?> keySet() throws IOException {
		return new KeySetIterator(bTree);
	}
	/**
	* Not a real subset, returns Iterator
	* @param fkey return from value to end
	* @return The Iterator over the tailSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	public Iterator<?> tailSet(Comparable fkey) throws IOException {
		return new TailSetIterator(fkey, bTree);
	}
	/**
	* Not a real subset, returns Iterator
	* @param fkey return from value to end
	* @return The KeyValuePair Iterator over the tailSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	public Iterator<?> tailSetKV(Comparable fkey) throws IOException {
		return new TailSetKVIterator(fkey, bTree);
	}
	
	/**
	 * Contains
	 * @param o
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	public boolean contains(Comparable o) throws IOException {
		Object obj = bTree.seek(o);
		if( obj != null ) {
			bTree.getIO().deallocOutstanding();
			return obj.equals(o);
		}
		bTree.getIO().deallocOutstanding();
		return false;
	}
	
	/**
	* @return null or previous object
	*/
	@SuppressWarnings("rawtypes")
	public Object remove(Comparable o) throws IOException {
		bTree.delete(o);
		return o; //fluent interface style
	}
	
	public Object first() throws IOException {
		bTree.rewind();
		if (bTree.gotoNextKey() > 0)
			throw new IOException("next key fault");
		Object retVal = bTree.getCurrentObject();
		bTree.getIO().deallocOutstanding();
		return retVal;
	}
	
	@SuppressWarnings("rawtypes")
	public Comparable firstKey() throws IOException {
		bTree.rewind();
		if (bTree.gotoNextKey() > 0)
			throw new IOException("next key fault");
		Comparable retVal = bTree.getCurrentKey();
		bTree.getIO().deallocOutstanding();
		return retVal;
	}
	
	public Object last() throws IOException {
		bTree.toEnd();
		if (bTree.gotoPrevKey() > 0)
			throw new IOException("prev key fault");
		Object retVal = bTree.getCurrentObject();
		bTree.getIO().deallocOutstanding();
		return retVal;
	}
	
	@SuppressWarnings("rawtypes")
	public Comparable lastKey() throws IOException {
		bTree.toEnd();
		if (bTree.gotoPrevKey() > 0)
			throw new IOException("prev key fault");
		Comparable retVal = bTree.getCurrentKey();
		bTree.getIO().deallocOutstanding();
		return retVal;
	}
	
	public long size() throws IOException {
		return bTree.getNumKeys();
	}
	/**
	 * not sure when to use this
	 * @throws IOException
	 */
	public void clear() throws IOException {
		bTree.getIO().resetBuckets();
		bTree.getIO().forceBufferClear();
		bTree.getIO().createOrLoad(false);
		bTree.getIO().getUlog().resetLog();
		bTree.setNumKeys(0);
		if( Props.DEBUG ) System.out.println("Clear called, all reset..");
	}
	
	public boolean isEmpty() throws IOException {
		return (size() == 0L);
	}

	/**
	* Close this session.
	* @param rollback true to roll back, false to commit
	* @exception IOException For low level failure
	*/
	public void Close(boolean rollback) throws IOException {
		rollupSession(rollback);
		//SessionManager.releaseSession(this);
		bTree.getIO().Close();
	}
	
	public void Open() throws IOException {
		bTree.getIO().Open();
	}
	
	/**
	* @exception IOException for low level failure
	*/
	public void Rollback() throws IOException {
		rollupSession(true);
	}
	
	/**
	* Commit the blocks.
	* @exception IOException For low level failure
	*/
	public void Commit() throws IOException {
		rollupSession(false);
	}
	/**
	 * Checkpoint the current transaction
	 * @throws IOException 
	 * @throws IllegalAccessException 
	 */
	public void Checkpoint() throws IllegalAccessException, IOException {
		bTree.getIO().getUlog().checkpoint();
	}
	/**
	* Generic session roll up.  Data is committed based on rollback param
	* @param rollback true to roll back, false to commit
	* @exception IOException For low level failure
	*/
	private void rollupSession(boolean rollback) throws IOException {
		if (rollback) {
			bTree.getIO().deallocOutstandingRollback();
			bTree.getIO().getUlog().getLogToFile().deleteOnlineArchivedLogFiles();
		} else {
			bTree.getRoot().putPages(bTree.getIO());
			bTree.getIO().deallocOutstandingCommit();
			bTree.getIO().getUlog().getLogToFile().deleteObsoleteLogfilesOnCommit();
		}

	}
	
	/**
	* This forces a close with rollback.
	* for offlining of db's
	* @exception IOException if low level error occurs
	*/
	protected void forceClose() throws IOException {
		Close(true);
	}
	/**
	 * hidden easter egg or dangerous secret, its up to you
	 */
	public BTreeMain getBTree() { return bTree; }
	/**
	 * Scans through tablespaces and analyzes space utilization
	 * BEWARE - doing this under multithreading, well, youre asking for it..
	 * @exception Exception
	 */
	public void analyze(boolean verbose) throws Exception {
		float totutil = 0;
		Datablock db = new Datablock(DBPhysicalConstants.DBLOCKSIZ);
		int itab = 0;
		long xpos = 0L;
		int numberBlks = 1;
		long xblk = bTree.getIO().firstTableSpace();
		do {
			long fsiz = bTree.getIO().Fsize(itab);
			System.out.println("BigSack Analysis|Tablespace "+itab+" bytes: " + fsiz);
			while (xpos < fsiz) {
				bTree.getIO().FseekAndRead(xblk, db);
				//if( Props.DEBUG ) System.out.println("Block:" + xblk + ": " + db.toString());
				//if( Props.DEBUG ) System.out.println("Block:" + xblk + ": " + db.toBriefString());
				if( verbose ) {
					String dbb = db.toBriefString();
					if( !dbb.isEmpty() ) {
						System.out.println("BigSack Block: "+xblk+":"+dbb);
					}
				}
				totutil += ((float) (db.getBytesinuse()));
				xpos += DBPhysicalConstants.DBLOCKSIZ;
				xblk = GlobalDBIO.makeVblock(itab, xpos);
				++numberBlks;
			}
			xpos = 0;
		}
		while ((xblk = bTree.getIO().nextTableSpace(itab++)) != 0L);
		System.out.println("Total BigSack utilization: " + (int)totutil + " bytes in "+numberBlks+" blocks");
		int tTotal = DBPhysicalConstants.DBLOCKSIZ * numberBlks; // bytes total theoretical
		int aTotal = (int) ((totutil / (float)tTotal) * 100.0); // ratio of total to used
		System.out.println("Maximum possible utilization is "+tTotal+" bytes, making data Utilization "+aTotal+"%");
	}
	
	public boolean equals(Object o) {
		return( getTransactionId() == ((Long)o).longValue());
	}
}
