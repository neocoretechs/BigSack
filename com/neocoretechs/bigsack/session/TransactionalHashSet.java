package com.neocoretechs.bigsack.session;

import java.io.IOException;
import java.util.Iterator;

import com.neocoretechs.bigsack.keyvaluepages.KeySearchResult;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;

public class TransactionalHashSet implements SetInterface, TransactionInterface {
	protected BigSackSession session;
	public TransactionInterface getSession() {
		return session;
	}
	
	public TransactionalHashSet(String tdbname, String backingStore, int poolBlocks) throws IOException, IllegalAccessException {
		session = SessionManager.Connect(tdbname, "HMap", backingStore, poolBlocks);
	}

	@Override
	public long getTransactionId() {
		synchronized (session.getMutexObject()) {
			return session.getTransactionId();
		}
	}

	@Override
	public void Close(boolean rollback) throws IOException {
		rollupSession(rollback);
	}

	@Override
	public void Rollback() throws IOException {
		synchronized (session.getMutexObject()) {
			session.Rollback();
		}
	}

	@Override
	public void Commit() throws IOException {
		synchronized (session.getMutexObject()) {
			session.Commit();
		}
	}

	@Override
	public void Checkpoint() throws IllegalAccessException, IOException {
		synchronized (session.getMutexObject()) {
			session.Checkpoint();
		}
	}

	@Override
	public void rollupSession(boolean rollback) throws IOException {
		synchronized (session.getMutexObject()) {
			session.rollupSession(rollback);
		}
	}

	@Override
	public String getDBName() {
		return session.getDBname();
	}

	@Override
	public String getDBPath() {
		return session.getDBPath();
	}

	@Override
	public int getUid() {
		return session.getUid();
	}

	@Override
	public int getGid() {
		return session.getGid();
	}

	@Override
	public Object getMutexObject() {
		return session.getMutexObject();
	}

	@Override
	public boolean put(Comparable o) throws IOException {
		synchronized (session.getMutexObject()) {
			// now put new
			return session.put(o);
		}
	}

	@Override
	public Object get(Comparable o) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.get(o);
		}
	}

	@Override
	public KeySearchResult locate(Comparable key) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.locate(key);
		}
	}

	@Override
	public Iterator<?> iterator() throws IOException {
		synchronized(session.getMutexObject()) {
			return session.entrySet();
		}
	}

	@Override
	public boolean contains(Comparable o) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.contains(o);
		}
	}

	@Override
	public Object remove(Comparable o) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.remove(o);	
		}
	}

	/**
	* Return the last element
	* @return A long value of number of elements
	* @exception IOException If backing store retrieval failure
	*/
	public Object last() throws IOException {
		synchronized (session.getMutexObject()) {
				Object o = session.lastKey();
				return o;
		}
	}
	/**
	* Return the first element
	* @return A long value of number of elements
	* @exception IOException If backing store retrieval failure
	*/
	public Object first() throws IOException {
		synchronized (session.getMutexObject()) {
				Object o = session.firstKey();
				return o;
		}
	
	}

	@Override
	public long size() throws IOException {
		synchronized (session.getMutexObject()) {
			long siz = session.size();
			return siz;
		}
	}

	@Override
	public boolean isEmpty() throws IOException {
		synchronized (session.getMutexObject()) {
			boolean ret = session.isEmpty();
			return ret;
		}
	}

	@Override
	public void Open() throws IOException {
		synchronized(session.getMutexObject()) {
			session.Open();
		}
		
	}

	@Override
	public void forceClose() throws IOException {
		synchronized(session.getMutexObject()) {
			session.forceClose();
		}
		
	}
	
	@Override
	public KeyValueMainInterface getKVStore() {
		synchronized(session.getMutexObject()) {
			return session.getKVStore();
		}
	}

}
