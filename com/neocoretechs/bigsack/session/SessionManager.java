package com.neocoretechs.bigsack.session;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.neocoretechs.arieslogger.core.LogInstance;
import com.neocoretechs.arieslogger.core.impl.LogCounter;
import com.neocoretechs.arieslogger.core.impl.LogRecord;
import com.neocoretechs.arieslogger.core.impl.LogToFile;
import com.neocoretechs.arieslogger.core.impl.Scan;
import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
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
* SessionManager class is a singleton 
* that accepts connections and returns a BigSackSession object. A table of one to one sessions and
* tables is maintained. A path to the log dirs and a path to the tablespace dirs can be specified or
* a default file structure based on mode can be used if the remoteDbName is null.
* @author Jonathan Groff (c) NeoCoreTechs 2003, 2017, 2021
*/
public final class SessionManager {
	private static boolean DEBUG = false;
	private static ConcurrentHashMap<String, BigSackSession> SessionTable = new ConcurrentHashMap<String, BigSackSession>();
	@SuppressWarnings("rawtypes")
	private static ConcurrentHashMap<?, ?> AdminSessionTable = new ConcurrentHashMap();
	private static Vector<String> OfflineDBs = new Vector<String>();
	private static long globalTransId = System.currentTimeMillis();
	private static String backingStoreType;
	//
	// Sets the maximum number users
	@SuppressWarnings("unused")
	private static final int MAX_USERS = -1;
	//
	// Multithreaded double check Singleton setups:
	// 1.) privatized constructor; no other class can call
	private SessionManager() {
	}
	// 2.) volatile instance
	private static volatile SessionManager instance = null;
	// 3.) lock class, assign instance if null
	public static SessionManager getInstance() {
		synchronized(SessionManager.class) {
			if(instance == null) {
				instance = new SessionManager();
			}
		}
		return instance;
	}
	// Global transaction timestamps
	private static long lastStartTime = 0L;
	private static long lastCommitTime = 0L;
	/*
	public static String getDbPath(String dbName) {
		 BigSackSession dbs = SessionTable.get(dbName);
		 if( dbs != null )
			 return dbs.getDBPath();
		 return null;
	}
	
	public static String getRemoteDBName(String dbName) {
		 BigSackSession dbs = SessionTable.get(dbName);
		 if( dbs != null ) {
			 if( DEBUG )
				 System.out.println("SessionManager.getRemoteDBName "+dbs.getRemoteDBName()+" from "+dbName);
			 return dbs.getRemoteDBName();
		 } else
			 if( DEBUG )
				 System.out.println("SessionManager.getRemoteDBName remote name is NULL from"+dbName);
		 return null;
	}
	*/
	/**
	 * Increment the base global trans id and return, one for each session
	 * @return
	 */
	public static long getGlobalTransId() {
		return ++globalTransId;
	}
	/**
	* Get begin transaction timestamp
	*/
	protected static void getBeginStamp() {
		long bts = System.currentTimeMillis();
		// if < or =, it started at same time (< cause we might have bumped it already)
		if (bts <= lastStartTime)
			++lastStartTime;
		else
			lastStartTime = bts;
		//
	}
	/**
	* @return Current, or end, transaction timestamp
	*/
	protected static long getEndStamp() {
		long ets = System.currentTimeMillis();
		// if < or =, it started at same time (< cause we might have bumped it already)
		if (ets <= lastCommitTime)
			++lastCommitTime;
		else
			lastCommitTime = ets;
		//
		return lastCommitTime;
	}
	/**
	* Connect and return Session instance that is the session.
	* @param dbname The database name as full path
	* @param keystoreType "HMap", "BTree" etc.
	* @param backingstoreType The type of filesystem of memory map "File" "MMap" etc.
	* @param poolBlocks The number of blocks in the buffer pool
	* @return BigSackSession The session we use to control access
	* @exception IOException If low level IO problem
	* @exception IllegalAccessException If access to database is denied
	*/
	public static synchronized BigSackSession Connect(String dbname, String keystoreType, String backingstoreType, int poolBlocks) throws IOException, IllegalAccessException {
		if( DEBUG ) {
			System.out.printf("Connecting to database:%s with key store:%s and backing store:%s with pool blocks:%d%n", dbname, keystoreType, backingstoreType, poolBlocks);
		}
		backingStoreType = backingstoreType;
		// translate user name to uid and group
		// we can restrict access at database level here possibly
		int uid = 0;
		int gid = 1;
		//if( SessionTable.size() >= MAX_USERS && MAX_USERS != -1) throw new IllegalAccessException("Maximum number of users exceeded");
		if (OfflineDBs.contains(dbname))
			throw new IllegalAccessException("Database is offline, try later");
		BigSackSession hps = (SessionTable.get(dbname));
		if (hps == null) {
			// did'nt find it, create anew, throws IllegalAccessException if no go.
			// Global IO and main Key/Value index
			GlobalDBIO objIO = new GlobalDBIO(dbname, keystoreType, backingstoreType, getGlobalTransId(), poolBlocks);
			hps = new BigSackSession(objIO, uid, gid);
			SessionTable.put(dbname, hps);
			if( DEBUG )
				System.out.printf("New session for db:%s session:%s kvmain:%s GlobalIo:%s%n",dbname,hps,objIO.getKeyValueMain(),objIO);
		} else {
			// if closed, then open, else if open this does nothing
			hps.Open();
			if( DEBUG )
				System.out.printf("Re-opening existing session for db:%s session:%s%n",dbname,hps);
		}
		return hps;
	}
	/**
	 * Start the DB with no logging for debugging purposes
	 * or to run read only without logging for some reason
	 * @param dbname the path to the database (path+dbname)
	 * @param remoteDBName The remote path to database tablespace directories (tablespace prepended to endof path) or null
	 * @return
	 * @throws IOException
	 * @throws IllegalAccessException
	 */
	public static synchronized BigSackSession ConnectNoRecovery(String dbname, String keystoreType, String backingstoreType, int poolBlocks) throws IOException, IllegalAccessException {
		if( DEBUG ) {
			System.out.println("Connecting WITHOUT RECOVERY to "+dbname);
		}
		// translate user name to uid and group
		// we can restrict access at database level here possibly
		int uid = 0;
		int gid = 1;
		//if( SessionTable.size() >= MAX_USERS && MAX_USERS != -1) throw new IllegalAccessException("Maximum number of users exceeded");
		if (OfflineDBs.contains(dbname))
			throw new IllegalAccessException("Database is offline, try later");
		BigSackSession hps = (SessionTable.get(dbname));
		if (hps == null) {
			// did'nt find it, create anew, throws IllegalAccessException if no go
			// Global IO and main KeyValue implementation
			if( DEBUG )
				System.out.println("SessionManager.ConectNoRecovery bringing up IO");
			GlobalDBIO objIO = new GlobalDBIO(dbname, keystoreType, backingstoreType, getGlobalTransId(), poolBlocks);
			if( DEBUG )
				System.out.println("SessionManager.ConectNoRecovery bringing up BTree");
			hps = new BigSackSession(objIO, uid, gid);
			if( DEBUG )
				System.out.println("SessionManager.ConectNoRecovery logging session");
			SessionTable.put(dbname, hps);
		} else
			// if closed, then open, else if open this does nothing
			hps.Open();
		//
		return hps;
	}
	
	/**
	* Set the database offline, kill all sessions using it
	* @param dbname The database to offline
	* @exception IOException if we can't force the close
	*/
	protected static synchronized void setDBOffline(String dbname) throws IOException {
		OfflineDBs.addElement(dbname);
		// look for session instance, then signal close
		BigSackSession hps = (SessionTable.get(dbname));
		if (hps != null) {
			hps.forceClose();
		}
	}
	protected static synchronized void setDBOnline(String dbname) {
		OfflineDBs.removeElement(dbname);
	}
	public static synchronized boolean isDBOffline(String dbname) {
		return OfflineDBs.contains(dbname);
	}
	protected static synchronized void releaseSession(TransactionInterface DS) {
		SessionTable.remove(DS);
	}

	public static ConcurrentHashMap<String, BigSackSession> getSessionTable() {
		return SessionTable;
	}
	
	public static boolean contains(long transId) {
		Collection<BigSackSession> sess = SessionTable.values();
		Iterator<BigSackSession> its = sess.iterator();
		while(its.hasNext()) {
			BigSackSession sits = its.next();
			if( sits.getTransactionId() == transId )
				return true;
		}
		return false;
	}
	/**
	 * For those that wish to maintain admin tables
	 * @return The Hashtable of Admin sessions - you define
	 */
	protected static ConcurrentHashMap<?, ?> getAdminSessionTable() {
		return AdminSessionTable;
	}

	public static void analyze(String dbname, String keyStoreType, boolean verbose) throws Exception {
		BigSackSession bss = SessionManager.ConnectNoRecovery(dbname, keyStoreType, "File", 128);
		System.out.println("Proceeding to analyze "+dbname);
		bss.analyze(verbose);
	}
	
	/**
	 * Perform a backward scan of log files.
	 */
	public static void AnalyzeLogs(String dbname, String keyStoreType) throws Exception {
			// init with no recovery
			BigSackSession bss = SessionManager.ConnectNoRecovery(dbname, keyStoreType, "File", 128);
			GlobalDBIO gdb = bss.getKVStore().getIO();
			LogCounter startAt = new LogCounter(1,LogToFile.LOG_FILE_HEADER_SIZE);
			Scan ls;
			for(int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
				ls = (Scan) gdb.getIOManager().getUlog(i).getLogToFile().openForwardScan(startAt, null);
				HashMap<LogInstance, LogRecord> records = null;
				// backward scan records in reverse order
				while ((records =  ls.getNextRecord(0)) != null) 
				{
					Iterator<Entry<LogInstance, LogRecord>> irecs = records.entrySet().iterator();
					while(irecs.hasNext()) {
						Entry<LogInstance, LogRecord> recEntry = irecs.next();
						LogRecord record = recEntry.getValue();
						System.out.printf("Tablespace %d Log record:%s%n",i,record);
					}
				}
			}
	}


}
