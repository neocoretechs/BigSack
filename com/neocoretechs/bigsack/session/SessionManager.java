package com.neocoretechs.bigsack.session;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import com.neocoretechs.arieslogger.core.CheckpointDaemonInterface;
import com.neocoretechs.arieslogger.core.impl.CheckpointDaemon;
import com.neocoretechs.bigsack.Props;
import com.neocoretechs.bigsack.btree.BTreeMain;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.pooled.ObjectDBIO;
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
* that accepts connections and returns a BigSackSession object
* @author Groff
*/
public final class SessionManager {
	private static Hashtable<String, BigSackSession> SessionTable = new Hashtable<String, BigSackSession>();
	@SuppressWarnings("rawtypes")
	private static Hashtable<?, ?> AdminSessionTable = new Hashtable();
	private static Vector<String> OfflineDBs = new Vector<String>();
	private static String dbPath = "/";
	private static long globalTransId = System.currentTimeMillis();
	private static CheckpointDaemonInterface checkpointDaemon = null;
	//
	// Sets the maximum number users
	@SuppressWarnings("unused")
	private static final int MAX_USERS = -1;
	//
	// Singleton setups:
	// 1.) privatized constructor; no other class can call
	private SessionManager() {
	}
	// 2.) create only instance, save it to private static
	private static SessionManager instance = new SessionManager();
	// 3.) make the instance available
	public static SessionManager getInstance() {
		return instance;
	}
	//
	// Global transaction timestamps
	private static long lastStartTime = 0L;
	private static long lastCommitTime = 0L;
	//
	public static String getDbPath() {
		 return dbPath;
	}
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
	* @param create Create database if not existing
	* @return BigSackSession The session we use to control access
	* @exception IOException If low level IO problem
	* @exception IllegalAccessException If access to database is denied
	*/
	public static synchronized BigSackSession Connect(String dbname, boolean create) throws IOException, IllegalAccessException {
		if( Props.DEBUG ) {
			System.out.println("Connecting to "+dbname+" create:"+create);
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
			// Global IO and main Btree index
			dbPath = (new File(dbname)).toPath().getParent().toString();
			ObjectDBIO objIO = new ObjectDBIO(dbname, create, getGlobalTransId());
			BTreeMain bTree =  new BTreeMain(objIO);
			hps = new BigSackSession(bTree, uid, gid);
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
	protected static synchronized void setDBOffline(String dbname)
		throws IOException {
		OfflineDBs.addElement(dbname);
		// look for session instance, then signal close
		BigSackSession hps = (SessionTable.get(dbname));
		if (hps != null) {
			hps.forceClose();
		}
	}
	protected static synchronized void setDBOnline(String name) {
		OfflineDBs.removeElement(name);
	}
	public static boolean isDBOffline(String dbname) {
		return OfflineDBs.contains(dbname);
	}
	protected static synchronized void releaseSession(BigSackSession DS) {
		SessionTable.remove(DS);
	}

	public static Hashtable<String, BigSackSession> getSessionTable() {
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
	protected static Hashtable<?, ?> getAdminSessionTable() {
		return AdminSessionTable;
	}
	
	public static CheckpointDaemonInterface getCheckpointDaemon() {
		//if(checkpointDaemon == null) {
		//	checkpointDaemon = new CheckpointDaemon();
		//}
		return checkpointDaemon;
	}
	
	public static synchronized void stopCheckpointDaemon(String dbname) throws IOException {
		BigSackSession hps = (SessionTable.get(dbname));
		if (hps != null) {
			hps.getBTree().getIO().getUlog().stop();
		}
	}

}
