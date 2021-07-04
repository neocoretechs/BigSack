package com.neocoretechs.bigsack.session;

import java.io.IOException;
import java.util.Iterator;
import java.util.stream.Stream;

import com.neocoretechs.bigsack.keyvaluepages.KeySearchResult;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;

interface TransactionInterface {

	boolean COMMIT = false;
	boolean ROLLBACK = true;
	
	long getTransactionId();

	/**
	* Close this session.
	* @param rollback true to roll back, false to commit
	* @exception IOException For low level failure
	*/
	void Close(boolean rollback) throws IOException;

	/**
	* @exception IOException for low level failure
	*/
	void Rollback() throws IOException;
	
	/**
	* Commit the blocks.
	* @exception IOException For low level failure
	*/
	void Commit() throws IOException;
	/**
	 * Checkpoint the current transaction
	 * @throws IOException 
	 * @throws IllegalAccessException 
	 */
	void Checkpoint() throws IllegalAccessException, IOException;
	/**
	* Generic session roll up.  Data is committed based on rollback param.
	* We deallocate the outstanding block
	* We iterate the tablespaces for each db removing obsolete log files.
	* Remove the WORKER threads from KeyValueMain, then remove this session from the SessionManager
	* @param rollback true to roll back, false to commit
	* @exception IOException For low level failure
	*/
	void rollupSession(boolean rollback) throws IOException;
	
}