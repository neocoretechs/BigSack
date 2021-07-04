package com.neocoretechs.bigsack.io;
import java.io.IOException;

import com.neocoretechs.arieslogger.core.LogInstance;
import com.neocoretechs.arieslogger.core.impl.FileLogger;
import com.neocoretechs.arieslogger.core.impl.LogToFile;

import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.pooled.MappedBlockBuffer;

/*
* Copyright (c) 1997,2002,2003,2014 NeoCoreTechs
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
* This class is the bridge to the recovery log subsystem which is based on the ARIES protocol.
* LogToFile is the main ARIES subsystem class used, with FileLogger being the higher level construct.
* We keep arrays of FileLogger, LogToFile, etc by tablespace and multiplex the calls to the proper tablespace
* This module is where the recovery logs are initialized because the logs operate at the block (database page) level.
* When this module is instantiated the RecoveryLogManager is assigned and a roll forward recovery
* is started. If there are any records in the log file they will scanned for low water marks and
* checkpoints etc and the determination is made based on the type of log record encountered.
* Our log granularity is the page level. We store DB blocks and their original mirrors to use in
* recovery. At the end of recovery we restore the logs to their initial state, as we do on a commit. 
* There is a simple paradigm at work here, we carry a single block access index in this class and use it
* to cursor through the blocks as we access them.
* @author Jonathan Groff Copyright (C) NeoCoreTechs 2015,2021
*/
public final class RecoveryLogManager  {
	private static boolean DEBUG = false;
	private GlobalDBIO globalDBIO;
	private FileLogger fl = null;
	private LogToFile ltf = null;
	private LogInstance firstTrans = null;
	private BlockAccessIndex tblk = null;
	private int tablespace;
	
	public GlobalDBIO getBlockIO() {
		return globalDBIO;
	}
	/**
	 * Call with IO manager, will create the LogToFile
	 * @param tglobalio
	 * @throws IOException
	 */
	public RecoveryLogManager(GlobalDBIO tglobalio, int tablespace) throws IOException {
		this.globalDBIO = tglobalio;
		this.tablespace = tablespace;
		this.tblk = BlockAccessIndex.createPageForRecovery(globalDBIO); // for writeLog reserved
		this.ltf = new LogToFile(globalDBIO, tablespace);
		this.fl = (FileLogger) ltf.getLogger();
		ltf.bootForRead(); // Fopen may initiate recovery, and is guaranteed to call stop()
		if(DEBUG)
			System.out.printf("%s constructed with temp blk=%s LogToFile=%s FileLogger=%s%n", this.getClass().getName(),this.tblk,this.ltf,this.fl);
	}
	
	public LogToFile getLogToFile() {
		return ltf;
	}
	/**
	* Write log entry. If necessary, boot the LogToFile for write if LogOut is null.<p/>
	* Get the original block from deep store and log it as undoable using FileLogger,
	* this will write the changed block to the DB after logging.<p/>
	* Set changed block inlog true. Set incore false. Reset and clear BlockAccessIndex access latch.<p/>
	* This is initiated before buffer pool block flush (writeblk), for instance. 
	* @param blk The block instance, payload of block about to be written to log
	* @exception IOException if cannot open or write
	*/
	public synchronized void writeLog(BlockAccessIndex blk) throws IOException {
		if( DEBUG ) {
			System.out.printf("%s.writeLog ioManager=%s blk=%s%n",this.getClass().getName(),globalDBIO.getIOManager(),blk.toString());
		}
		// Have we issued an intermediate commit or rollbacK?
		if(ltf.getLogOut() == null) {
			ltf.bootForWrite(); // commit will call stop on LogToFile
			if( DEBUG ) {
				System.out.printf("%s.writeLog bootForWrite ltf=%s%n",this.getClass().getName(),ltf);
			}
		}
		tblk.resetBlock(true);
		tblk.setBlockNumber(blk.getBlockNum());
		assert( tablespace == GlobalDBIO.getTablespace(blk.getBlockNum()));
		globalDBIO.getIOManager().readDirect(tablespace, GlobalDBIO.getBlock(blk.getBlockNum()), tblk.getBlk());
		if( DEBUG ) {
			System.out.printf("%s.writeLog read original page=%s%n",this.getClass().getName(),tblk);
		}
		UndoableBlock undoBlk = new UndoableBlock(tblk, blk);
		if( firstTrans == null )
			firstTrans = fl.logAndDo(globalDBIO, undoBlk);
		else
			fl.logAndDo(globalDBIO, undoBlk);
		if( DEBUG ) {
			System.out.printf("%s.writeLog firstTrans=%s%n",this.getClass().getName(),firstTrans);
		}
		blk.getBlk().setInlog(true);
		blk.getBlk().setIncore(false);
		if( DEBUG ) {
			System.out.println("RecoveryLogManager.writeLog EXIT with "+blk.toString());
		}
	}
	/**
	 * Remove archived files and reset log file 1 to its primordial state
	 * 
	 * @throws IOException
	 */
	public synchronized void commit(MappedBlockBuffer pool) throws IOException {
			if(DEBUG) System.out.println("RecoveryLogManager.commit called for db "+ltf.getDBName()+" tablespace "+tablespace+" log seq. int.");
			firstTrans = null;
			fl.commit(globalDBIO, pool);
			ltf.stop();
	}

	/**
	 * Version of method called when starting and we see an undolog ready to restore 
	 * @throws IOException
	 */
	public synchronized void rollBack() throws IOException {
		rollBackCache(); // synch main file buffs
		if( firstTrans != null) {
			System.out.printf("%s Rolling back %d%n",this.getClass().getName(), tablespace);
			fl.undo(globalDBIO, firstTrans, null);
			if(DEBUG) System.out.println("RecoveryLogManager.rollback Undo initial transaction recorded for rollback in tablespace "+tablespace+" in "+ltf.getDBName());
			firstTrans = null;
			ltf.stop();
		} else {
			System.out.printf("%s NOT Rolling back, firstTrans was null %d%n",this.getClass().getName(), tablespace);
		}
	}
	
	/**
	* This operation reads restored blocks to cache if they exist
	* therein
	* @exception IOException If we can't replace blocks
	*/
	private synchronized void rollBackCache() throws IOException {
		if(DEBUG )
			System.out.println("RecoveryLogManager.rollbackCache invoked");
		globalDBIO.getIOManager().Fforce(); // make sure we synch our main file buffers
	}
	
	/**
	 * Take a checkpoint. Force buffer flush, then write checkpoint. A checkpoint demarcates
	 * a recovery position in logs from which a recovery will roll forward. An undo from a checkpoint
	 * will restore the raw store to its state at that checkpoint. Use with caution and only with wise counsel. 
	 * TODO Make it a request? Or is a checkpoint imperative?
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public synchronized void checkpoint() throws IllegalAccessException, IOException {
		globalDBIO.forceBufferWrite();
		ltf.checkpoint(true);
		if( DEBUG ) System.out.println("RecoveryLogManager.checkpoint. Checkpoint taken for db "+globalDBIO.getDBName());
	}


}
