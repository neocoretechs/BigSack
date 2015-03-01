package com.neocoretechs.bigsack.io;
import java.io.IOException;

import com.neocoretechs.arieslogger.core.LogInstance;
import com.neocoretechs.arieslogger.core.impl.FileLogger;
import com.neocoretechs.arieslogger.core.impl.LogToFile;
import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.Props;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.BlockDBIO;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;

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
* @author Groff
*/
public final class RecoveryLogManager  {
	private static boolean DEBUG = true;
	private BlockDBIO blockIO;
	private FileLogger[] fl = null;
	private LogToFile[] ltf = null;
	private LogInstance[] firstTrans = null;
	private BlockAccessIndex tblk = null;
	
	public BlockDBIO getBlockIO() {
		return blockIO;
	}
	/**
	 * Call with IO manager, will create the LogToFile
	 * @param tglobalio
	 * @throws IOException
	 */
	public RecoveryLogManager(BlockDBIO tglobalio) throws IOException {
		blockIO = tglobalio;
		tblk = new BlockAccessIndex(blockIO); // for writeLog reserved
		ltf = new LogToFile[DBPhysicalConstants.DTABLESPACES];
		fl = new FileLogger[DBPhysicalConstants.DTABLESPACES];
		firstTrans = new LogInstance[DBPhysicalConstants.DTABLESPACES];
		for(int tablespace = 0; tablespace < DBPhysicalConstants.DTABLESPACES; tablespace++) {
			ltf[tablespace] = new LogToFile(blockIO, tablespace);
			fl[tablespace] = (FileLogger) ltf[tablespace].getLogger();
			ltf[tablespace].boot();
		}
	}
	/**
	 * Close the random access files and buffers for recovery log. Call stop in LogToFile
	 * Closes logAccessfile and calls deleteObsoleteLogFiles IF NO CORRUPTION
	 * @throws IOException
	 */
	public synchronized void stop( ) throws IOException {
		if( DEBUG )
			System.out.println("RecoveryLogManager.stop invoked.");
		for(int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++)
			ltf[i].stop();
		if( DEBUG )
			System.out.println("RecoveryLogManager.stop terminated.");
	}
	
	public synchronized void stop(int tblsp) throws IOException {
		if( DEBUG )
			System.out.println("RecoveryLog.stop invoked for tablespace "+tblsp);
		ltf[tblsp].stop();
	}
	
	public LogToFile getLogToFile(int tablespace) {
		return ltf[tablespace];
	}
	/**
	* Write log entry - uses current db. Set inlog true
	* This is initiated before buffer pool block flush (writeblk). Get the original block
	* from deep store and log it as undoable
	* @param blk The block instance, payload of block about to be written to log
	* @exception IOException if cannot open or write
	*/
	public synchronized void writeLog(BlockAccessIndex blk) throws IOException {
		//if( DEBUG ) {
		//	System.out.println("RecoveryLogManager.writeLog "+blk.toString());
		//}
		tblk.setTemplateBlockNumber(blk.getBlockNum());
		blockIO.getIOManager().FseekAndRead(tblk.getBlockNum(), tblk.getBlk());
		UndoableBlock undoBlk = new UndoableBlock(tblk, blk);
		int tblsp = GlobalDBIO.getTablespace(blk.getBlockNum());
		if( firstTrans[tblsp] == null )
			firstTrans[tblsp] = fl[tblsp].logAndDo(blockIO, undoBlk);
		else
			fl[tblsp].logAndDo(blockIO, undoBlk);
		blk.getBlk().setInlog(true);
		return;
	}
	/**
	 * Remove archived files and reset log file 1 to its primordial state
	 * 
	 * @throws IOException
	 */
	public synchronized void commit() throws IOException {
		for(int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			if(DEBUG) System.out.println("RecoveryLogManager.commit called for db "+ltf[i].getDBName()+" tablespace "+i+" log seq. int.");
			firstTrans[i] = null;
			ltf[i].stop();
			ltf[i].deleteObsoleteLogfilesOnCommit();
			ltf[i].initializeLogFileSequence();
		}
	}
	/**
	 * Remove archived files and reset log file 1 to its primordial state
	 * 
	 * @throws IOException
	 */
	public synchronized void commit(int tblsp) throws IOException {
			if(DEBUG) System.out.println("RecoveryLogManager.commit called for db "+ltf[tblsp].getDBName()+" tablespace "+tblsp+" log seq. int.");
			firstTrans[tblsp] = null;
			ltf[tblsp].stop();
			ltf[tblsp].deleteObsoleteLogfilesOnCommit();
			ltf[tblsp].initializeLogFileSequence();
	}
	/**
	 * Version of method called when starting and we see an undolog ready to restore 
	 * @throws IOException
	 */
	public synchronized void rollBack() throws IOException {
		rollBackCache(); // synch main file buffs
		blockIO.forceBufferClear(); // flush buffer pools
		for(int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			if( firstTrans[i] != null) {
				fl[i].undo(blockIO, firstTrans[i], null);
				if(DEBUG) System.out.println("RecoveryLogManager.rollback Undo initial transaction recorded for rollback in tablespace "+i+" in "+ltf[i].getDBName());
				firstTrans[i] = null;
				ltf[i].deleteObsoleteLogfilesOnCommit();
				ltf[i].initializeLogFileSequence();
			}
		}
	}
	
	/**
	* This operation reads restored blocks to cache if they exist
	* therein
	* @exception IOException If we can't replace blocks
	*/
	public synchronized void rollBackCache() throws IOException {
		if(DEBUG )
			System.out.println("RecoveryLogManager.rollbackCache invoked");
		blockIO.getIOManager().Fforce(); // make sure we synch our main file buffers
	}
	
	/**
	 * Take a checkpoint. Force buffer flush, then write checkpoint. A checkpoint demarcates
	 * a recovery position in logs from which a recovery will roll forward. An undo from a checkpoint
	 * will restore the raw store to its state at that checkpoint. Use with caution and only with wise counsel. 
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public synchronized void checkpoint() throws IllegalAccessException, IOException {
		blockIO.forceBufferWrite();
		for(int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++)
			ltf[i].checkpoint(true);
		if( DEBUG ) System.out.println("RecoveryLogManager.checkpoint. Checkpoint taken for db "+blockIO.getDBName());
	}


}
