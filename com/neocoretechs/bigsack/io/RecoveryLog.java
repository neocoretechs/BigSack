package com.neocoretechs.bigsack.io;
import java.io.IOException;

import com.neocoretechs.arieslogger.core.LogInstance;
import com.neocoretechs.arieslogger.core.impl.FileLogger;
import com.neocoretechs.arieslogger.core.impl.LogToFile;
import com.neocoretechs.bigsack.Props;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.BlockDBIO;

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
* LogToFile is the main ARIES subsystem class used, with FileLogger being the higher level construct
* @author Groff
*/

public final class RecoveryLog  {
	private BlockDBIO blockIO;
	FileLogger fl = null;
	LogToFile ltf = null;
	LogInstance firstTrans = null;
	BlockAccessIndex tblk = null;
	public BlockDBIO getBlockIO() {
		return blockIO;
	}

	public RecoveryLog(BlockDBIO tglobalio, boolean create) throws IOException {
		blockIO = tglobalio;
		tblk = new BlockAccessIndex(blockIO); // for writeLog reserved
		ltf = new LogToFile(blockIO);
		fl = (FileLogger) ltf.getLogger();
		ltf.boot(create);
	}
	
	public void stop( ) throws IOException {
		ltf.stop();
	}
	
	public LogToFile getLogToFile() {
		return ltf;
	}
	/**
	* Write log entry - uses current db.
	* This is initiated before buffer pool block flush (writeblk). Get the original block
	* from deep store and log it as undoable
	* @param blk The block instance, payload of block about to be written to log
	* @exception IOException if cannot open or write
	*/
	public void writeLog(BlockAccessIndex blk) throws IOException {
		if( Props.DEBUG ) {
			System.out.println("RecoveryLog.writeLog "+blk.toString());
		}
		tblk.setTemplateBlockNumber(blk.getBlockNum());
		blockIO.getIOManager().FseekAndRead(tblk.getBlockNum(), tblk.getBlk());
		UndoableBlock undoBlk = new UndoableBlock(tblk, blk);
		if( firstTrans == null )
			firstTrans = fl.logAndDo(blockIO, undoBlk);
		else
			fl.logAndDo(blockIO, undoBlk);
		blk.getBlk().setInlog(true);
		return;
	}
	/**
	 * Remove archived files and reset log file 1 to its primordial state
	 * 
	 * @throws IOException
	 */
	public void commit() throws IOException {
		if(Props.DEBUG) System.out.println("Commit called");
		firstTrans = null;
		ltf.initializeLogFileSequence();
	}
	
	/**
	 * Version of method called when starting and we see an undolog ready to restore 
	 * @throws IOException
	 */
	public void rollBack() throws IOException {
		rollBackCache(); // synch main file buffs
		blockIO.forceBufferClear(); // flush buffer pool
		if( firstTrans != null)
			fl.undo(blockIO, firstTrans, null);
		else
			if( Props.DEBUG) System.out.println("No initial transaction recorded for rollback");
		firstTrans = null;
	}
	
	/**
	* This operation reads restored blocks to cache if they exist
	* therein
	* @exception IOException If we can't replace blocks
	*/
	public void rollBackCache() throws IOException {
		blockIO.getIOManager().Fforce(); // make sure we synch our main file buffers
	}
	
	public void resetLog() throws IOException {
		clear();
	}
	
	/**
	*/
	private void clear() throws IOException {

		if( Props.DEBUG ) System.out.println("RecoveryLog cleared");
	}
	/**
	 * Take a checkpoint. Force buffer flush, then write checkpoint 
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public void checkpoint() throws IllegalAccessException, IOException {
		blockIO.forceBufferWrite();
		ltf.checkpoint(true);
		if( Props.DEBUG ) System.out.println("Checkpoint taken");
	}


}
