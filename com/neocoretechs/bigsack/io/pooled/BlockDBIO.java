package com.neocoretechs.bigsack.io.pooled;

import java.io.IOException;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.Optr;
import com.neocoretechs.bigsack.io.RecoveryLog;

/*
* Copyright (c) 2003,2014 NeoCoreTechs
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
* Session-level IO used by GlobalDBIO
* This module is where the recovery logs are initialized because the logs operate at the block (database page) level.
* When this module is instantiated the RecoveryLog is assigned to 'ulog' and a roll forward recovery
* is started. If there are any records in the log file they will scanned for low water marks and
* checkpoints etc and the determination is made based on the type of log record encountered.
* Our log granularity is the page level. We store DB blocks and their original mirrors to use in
* recovery. At the end of recovery we restore the logs to their initial state, as we do on a commit. 
* There is a simple paradigm at work here, we carry a single block access index in this class and use it
* to cursor through the blocks as we access them.
* @author Groff
*/
public class BlockDBIO extends GlobalDBIO implements BlockDBIOInterface {
	private static final boolean DEBUG = false;
	private BlockAccessIndex lbai  = null;

	protected synchronized Datablock getBlk() { return lbai.getBlk(); }
	public synchronized short getByteindex() { return lbai.byteindex; }
	protected synchronized void moveByteindex(short runcount) { lbai.byteindex += runcount; }
	protected synchronized void incrementByteindex() { ++lbai.byteindex; }
	
	protected synchronized BlockAccessIndex getBlockIndex() { return lbai;}

	/**
	* Create the block IO and up through the chain to global IO. After constructing, create a recovery log instance
	* and determine if a roll forward recovery is needed. The flow is create_recovery_log which calls boot()
	* undolog instance is then set after construction.Finally, the LogToFile instance is extracted and 'recover' is called
	* @param transId 
	* @param create 
	* @param objname 
	* @exception IOException If problems setting up IO
	*/
	public BlockDBIO(String objname, boolean create, long transId) throws IOException {
		super(objname, create, transId);
		// create the ARIES protocol recovery log
		setUlog(new RecoveryLog(this));
		// attempt recovery if needed
		getUlog().getLogToFile().recover();
	}
	/**
	 * Bring up IO without logging
	 * @param dbname
	 * @throws IOException
	 */
	protected BlockDBIO(String dbname) throws IOException {
		super(dbname, false, -1);
		// create the ARIES protocol recovery log
		setUlog(new RecoveryLog(this));
	}
	/**
	 * Get the data block portion of our block access index
	 */
	public synchronized Datablock getDatablock() {
		return lbai.getBlk();
	}
	/**
	 * Get the physical block number of our block access index
	 */
	public synchronized long getCurblock() {
		return lbai.getBlockNum();
	}
	/**
	 * Set our current blockaccessindex to the passed one, perform an allocation on it, and set
	 * the byteindex cursor to 0, basically, get it and get it ready to read/write
	 * @param tbai
	 * @throws IOException
	 */
	synchronized void setLbn(BlockAccessIndex tbai) throws IOException {
		lbai = tbai;
		alloc(lbai);
		lbai.setByteindex((short) 0);
	}
	/**
	 * dealloc outstanding block. if not null, do a dealloc and set null
	 * @throws IOException
	 */
	public synchronized void deallocOutstanding() throws IOException {
		if (lbai != null) {
			dealloc(lbai.getBlockNum());
			//globalIO.Fforce();
			lbai = null;
		}		
	}
	
	/**
	 * Deallocate the outstanding block and call commit on the recovery log
	 * @throws IOException
	 */
	public synchronized void deallocOutstandingCommit() throws IOException {
		deallocOutstanding();
		commitBufferFlush();
		getUlog().commit();
	}
	/**
	 * Deallocate the outstanding block and call rollback on the recovery log
	 * @throws IOException
	 */
	public synchronized void deallocOutstandingRollback() throws IOException {
		deallocOutstanding();
		rollbackBufferFlush();
		getUlog().rollBack();
	}
	
	/**
	* Find or add the block to in-mem list.  First deallocate the currently
	* used block, get the new block, then allocate it
	* @param tbn The virtual block
	* @exception IOException If low-level access fails
	*/
	public synchronized void findOrAddBlock(long tbn) throws IOException {
		if( DEBUG )
			System.out.println("BlockDBIO.findOrAddBlock:"+valueOf(tbn)+" current:"+lbai);
		if (lbai != null) {
			if (tbn == lbai.getBlockNum()) {
				if( DEBUG )
					System.out.println("BlockDBIO.findOrAddBlock:"+valueOf(tbn)+" target = current "+lbai);
				lbai.setByteindex((short) 0);
				return;
			}
			dealloc(lbai.getBlockNum());
		}
		setLbn(findOrAddBlockAccess(tbn));
	}
	
	/**
	* objseek - seek to offset within block
	* @param adr block/offset to seek to
	* @exception IOException If problem seeking block
	* @see Optr
	*/
	public synchronized void objseek(Optr adr) throws IOException {
		if (adr.getBlock() == -1L)
			throw new IOException("Sentinel block seek error");
		findOrAddBlock(adr.getBlock());
		lbai.setByteindex(adr.getOffset());
	}
	/**
	* objseek - seek to offset within block
	* @param adr long block to seek to
	* @exception IOException If problem seeking block
	* @see Optr
	*/
	public synchronized void objseek(long adr) throws IOException {
		if (adr == -1L)
			throw new IOException("Sentinel block seek error");
		findOrAddBlock(adr);
		lbai.setByteindex((short) 0);
	}

	/**
	* getnextblk - read the next chained Datablock
	* @return true if success
	* @exception IOException If we cannot read next block
	*/
	public synchronized boolean getnextblk() throws IOException {
		if (lbai.getBlk().getNextblk() == -1L) {
			if( DEBUG )
				System.out.println("BlockDBIO.getnextblk returning with no next block "+lbai);
			return false;
		}
		findOrAddBlock(lbai.getBlk().getNextblk());
		return true;
	}

	/**
	* Acquire first Datablock, set position, link it to last
	* @return The block number acquired
	* @exception IOException If the block cannot be acquired
	*/
	public synchronized long acquireBlock() throws IOException {
		setLbn(acquireblk(lbai));
		return getCurblock();
	}
	/**
	* Steal block from free list, link it to nothing
	* @return The block number acquired
	* @exception IOException If the block cannot be acquired 
	*/
	public synchronized long stealBlock() throws IOException {
		setLbn(stealblk(lbai)); // pass for dealloc
		return getCurblock();
	}

	/**
	* new_node_position<br>
	* determine location of new node, store in new_node_pos.
	* Attempts to cluster entries in used blocks near insertion point
	* @return The Optr pointing to the new node position
	* @exception IOException If we cannot get block for new node
	*/
	public synchronized Optr new_node_position() throws IOException {
		if (getNew_node_pos_blk() == -1L) {
			stealBlock();
		} else {
			objseek(Optr.valueOf(getNew_node_pos_blk()));
			// ok, 5 bytes is rather arbitrary but seems a waste to start a big ole object so close to the end of a block
			if (getDatablock().getBytesused()+5 >= DBPhysicalConstants.DATASIZE)
				stealBlock();
		}
		return new Optr(getCurblock(), getDatablock().getBytesused());
	}
	/**
	* set_new_node_position<br>
	* set location of next new node, store in new_node_pos.
	* Attempts to cluster entries in used blocks near insertion point.
	* We leave it to the app to decide when to call this and use for packing
	*/
	public synchronized void set_new_node_position() {
		setNew_node_pos_blk(getCurblock());
	}

	/**
	* size - determine number elements, override this with concrete
	* @return The number of elements
	* @exception IOException If low-level IO has failed
	*/
	synchronized long size() throws IOException {
		return 0L;
	}
	/**
	* empty - determine if but even one exists
	* @return true if table is empty
	* @exception IOException If low-level IO has failed
	*/
	synchronized boolean empty() throws IOException {
		return (size() == 0L);
	}

}
