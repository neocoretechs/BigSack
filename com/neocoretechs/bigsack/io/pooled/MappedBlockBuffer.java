package com.neocoretechs.bigsack.io.pooled;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.IoManagerInterface;
import com.neocoretechs.bigsack.io.Optr;
import com.neocoretechs.bigsack.io.RecoveryLogManager;
import com.neocoretechs.bigsack.io.request.cluster.CompletionLatchInterface;

/**
 * The MappedBlockBuffer is the buffer pool for each tablespace of each db. It functions
 * as the request processor for IoManagerInterface implementors.
 * The class functions as the used block list for BlockAccessIndex elements that represent our
 * in memory pool of disk blocks. Its construction involves keeping track of the list of
 * free blocks as well to move items between the two. 
 * There are one of these, per tablespace, per database, and act on a particular tablespace.
 * The thread is designed to interact with latches and cyclic barriers bourne on the requests. 
 * Create the request with the appropriate instance of 'this' MappedBlockBuffer to call back
 * methods here. Then the completion latch for that request is counted down.
 * In the Master IO, the completion latch of the request is monitored for the proper number
 * of counts. A series of cyclic barriers, for specific requests, allow barrier synchronization
 * via the requests to ensure a consistent state before return to the thread processing. It is
 * assumed the cyclic barriers and countdown latches are stored outside of this class to be used
 * to synch operations coming from here. A corollary is the cyclic barriers used to internally
 * synchronize then its for reuseability.
 * This class knows about the IO manager that sits above it and performs the lower level accesses
 * from the global IO class. It has no knowledge of the blockstreams and recovery manager etc.
 * @author jg
 *
 */
public class MappedBlockBuffer extends ConcurrentHashMap<Long, BlockAccessIndex> implements Runnable {
	private static final long serialVersionUID = -5744666991433173620L;
	private static final boolean DEBUG = false;
	private static final boolean NEWNODEPOSITIONDEBUG = false;
	private boolean shouldRun = true;
	private BlockingQueue<BlockAccessIndex> freeBL; // free block list
	private ObjectDBIO globalIO;
	private IoManagerInterface ioManager;
	private int tablespace;
	private int minBufferSize = 10; // minimum number of buffers to reclaim on flush attempt
	private ArrayBlockingQueue<CompletionLatchInterface> requestQueue; // Request processing queue
	private final Set<BlockChangeEvent> mObservers = Collections.newSetFromMap(new ConcurrentHashMap<BlockChangeEvent, Boolean>(0));
  
	private static int POOLBLOCKS;
	private static int QUEUEMAX = 256; // max requests before blocking
	private static int cacheHit = 0; // cache hit rate
	private static int cacheMiss = 0;
	/**
	 * Construct the buffer for this tablespace and link the global IO manager
	 * @param ioManager Manager such as MultiThreadedIOManager or ClusterIOManager
	 * @param tablespace
	 * @throws IOException If we try to add an active block the the freechain
	 */
	public MappedBlockBuffer(IoManagerInterface ioManager, int tablespace) throws IOException {
		super(ioManager.getIO().getMAXBLOCKS()/DBPhysicalConstants.DTABLESPACES);
		POOLBLOCKS = ioManager.getIO().getMAXBLOCKS()/DBPhysicalConstants.DTABLESPACES;
		this.globalIO = ioManager.getIO();
		this.ioManager = ioManager;
		this.tablespace = tablespace;
		this.freeBL = new ArrayBlockingQueue<BlockAccessIndex>(POOLBLOCKS, true); // free blocks
		// populate with blocks, they're all free for now
		for (int i = 0; i < POOLBLOCKS; i++) {
			// new Long(i*DBPhysicalConstants.DBLOCKSIZ)
			try {
				freeBL.put(new BlockAccessIndex(true));
			} catch (InterruptedException e) {}
		}
		minBufferSize = POOLBLOCKS/10; // we need at least one
		requestQueue = new ArrayBlockingQueue<CompletionLatchInterface>(QUEUEMAX, true); // true maintains FIFO order	   
	}
	
	public void addBlockChangeObserver(BlockChangeEvent bce) { mObservers.add(bce); }
	
	/**
    * This method notifies currently registered observers about BlockChangeEvent change.
    * We wont do this for methods that return the BLockAccessIndex from one block returned by a call
    * because we assume the operation will be carried out downstream so we save this expensive
    * operation. But in a case where we are iterating through multiple blocks we will notify the
    * BlockStream and other listeners of the event. In short, dont look for total updates on this channel.
    */
    private void notifyObservers(BlockAccessIndex bai) {
        for (BlockChangeEvent observer : mObservers) { // this is safe due to thread-safe Set
            observer.blockChanged(tablespace, bai);
        }
    }
    
	public synchronized IoManagerInterface getIoManager() { return ioManager; }
	public synchronized ObjectDBIO getGlobalIO() { return globalIO;}
	/**
	 * acquireblk - get block from unused chunk or create a new blockchain. this method links a previous block<br>
	 * return acquired block
	 * Add a block to table of blocknums and block access index.
	 * No setting of block in BlockAccessIndex, no initial read reading through ioManager.addBlockAccessnoRead
	 * @param lastGoodBlk The block for us to link to
	 * @return The BlockAccessIndex
	 * @exception IOException if db not open or can't get block
	 */
	public synchronized BlockAccessIndex acquireblk(BlockAccessIndex lastGoodBlk) throws IOException {
		if( DEBUG )
			System.out.println("MappedBlockBuffer.acquireblk, last good is "+lastGoodBlk);
		long newblock;
		BlockAccessIndex ablk = lastGoodBlk;
		// this way puts it all in one tablespace
		//int tbsp = getTablespace(lastGoodBlk.getBlockNum());
		//int tbsp = new Random().nextInt(DBPhysicalConstants.DTABLESPACES);
		newblock = GlobalDBIO.makeVblock(tablespace, ioManager.getNextFreeBlock(tablespace));
		// update old block
		ablk.getBlk().setNextblk(newblock);
		ablk.getBlk().setIncore(true);
		ablk.getBlk().setInlog(false);
		//ablk.decrementAccesses();
		// new block number for BlockAccessIndex set in addBlockAccessNoRead
		BlockAccessIndex dblk =  addBlockAccessNoRead(new Long(newblock));
		dblk.getBlk().resetBlock();
		dblk.getBlk().setPrevblk(lastGoodBlk.getBlockNum());
		dblk.getBlk().setIncore(true);
		if( DEBUG )
			System.out.println("MappedBlockBuffer.acquireblk returning from:"+ablk+" to:"+dblk);
		return dblk;
	}
	/**
	 * stealblk - get block from unused chunk or create chunk and get.
	 * We dont try to link it to anything as we do in acquireblk because our little linked lists
	 * of instance blocks are not themselves linked to anything<br>
	 * Read via addBlockAccessNoRead with new block address.
	 * @param currentBlk Current block so we can deallocate and replace it off chain
	 * @return The BlockAccessIndex of stolen blk
	 * @exception IOException if db not open or can't get block
	 */
	public synchronized BlockAccessIndex stealblk(BlockAccessIndex currentBlk) throws IOException {
		if( DEBUG )
			System.out.println("MappedBlockBuffer.stealblk, last good is "+currentBlk);
		long newblock;
		if (currentBlk != null)
			currentBlk.decrementAccesses();
		newblock = GlobalDBIO.makeVblock(tablespace, ioManager.getNextFreeBlock(tablespace));
		// new block number for BlockAccessIndex set in addBlockAccessNoRead
		// calls setBlockNumber, which should up the access
		BlockAccessIndex dblk = addBlockAccessNoRead(new Long(newblock));
		dblk.getBlk().resetBlock();
		if( DEBUG )
			System.out.println("MappedBlockBuffer.stealblk, returning "+dblk);
		return dblk;
	}

	
	public synchronized void put(BlockAccessIndex bai) { 
			freeBL.add(bai); 
	}
	
	public synchronized void forceBufferClear() {
		Enumeration<BlockAccessIndex> it = elements();
		while(it.hasMoreElements()) {
				BlockAccessIndex bai = (BlockAccessIndex) it.nextElement();
				bai.resetBlock(true); // reset and clear access latch
				put(bai);
		}
		clear();
	}
		
	/**
	* seek_fwd - long seek forward from current spot. If we change blocks, notify the observers
	* @param offset offset from current
	* @exception IOException If we cannot acquire next block
	*/
	public synchronized boolean seek_fwd(BlockAccessIndex tbai, long offset) throws IOException {
		long runcount = offset;
		BlockAccessIndex lbai = tbai;
		do {
			if (runcount >= (lbai.getBlk().getBytesused() - lbai.getByteindex())) {
				runcount -= (lbai.getBlk().getBytesused() - lbai.getByteindex());
				lbai = getnextblk(lbai);
				if(lbai.getBlk().getNextblk() == -1)
					return false;
			} else {
				lbai.setByteindex((short) (lbai.getByteindex() + runcount));
				runcount = 0;
			}
		} while (runcount > 0);
		return true;
	
	}
	/**
	* new_node_position<br>
	* determine location of new node, store in new_node_pos.
	* Attempts to cluster entries in used blocks near insertion point
	* @return The Optr pointing to the new node position
	* @exception IOException If we cannot get block for new node
	*/
	public synchronized Optr getNewNodePosition(BlockAccessIndex lbai) throws IOException {
		long blockNum = lbai.getBlockNum();
		short bytesUsed = lbai.getBlk().getBytesused();
		if (blockNum == -1L) {
			BlockAccessIndex tbai = stealblk(lbai);
			blockNum = tbai.getBlockNum();
			bytesUsed = tbai.getBlk().getBytesused();
		}
		if( NEWNODEPOSITIONDEBUG )
			System.out.println("MappedBlockBuffer.getNewNodePosition "+blockNum+" "+bytesUsed);
		return new Optr(blockNum, bytesUsed);
	}
	/**
	* Attempt to free pool blocks by the following:
	* we will try to allocate at least 1 free block if size is 0, if we cant, throw exception
	* If we must sweep buffer, try to fee up to minBufferSize
	* We collect the elements and then transfer them from used to free block list
	* 1) Try and roll through the buffer finding 0 access blocks
	* 2) if 0 access block found, check if its in core, written and then deallocated but not written
	* 3) If its in core and not in log, write the log then the block
	* 4) If BOTH in core and in log, throw an exception, should NEVER be in core and under write.
	* 5) If it is not the 0 index block per tablespace then add it to list of blocks to remove
	* 6) check whether minimum blocks to recover reached, if so continue to removal
	* 7) If no blocks reached at end, an error must be thrown
	* 
	*/
	public synchronized void checkBufferFlush(long Lbn) throws IOException {
			int latched = 0;
			int bufSize = this.size();
			if( bufSize < POOLBLOCKS )
				return;
			Enumeration<BlockAccessIndex> elbn = this.elements();
			int numGot = 0;
			BlockAccessIndex[] found = new BlockAccessIndex[minBufferSize];// our candidates
			while (elbn.hasMoreElements()) {
				BlockAccessIndex ebaii = (elbn.nextElement());
				if( DEBUG ) {
					System.out.println("MappedBlockBuffer.checkBufferFlush Block buffer "+ebaii);
					if( ebaii.getAccesses() > 1 )
						System.out.println("****COMMIT BUFFER access "+ebaii.getAccesses()+" for buffer "+ebaii);
				}
				assert(!(ebaii.getBlk().isIncore() && ebaii.getBlk().isInlog())) : "****COMMIT BUFFER block in core and log simultaneously! "+ebaii;
				if (Lbn != ebaii.getBlockNum() && ebaii.getAccesses() <= 1) {
					if(ebaii.getBlk().isIncore()) {
						continue; // cant throw out this one
						//ioManager.getUlog(tablespace).writeLog(ebaii); // will set incore, inlog, and push to raw store via applyChange of Loggable
						//throw new IOException("Accesses 0 but incore true " + ebaii);
					}
					// Try not to toss page blocks
					if( ebaii.getBlk().isKeypage() )
						continue;
					found[numGot] = ebaii;
					if( ++numGot == minBufferSize ) {
						break;
					}
				} else {
					if( DEBUG )
						if( ebaii.getAccesses() > 1 ) System.out.println("FLUSH:"+ebaii);
					++latched;
				}
			}
			//
			// We found none this way, proceed to look for accessed blocks
			int latched2 = 0;
			if( numGot == 0 ) {
				elbn = this.elements();
				while (elbn.hasMoreElements()) {
					BlockAccessIndex ebaii = (elbn.nextElement());
					if( DEBUG )
						System.out.println("MappedBlockBuffer.checkBufferFlush PHASE II Block buffer "+ebaii+" "+this);
					if(Lbn != ebaii.getBlockNum() && ebaii.getAccesses() == 1) {
						if(ebaii.getBlk().isIncore() && !ebaii.getBlk().isInlog()) {
							if( DEBUG )
								System.out.println("MappedBlockBuffer.checkBufferFlush set to write pool entry to log "+ebaii);
							ioManager.getUlog(tablespace).writeLog(ebaii); // will set incore, inlog, and push to raw store via applyChange of Loggable
							//throw new IOException("Accesses 0 but incore true " + ebaii);
						}
						
						// Dont toss block at 0,0. its our BTree root and we will most likely need it soon
						// other key pages may have to go
						if( ebaii.getBlockNum() == 0L )
							continue;
						found[numGot] = ebaii;
						if( ++numGot == minBufferSize ) {
							break;
						}
					} else {
						++latched2;
					}
				}
				if( numGot == 0 ) {
					if( DEBUG ) {
						Enumeration elems = this.elements();
						while(elems.hasMoreElements())
							System.out.println(elems.nextElement());
					}
					throw new IOException("INCREASE BUFFER POOL SIZE. Unable to free up blocks with "+latched+" singley and "+latched2+" MULTIPLY latched.");
				}
			}
			
			for(int i = 0; i < numGot; i++) {
				if( found[i] != null ) {				
					this.remove(found[i].getBlockNum());
					// reset all to zero before re-freechain
					found[i].resetBlock(true); // clear access latch true
					freeBL.add(found[i]);
				}
			}

	}
	/**
	 * Commit all outstanding blocks in the buffer.
	 * @throws IOException
	 */
	public synchronized void commitBufferFlush(RecoveryLogManager rlm) throws IOException {
		Enumeration<BlockAccessIndex> elbn = this.elements();
		while (elbn.hasMoreElements()) {
				BlockAccessIndex ebaii = (elbn.nextElement());
				if( ebaii.getAccesses() > 1 )
					throw new IOException("****COMMIT BUFFER access "+ebaii.getAccesses()+" for buffer "+ebaii);
				if(ebaii.getBlk().isIncore() && ebaii.getBlk().isInlog())
					throw new IOException("****COMMIT BUFFER block in core and log simultaneously! "+ebaii);
				if (ebaii.getAccesses() < 2) {
					if(ebaii.getBlk().isIncore() && !ebaii.getBlk().isInlog()) {
						//ioManager.getUlog(tablespace).writeLog(ebaii); 
						// will set incore, inlog, and push to raw store via applyChange of Loggable
						if( DEBUG )
							System.out.println("MappedeBlockBuffer.commitBufferFlush of block "+ebaii);
						rlm.writeLog(ebaii);
					}
					ebaii.decrementAccesses();
					ebaii.getBlk().resetBlock();
					try {
						freeBL.put(ebaii);
					} catch (InterruptedException e) {}
				}
		}
		clear();
		cacheHit = 0;
		cacheMiss = 0;
	
	}
	/**
	 * Commit all outstanding blocks in the buffer, bypassing the log subsystem. Should be used with forethought
	 * @throws IOException
	 */
	public synchronized void directBufferWrite() throws IOException {
		Enumeration<BlockAccessIndex> elbn = this.elements();
		if(DEBUG) System.out.println("MappedBlockBuffer.direct buffer write");
		while (elbn.hasMoreElements()) {
					BlockAccessIndex ebaii = (elbn.nextElement());
					if (ebaii.getAccesses() == 0 && ebaii.getBlk().isIncore() ) {
						if(DEBUG)
							System.out.println("MappedBlockBuffer.directBufferWrite fully writing "+ebaii.getBlockNum()+" "+ebaii.getBlk());
						ioManager.FseekAndWriteFully(ebaii.getBlockNum(), ebaii.getBlk());
						ioManager.Fforce();
						ebaii.getBlk().setIncore(false);
					}
		}
	}
	
	/**
	* Find or add the block to in-mem cache. If we dont get a cache hit we go to 
	* deep store to bring it in.
	* @param tbn The virtual block to retrieve
	* @exception IOException If low-level access fails
	*/
	public synchronized BlockAccessIndex findOrAddBlock(long bn) throws IOException {
		if( DEBUG ) {
			System.out.println("MappedBlockBuffer.findOrAddBlock "+GlobalDBIO.valueOf(bn)+" "+this);
		}
		Long Lbn = new Long(bn);
		BlockAccessIndex bai = getUsedBlock(bn);
		if( DEBUG ) {
			System.out.println("MappedBlockBuffer.findOrAddBlock "+GlobalDBIO.valueOf(bn)+" got block "+bai+" "+this);
		}
		if (bai != null) {
			if( bai.getAccesses() == 0 )
				bai.addAccess();
			if( DEBUG )
				System.out.println("MappedBlockBuffer.findOrAddBlock returning "+bai);
			return bai;
		}
		// didn't find it, we must add
		return addBlockAccess(Lbn);
	}
	/**
	* Get block from free list, puts in used list of block access index.
	* Comes here when we can't find blocknum in table in findOrAddBlock.
	* New instance of BlockAccessIndex causes allocation
	* @param Lbn block number to add
	* @exception IOException if new dblock cannot be created
	*/
	private BlockAccessIndex addBlockAccess(Long Lbn) throws IOException {
		// make sure we have open slots
		if( DEBUG ) {
			System.out.println("MappedBlockBuffer.addBlockAccess "+GlobalDBIO.valueOf(Lbn)+" "+this);
		}
		checkBufferFlush(Lbn);
		BlockAccessIndex bai = null;
		try {
			bai = freeBL.take();
		} catch (InterruptedException e) {}
		// ups access, set blockindex 0
		bai.setBlockNumber(Lbn);
		ioManager.FseekAndRead(Lbn, bai.getBlk());
		put(Lbn, bai);
		if( DEBUG ) {
				System.out.println("MappedBlockBuffer.addBlockAccess "+GlobalDBIO.valueOf(Lbn)+" returning after freeBL take "+bai+" "+this);
		}
		return bai;
	}
	/**
	* Add a block to table of blocknums and block access index.
	* Comes here for acquireBlock. No setting of block in BlockAccessIndex, no initial read
	* @param Lbn block number to add
	* @exception IOException if new dblock cannot be created
	*/
	public synchronized BlockAccessIndex addBlockAccessNoRead(Long Lbn) throws IOException {
		if( DEBUG ) {
			System.out.println("MappedBlockBuffer.addBlockAccessNoRead "+GlobalDBIO.valueOf(Lbn)+" "+this);
		}
		// make sure we have open slots
		checkBufferFlush(Lbn);
		BlockAccessIndex bai = null;
		try {
			bai = freeBL.take();
		} catch (InterruptedException e) {}
		// ups the access latch, set byteindex to 0
		bai.setBlockNumber(Lbn);
		put(Lbn, bai);
		if( DEBUG ) {
			System.out.println("MappedBlockBuffer.addBlockAccessNoRead "+GlobalDBIO.valueOf(Lbn)+" returning after freeBL take "+bai+" "+this);
		}
		return bai;
	}
	
	/**
	 * Return a block currently in the pool. If the block is not in the pool up the cache miss value and
	 * return a null
	 * @param loc The block number of a presumed pool resident block
	 * @return the BlockAccessIndex with the block or null if its not in the cache
	 */
	public synchronized BlockAccessIndex getUsedBlock(long loc) {
		if( DEBUG )
			System.out.println("MappedBlockBuffer.getusedBlock Calling for USED BLOCK "+GlobalDBIO.valueOf(loc)+" "+loc);
		BlockAccessIndex bai = get(loc);
		if( bai == null ) {
			++cacheMiss;
		} else {
			++cacheHit;
			bai.setByteindex((short) 0);
		}
		return bai;

	}
	
	/**
	* getnextblk - read the next chained Datablock based on the next block value of passed, previous block.
	* We will notify all observers of BlockChangeEvent when that happens. 
	* @param labi The BlockAccessIndex that contains next block pointer
	* @return The BlockAccessIndex of the next block in the chain, or null if there is no more
	* @exception IOException If a low level read fail occurs
	*/
	public synchronized BlockAccessIndex getnextblk(BlockAccessIndex lbai) throws IOException {
		lbai.decrementAccesses();
		if (lbai.getBlk().getNextblk() == -1L) {
			//if( DEBUG )
			//	System.out.println("MappedBlockBuffer.getnextblk returning with no next block "+lbai);
			return null;
		}
		if( DEBUG )
			System.out.println("MappedBlockBuffer.getnextblk next block fetch:"+lbai);
		BlockAccessIndex nextBlk = findOrAddBlock(lbai.getBlk().getNextblk());
		if( nextBlk != null )
			notifyObservers(nextBlk);
		return nextBlk;
	}
	
	/**
	* readn - read n bytes from pool. will start on the passed BlockAccessIndex filling buf from offs until numbyte.
	* Records spanning blocks will be successively read until buffer is full
	* @param buf byte buffer to fill
	* @param numbyte number of bytes to read
	* @return number of bytes read, -1 if we reach end of stream and/or there is no next block
	* @exception IOException If we cannot acquire next block
	*/
	public synchronized int readn(BlockAccessIndex lbai, byte[] buf, int offs, int numbyte) throws IOException {
		BlockAccessIndex tblk;
		int i = offs, runcount = numbyte, blkbytes;
		// see if we need the next block to start
		// and flag our position
		if (lbai.getByteindex() >= lbai.getBlk().getBytesused()-1)
			if((tblk=getnextblk(lbai)) != null) {
				lbai=tblk;
			} else {
				return -1;
			}
		for (;;) {
			blkbytes = lbai.getBlk().getBytesused() - lbai.getByteindex();
			if (runcount > blkbytes) {
				runcount -= blkbytes;
				System.arraycopy(
					lbai.getBlk().getData(),
					lbai.getByteindex(),
					buf,
					i,
					blkbytes);
				lbai.setByteindex((short) (lbai.getByteindex() + (short)blkbytes));
				i += blkbytes;
				if ((tblk=getnextblk(lbai)) != null) {
					lbai=tblk;
				} else {
					return (i != 0 ? (i-offs) : -1);
				}
			} else {
				System.arraycopy(
					lbai.getBlk().getData(),
					lbai.getByteindex(),
					buf,
					i,
					runcount);
				lbai.setByteindex((short) (lbai.getByteindex() + runcount));
				i += runcount;
				return (i != 0 ? (i-offs) : -1);
			}
		}
	}
	
	public synchronized int readn(BlockAccessIndex lbai, byte[] buf, int numbyte) throws IOException {
		return readn(lbai, buf, 0, numbyte);
	}
	/**
	* readn - read n bytes from pool
	* @param buf byte buffer to fill
	* @param numbyte number of bytes to read
	* @return number of bytes read
	* @exception IOException If we cannot acquire next block
	*/
	public synchronized int readn(BlockAccessIndex lbai, ByteBuffer buf, int numbyte) throws IOException {
		BlockAccessIndex tblk;
		int i = 0, runcount = numbyte, blkbytes;
		// see if we need the next block to start
		// and flag our position
		if (lbai.getByteindex() >= lbai.getBlk().getBytesused()-1)
			if((tblk=getnextblk(lbai)) != null) {
				lbai=tblk;
			} else {
				return (i != 0 ? i : -1);
			}
		for (;;) {
			blkbytes = lbai.getBlk().getBytesused() - lbai.getByteindex();
			if (runcount > blkbytes) {
				runcount -= blkbytes;
				buf.position(i);
				buf.put(lbai.getBlk().getData(), lbai.getByteindex(), blkbytes);
				lbai.setByteindex((short) (lbai.getByteindex() + (short)blkbytes));
				i += blkbytes;
				if((tblk=getnextblk(lbai)) != null) {
					lbai=tblk;
				} else {
					return (i != 0 ? i : -1);
				}
			} else {
				buf.position(i);
				buf.put(lbai.getBlk().getData(), lbai.getByteindex(), runcount);
				lbai.setByteindex((short) (lbai.getByteindex() + runcount));
				i += runcount;
				return (i != 0 ? i : -1);
			}
		}
	}
	/**
	* readi - read 1 byte from pool.
	* This method designed to be called from DBInput.
	* @return the byte as integer for InputStream
	* @exception IOException If we cannot acquire next block
	*/
	public synchronized int readi(BlockAccessIndex lbai) throws IOException {
		BlockAccessIndex tblk;
		// see if we need the next block to start
		// and flag our position
		if (lbai.getByteindex() >= lbai.getBlk().getBytesused()-1) {
			if((tblk=getnextblk(lbai)) == null) {
				return -1;
			}
			lbai = tblk;
		}
		int ret = lbai.getBlk().getData()[lbai.getByteindex()] & 255;
		lbai.setByteindex((short) (lbai.getByteindex() + 1));
		return ret;
	}
	/**
	* writen -  write n bytes to pool.  This
	* will overwrite to next block if necessary, or allocate from end
	* @param buf byte buffer to write
	* @param numbyte number of bytes to write
	* @return number of bytes written
	* @exception IOException if can't acquire new block
	*/
	public synchronized int writen(BlockAccessIndex lbai, byte[] buf, int numbyte) throws IOException {
		BlockAccessIndex tblk = null, ablk = null;
		int i = 0, runcount = numbyte, blkbytes;
		// see if we need the next block to start
		// and flag our position, tblk has passed block or a new acquired one
		if (lbai.getByteindex() >= DBPhysicalConstants.DATASIZE) {
			if ((tblk=getnextblk(lbai)) == null) { // no room in passed block, no next block, acquire one
				tblk = acquireblk(lbai);
			}
		} else { // we have some room in the passed block
			tblk = lbai;
		}
		// Iterate, reducing the byte count in buffer by room in each block
		for (;;) {
			blkbytes = DBPhysicalConstants.DATASIZE - tblk.getByteindex();
			if (runcount > blkbytes) {  //overflow block
				runcount -= blkbytes;
				System.arraycopy(
					buf,
					i,
					tblk.getBlk().getData(),
					tblk.getByteindex(),
					blkbytes);
				tblk.setByteindex((short) (tblk.getByteindex() + (short)blkbytes));
				i += blkbytes;
				tblk.getBlk().setBytesused(DBPhysicalConstants.DATASIZE);
				//update control info
				tblk.getBlk().setBytesinuse(DBPhysicalConstants.DATASIZE);
				tblk.getBlk().setIncore(true);
				tblk.getBlk().setInlog(false);
				if ((ablk=getnextblk(tblk)) == null) { // no linked block to write into? get one
					ablk = acquireblk(tblk);
				}
				tblk = ablk;
				// now tblk has next block in chain or new acquired block
			} else { // we can fit the remainder of buffer in this block
				System.arraycopy(
					buf,
					i,
					tblk.getBlk().getData(),
					tblk.getByteindex(),
					runcount);
				tblk.setByteindex((short) (tblk.getByteindex() + runcount));
				i += runcount;
				if (tblk.getByteindex() > tblk.getBlk().getBytesused()) {
					//update control info
					tblk.getBlk().setBytesused(tblk.getByteindex());
					tblk.getBlk().setBytesinuse(tblk.getBlk().getBytesused());
				}
				tblk.getBlk().setIncore(true);
				tblk.getBlk().setInlog(false);
				return i;
			}
		}
	}
	/**
	* writen -  write n bytes to pool.  This will overwrite to next block if necessary, or allocate from end
	* The blocks written have their 'inCore' property set to true and their 'inLog' property set to false.
	* The is used in the Seekable DB channel that moves data from store to pool
	* @param buf byte buffer to write
	* @param numbyte number of bytes to write
	* @return number of bytes written
	* @exception IOException if can't acquire new block
	*/
	public synchronized int writen(BlockAccessIndex lbai, ByteBuffer buf, int numbyte) throws IOException {
		BlockAccessIndex tblk = null, ablk = null;
		int i = 0, runcount = numbyte, blkbytes;
		// sets the incore to true and the inlog to false on both blocks
		// see if we need the next block to start
		// and flag our position, tblk has passed block or a new acquired one
		if (lbai.getByteindex() >= DBPhysicalConstants.DATASIZE) {
			if ((tblk=getnextblk(lbai)) == null) { // no room in passed block, no next block, acquire one
				tblk = acquireblk(lbai);
			}
		} else { // we have some room in the passed block
			tblk = lbai;
		}
		//
		for (;;) {
			blkbytes = DBPhysicalConstants.DATASIZE - tblk.getByteindex();
			if (runcount > blkbytes) {
				runcount -= blkbytes;
				buf.position(i);
				buf.get(tblk.getBlk().getData(), tblk.getByteindex(), blkbytes);
				tblk.setByteindex((short) (tblk.getByteindex() + (short)blkbytes));
				i += blkbytes;
				tblk.getBlk().setBytesused(DBPhysicalConstants.DATASIZE);
				//update control info
				tblk.getBlk().setBytesinuse(DBPhysicalConstants.DATASIZE);
				tblk.getBlk().setIncore(true);
				tblk.getBlk().setInlog(false);
				if ((ablk=getnextblk(tblk)) == null) {
					ablk = acquireblk(tblk);
				}
				tblk = ablk;
			} else {
				buf.position(i);
				buf.get(tblk.getBlk().getData(), tblk.getByteindex(), runcount);
				tblk.setByteindex((short) (tblk.getByteindex() + runcount));
				i += runcount;
				if (tblk.getByteindex() >= tblk.getBlk().getBytesused()) {
					//update control info
					tblk.getBlk().setBytesused(tblk.getByteindex());
					tblk.getBlk().setBytesinuse(tblk.getBlk().getBytesused());
				}
				tblk.getBlk().setIncore(true);
				tblk.getBlk().setInlog(false);
				return i;
			}
		}
	}

	/**
	* writei -  write 1 byte to pool.
	* This method designed to be called from DBOutput.
	* Will overwrite to next blk if necessary.
	* @param byte to write
	* @exception IOException If cannot acquire new block
	*/
	public synchronized void writei(BlockAccessIndex lbai, int tbyte) throws IOException {
		BlockAccessIndex tblk;
		// see if we need the next block to start
		// and flag our position
		if (lbai.getByteindex() >= DBPhysicalConstants.DATASIZE) {
			if ((tblk=getnextblk(lbai)) == null) { // no room in passed block, no next block, acquire one
				tblk = acquireblk(lbai);
			}
		} else { // we have some room in the passed block
			tblk = lbai;
		}
		if (!tblk.getBlk().isIncore())
			tblk.getBlk().setIncore(true);
		if (tblk.getBlk().isInlog())
			tblk.getBlk().setInlog(false);
		tblk.getBlk().getData()[tblk.getByteindex()] = (byte) tbyte;
		tblk.setByteindex((short) (tblk.getByteindex() + 1));
		if (tblk.getByteindex() > tblk.getBlk().getBytesused()) {
			//update control info
			tblk.getBlk().setBytesused( tblk.getByteindex()) ;
			tblk.getBlk().setBytesinuse(tblk.getBlk().getBytesused());
		}
	}

	/**
	* deleten -  delete n bytes from object / directory
	* @param osize number bytes to delete
	* @return true if success
	* @exception IOException If we cannot write block
	*/
	public synchronized boolean deleten(BlockAccessIndex lbai, int osize) throws IOException {
		BlockAccessIndex tblk;
		int runcount = osize;
		if (osize <= 0)
			throw new IOException("Attempt to delete object with size invalid: " + osize);

		for (;;) {
			// bytesused is high water mark, bytesinuse is # bytes occupied by data
			// we assume contiguous data
			// this case spans whole block or block to end
			//
			int bspan = (lbai.getBlk().getBytesused() - lbai.getByteindex());
			if (runcount >= bspan) {
				runcount -= bspan;
				lbai.getBlk().setBytesinuse((short) (lbai.getBlk().getBytesinuse() - bspan));
				// delete contiguously to end of block
				// byteindex is start of del entry
				// which is new high water byte count
				// since everything to end of block is going
				lbai.getBlk().setBytesused(lbai.getByteindex());
			} else {
				// we span somewhere in block to not end
				lbai.getBlk().setBytesinuse((short) (lbai.getBlk().getBytesinuse() - runcount));
				runcount = 0;
			}
			// assertion
			if (lbai.getBlk().getBytesinuse() < 0)
				throw new IOException(this.toString() + " negative bytesinuse "+lbai.getBlk().getBytesinuse()+" from runcount "+runcount);
			//
			lbai.getBlk().setIncore(true);
			lbai.getBlk().setInlog(false);
			//
			if (runcount > 0) {
				if((tblk = getnextblk(lbai)) == null)
					throw new IOException(
						"attempted delete past end of chain for "
							+ osize
							+ " bytes in "
							+ lbai);
				lbai = tblk;
			} else
				break;

		}
		return true;
	}
	

	public String toString() {
		return "MappedBlockBuffer tablespace "+tablespace+" blocks:"+this.size()+" free:"+freeBL.size()+" requests:"+requestQueue.size()+" cache hit="+cacheHit+" miss="+cacheMiss;
	}
	
	public synchronized void queueRequest(CompletionLatchInterface ior) {
		try {
			requestQueue.put(ior);
		} catch (InterruptedException e) {
			// executor calls for shutdown during wait for queue to process entries while full or busy 
		}
	}
	
	@Override
	public void run() {
		CompletionLatchInterface ior = null;
		while(shouldRun) {
			try {
				ior = requestQueue.take();
			} catch (InterruptedException e) {
				// executor calling for shutdown
				break;
			}
			try {
				ior.setTablespace(tablespace);
				if( DEBUG ) {
					System.out.println("MappedBlockBuffer.run processing request "+ior+" for tablespace:"+tablespace);
				}
				ior.process();
			} catch (IOException e) {
				System.out.println("MappedBlockBuffer exception processing request "+ior+" "+e);
				break;
			}
		}
		
	}

}
