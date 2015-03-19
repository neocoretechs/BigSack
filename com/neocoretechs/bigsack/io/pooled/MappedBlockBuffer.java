package com.neocoretechs.bigsack.io.pooled;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.neocoretechs.bigsack.ConcurrentArrayList;
import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.IoManagerInterface;
import com.neocoretechs.bigsack.io.MultithreadedIOManager;
import com.neocoretechs.bigsack.io.Optr;
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
 * @author jg
 *
 */
public class MappedBlockBuffer extends ConcurrentHashMap<Long, BlockAccessIndex> implements Runnable {
	private static final long serialVersionUID = -5744666991433173620L;
	private static final boolean DEBUG = false;
	private boolean shouldRun = true;
	private ConcurrentArrayList<BlockAccessIndex> freeBL; // free block list
	private ObjectDBIO globalIO;
	private IoManagerInterface ioManager;
	private int tablespace;
	private int minBufferSize = 10; // minimum number of buffers to reclaim on flush attempt
	private BlockingQueue<CompletionLatchInterface> requestQueue;
	   /** use this to lock for write operations like add/remove */
    private final Lock readLock;
    /** use this to lock for read operations like get/iterator/contains.. */
    private final Lock writeLock;

	private static int POOLBLOCKS;
	private static int QUEUEMAX = 256; // max requests before blocking
	private static int cacheHit = 0; // cache hit rate
	private static int cacheMiss = 0;
	/**
	 * Construct the buffer for this tablespace and link the global IO manager
	 * @param ioManager
	 * @param tablespace
	 * @throws IOException If we try to add an active block the the freechain
	 */
	public MappedBlockBuffer(IoManagerInterface ioManager, int tablespace) throws IOException {
		super(ioManager.getIO().getMAXBLOCKS()/DBPhysicalConstants.DTABLESPACES);
		POOLBLOCKS = ioManager.getIO().getMAXBLOCKS()/DBPhysicalConstants.DTABLESPACES;
		this.globalIO = ioManager.getIO();
		this.ioManager = ioManager;
		this.tablespace = tablespace;
		this.freeBL = new ConcurrentArrayList<BlockAccessIndex>(POOLBLOCKS); // free blocks
		// populate with blocks, they're all free for now
		for (int i = 0; i < POOLBLOCKS; i++) {
			freeBL.add(new BlockAccessIndex(true));
		}
		minBufferSize = POOLBLOCKS/10; // we need at least one
		requestQueue = new ArrayBlockingQueue<CompletionLatchInterface>(QUEUEMAX, true); // true maintains FIFO order
	    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
	    readLock = rwLock.readLock();
	    writeLock = rwLock.writeLock();
	}
	
	public IoManagerInterface getIoManager() { return ioManager; }
	public ObjectDBIO getGlobalIO() { return globalIO;}
	/**
	 * acquireblk - get block from unused chunk or create chunk and get<br>
	 * return acquired block
	 * Add a block to table of blocknums and block access index.
	 * No setting of block in BlockAccessIndex, no initial read reading through ioManager.addBlockAccessnoRead
	 * @param lastGoodBlk The block for us to link to
	 * @return The BlockAccessIndex
	 * @exception IOException if db not open or can't get block
	 */
	public BlockAccessIndex acquireblk(BlockAccessIndex lastGoodBlk) throws IOException {
	    //readLock.lock();
	    //try {
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
		ablk.decrementAccesses();
		// new block number for BlockAccessIndex set in addBlockAccessNoRead
		BlockAccessIndex dblk =  ioManager.addBlockAccessNoRead(new Long(newblock));
		dblk.getBlk().setPrevblk(lastGoodBlk.getBlockNum());
		dblk.getBlk().setNextblk(-1L);
		dblk.getBlk().setBytesused((short) 0);
		dblk.getBlk().setBytesinuse ((short)0);
		dblk.getBlk().setPageLSN(-1L);
		dblk.getBlk().setIncore(true);
		dblk.getBlk().setInlog(false);
		return dblk;
	    //} finally {
	    //  readLock.unlock();
	    //}
	}
	/**
	 * stealblk - get block from unused chunk or create chunk and get.
	 * We dont try to link it to anything because our little linked lists
	 * of instance blocks are not themselves linked to anything<br>
	 * return acquired block. Read through the ioManager calling addBlockAccessNoRead with new block
	 * @param currentBlk Current block so we can deallocate and replace it off chain
	 * @return The BlockAccessIndex of stolen blk
	 * @exception IOException if db not open or can't get block
	 */
	public BlockAccessIndex stealblk(BlockAccessIndex currentBlk) throws IOException {
		//readLock.lock();
		//try {
		long newblock;
		if (currentBlk != null)
			currentBlk.decrementAccesses();
		newblock = GlobalDBIO.makeVblock(tablespace, ioManager.getNextFreeBlock(tablespace));
		// new block number for BlockAccessIndex set in addBlockAccessNoRead
		BlockAccessIndex dblk = ioManager.addBlockAccessNoRead(new Long(newblock));
		dblk.getBlk().setPrevblk(-1L);
		dblk.getBlk().setNextblk(-1L);
		dblk.getBlk().setBytesused((short) 0);
		dblk.getBlk().setBytesinuse((short)0);
		dblk.getBlk().setPageLSN(-1L);
		dblk.getBlk().setIncore(false);
		dblk.getBlk().setInlog(false);
		return dblk;
		//} finally {
	    //  readLock.unlock();
	    //}
	}

	/**
	 * Get an element from free list 0 and remove and return it
	 * @return
	 */
	public BlockAccessIndex take() {
			return freeBL.remove(0);

	}
	
	public void put(BlockAccessIndex bai) { 
			//if( GlobalDBIO.valueOf(bai.getBlockNum()).equals("Tablespace_1_114688"))
			//	System.out.println("PUTTING Tablespace_1_114688");
			freeBL.add(bai); 
	}
	
	public void forceBufferClear() {
		writeLock.lock();
		try {
		Enumeration<BlockAccessIndex> it = elements();
		while(it.hasMoreElements()) {
				BlockAccessIndex bai = (BlockAccessIndex) it.nextElement();
				//if( GlobalDBIO.valueOf(bai.getBlockNum()).equals("Tablespace_1_114688"))
				//	System.out.println("CLEARING Tablespace_1_114688");
				bai.resetBlock();
				put(bai);
		}
		clear();
		} finally {
			writeLock.unlock();
		}
	}
	
	public BlockAccessIndex getUsedBlock(long loc) {
		//readLock.lock();
		//try {
		if( DEBUG )
			System.out.println("MappedBlockBuffer.getusedBlock Calling for USED BLOCK "+GlobalDBIO.valueOf(loc)+" "+loc);
		//if( GlobalDBIO.valueOf(loc).equals("Tablespace_1_114688"))
		//	System.out.println("GETTING USED Tablespace_1_114688 "+get(loc));
		BlockAccessIndex bai = get(loc);
		if( bai == null ) ++cacheMiss;
		else ++cacheHit;
		return bai;
		//} finally {
		//      readLock.unlock();
		//}
	}
	
	/**
	* Get from free list, puts in used list of block access index.
	* Comes here when we can't find blocknum in table in findOrAddBlock.
	* New instance of BlockAccessIndex causes allocation
	* @param Lbn block number to add
	* @exception IOException if new dblock cannot be created
	*/
	private BlockAccessIndex addBlockAccess(Long Lbn) throws IOException {
		//writeLock.lock();
		//try {
		// make sure we have open slots
		if( DEBUG ) {
			System.out.println("MappedBlockBuffer.addBlockAccess "+GlobalDBIO.valueOf(Lbn)+" "+this);
		}
		//if( GlobalDBIO.valueOf(Lbn).equals("Tablespace_1_114688"))
		//	System.out.println("addBlockAccess Tablespace_1_114688");
		checkBufferFlush(Lbn);
		BlockAccessIndex bai = take();
		bai.setBlockNumber(Lbn);
		ioManager.FseekAndRead(Lbn, bai.getBlk());
		//if( GlobalDBIO.valueOf(Lbn).equals("Tablespace_1_114688"))
		//	System.out.println("addBlockAccess Tablespace_1_114688:"+bai+" "+bai.getBlk().blockdump());
		put(Lbn, bai);
		if( DEBUG ) {
				System.out.println("MappedBlockBuffer.addBlockAccess "+GlobalDBIO.valueOf(Lbn)+" returning after freeBL take "+bai+" "+this);
		}
		return bai;
		//} finally {
		//	writeLock.unlock();
		//}
		
	}
	/**
	* Add a block to table of blocknums and block access index.
	* Comes here for acquireBlock. No setting of block in BlockAccessIndex, no initial read
	* @param Lbn block number to add
	* @exception IOException if new dblock cannot be created
	*/
	public BlockAccessIndex addBlockAccessNoRead(Long Lbn) throws IOException {
		//writeLock.lock();
		//try {
		if( DEBUG ) {
			System.out.println("MappedBlockBuffer.addBlockAccessNoRead "+GlobalDBIO.valueOf(Lbn)+" "+this);
		}
		// make sure we have open slots
		checkBufferFlush(Lbn);
		//if( GlobalDBIO.valueOf(Lbn).equals("Tablespace_1_114688"))
		//	System.out.println("putting Tablespace_1_114688");
		BlockAccessIndex bai = take();
		bai.setBlockNumber(Lbn);
		put(Lbn, bai);//put(bai, null);
		if( DEBUG ) {
			System.out.println("MappedBlockBuffer.addBlockAccessNoRead "+GlobalDBIO.valueOf(Lbn)+" returning after freeBL take "+bai+" "+this);
		}
		return bai;
		//} finally {
		//	writeLock.unlock();
		//}
	}
	
	public BlockAccessIndex findOrAddBlockAccess(long bn) throws IOException {
		//readLock.lock();
		//try {
		if( DEBUG ) {
			System.out.println("MappedBlockBuffer.findOrAddBlockAccess "+GlobalDBIO.valueOf(bn)+" "+this);
		}
		//if( GlobalDBIO.valueOf(bn).equals("Tablespace_1_114688"))
		//	System.out.println("findoraddblockaccess Tablespace_1_114688");
		Long Lbn = new Long(bn);
		BlockAccessIndex bai = getUsedBlock(bn);
		if( DEBUG ) {
			System.out.println("MappedBlockBuffer.findOrAddBlockAccess "+GlobalDBIO.valueOf(bn)+" got block "+bai+" "+this);
		}
		if (bai != null) {
			return bai;
		}
		// didn't find it, we must add
		return addBlockAccess(Lbn);
		//} finally {
		//      readLock.unlock();
		//}
	}
	
	/**
	* seek_fwd - long seek forward from current spot
	* @param offset offset from current
	* @exception IOException If we cannot acquire next block
	*/
	public boolean seek_fwd(BlockAccessIndex tbai, long offset) throws IOException {
		//readLock.lock();
		//try {
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
		//} finally {
	    //  readLock.unlock();
		//}
	}
	/**
	* new_node_position<br>
	* determine location of new node, store in new_node_pos.
	* Attempts to cluster entries in used blocks near insertion point
	* @return The Optr pointing to the new node position
	* @exception IOException If we cannot get block for new node
	*/
	public Optr getNewNodePosition(BlockAccessIndex lbai) throws IOException {
		//readLock.lock();
		//try {
		if (lbai.getBlockNum() == -1L) {
			stealblk(lbai);
		} else {
			//BlockAccessIndex tlbai = findOrAddBlock(lbai.getBlockNum());
			//newNodePosBlk = tlbai.getBlockNum();
			// ok, 5 bytes is rather arbitrary but seems a waste to start a big ole object so close to the end of a block
			if (lbai.getBlk().getBytesused()+5 >= DBPhysicalConstants.DATASIZE)
				stealblk(lbai);
		}
		return new Optr(lbai.getBlockNum(), lbai.getBlk().getBytesused());
		//} finally {
	    //  readLock.unlock();
		//}
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
	public void checkBufferFlush(long Lbn) throws IOException {
		writeLock.lock();
		try {
		int latched = 0;
			int bufSize = this.size();
			if( bufSize < POOLBLOCKS )
				return;
			Enumeration<BlockAccessIndex> elbn = this.elements();
			int numGot = 0;
			BlockAccessIndex[] found = new BlockAccessIndex[minBufferSize];// our candidates
			while (elbn.hasMoreElements()) {
				BlockAccessIndex ebaii = (elbn.nextElement());
				//if( GlobalDBIO.valueOf(ebaii.getBlockNum()).equals("Tablespace_1_114688"))
				//	System.out.println("checkBufferFlush Tablespace_1_114688 "+ebaii);
				if( DEBUG )
					System.out.println("MappedBlockBuffer.checkBufferFlush Block buffer "+ebaii);
				if( ebaii.getAccesses() > 1 )
					System.out.println("****COMMIT BUFFER access "+ebaii.getAccesses()+" for buffer "+ebaii);
				assert(!(ebaii.getBlk().isIncore() && ebaii.getBlk().isInlog())) : "****COMMIT BUFFER block in core and log simultaneously! "+ebaii;
				if (Lbn != ebaii.getBlockNum() && ebaii.getAccesses() <= 1) {
					if(ebaii.getBlk().isIncore()) {
						continue; // cant throw out this one
						//ioManager.getUlog(tablespace).writeLog(ebaii); // will set incore, inlog, and push to raw store via applyChange of Loggable
						//throw new IOException("Accesses 0 but incore true " + ebaii);
					}
					// Dont toss block at 0,0. its our BTree root and we will most likely need it soon
					if( ebaii.getBlockNum() == 0L )
						continue;
					found[numGot] = ebaii;
					if( ++numGot == minBufferSize ) {
						break;
					}
				} else {
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
					//if( GlobalDBIO.valueOf(ebaii.getBlockNum()).equals("Tablespace_1_114688"))
					//	System.out.println("checkBufferFlush Tablespace_1_114688 "+ebaii);
					if( DEBUG )
						System.out.println("MappedBlockBuffer.checkBufferFlush PHASE II Block buffer "+ebaii+" "+this);
					if(Lbn != ebaii.getBlockNum() && ebaii.getAccesses() == 1) {
						if(ebaii.getBlk().isIncore() ) continue;/*&& !ebaii.getBlk().isInlog()) {
							if( DEBUG )
								System.out.println("MappedBlockBuffer.checkBufferFlush set to write pool entry to log "+ebaii);
							ioManager.getUlog(tablespace).writeLog(ebaii); // will set incore, inlog, and push to raw store via applyChange of Loggable
							//throw new IOException("Accesses 0 but incore true " + ebaii);
						}
						*/
						// Dont toss block at 0,0. its our BTree root and we will most likely need it soon
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
				if( numGot == 0 )
					throw new IOException("Unable to free up blocks in buffer pool with "+latched+" singley and "+latched2+" MULTIPLY latched.");
			}
			
			for(int i = 0; i < numGot; i++) {
				if( found[i] != null ) {				
					this.remove(found[i].getBlockNum());
					//if( GlobalDBIO.valueOf(found[i].getBlockNum()).equals("Tablespace_1_114688"))
					//	System.out.println("REMOVING via checkBufferFlush Tablespace_1_114688");
					found[i].decrementAccesses();
					found[i].resetBlock();
					freeBL.add(found[i]);
				}
			}
		} finally {
			writeLock.unlock();
		}
	}
	/**
	 * Commit all outstanding blocks in the buffer.
	 * @throws IOException
	 */
	public void commitBufferFlush() throws IOException {
		writeLock.lock();
		try {
		Enumeration<BlockAccessIndex> elbn = this.elements();
		while (elbn.hasMoreElements()) {
				BlockAccessIndex ebaii = (elbn.nextElement());
				if( ebaii.getAccesses() > 1 )
					throw new IOException("****COMMIT BUFFER access "+ebaii.getAccesses()+" for buffer "+ebaii);
				if(ebaii.getBlk().isIncore() && ebaii.getBlk().isInlog())
					throw new IOException("****COMMIT BUFFER block in core and log simultaneously! "+ebaii);
				if (ebaii.getAccesses() < 2) {
					if(ebaii.getBlk().isIncore() && !ebaii.getBlk().isInlog()) {
						ioManager.getUlog(tablespace).writeLog(ebaii); // will set incore, inlog, and push to raw store via applyChange of Loggable
					}
					ebaii.decrementAccesses();
					ebaii.getBlk().resetBlock();
					freeBL.add(ebaii);
					//if( GlobalDBIO.valueOf(ebaii.getBlockNum()).equals("Tablespace_1_114688"))
					//	System.out.println("FREELIST ADD Tablespace_1_114688");
				}
		}
		clear();
		cacheHit = 0;
		cacheMiss = 0;
		} finally {
			writeLock.unlock();
		}
	}
	/**
	 * Commit all outstanding blocks in the buffer, bypassing the log subsystem. Should be used with forethought
	 * @throws IOException
	 */
	public void directBufferWrite() throws IOException {
		writeLock.lock();
		try {
		Enumeration<BlockAccessIndex> elbn = this.elements();
		if(DEBUG) System.out.println("MappedBlockBuffer.direct buffer write");
		while (elbn.hasMoreElements()) {
					BlockAccessIndex ebaii = (elbn.nextElement());
					if (ebaii.getAccesses() == 0 && ebaii.getBlk().isIncore() ) {
						if(DEBUG)
							System.out.println("fully writing "+ebaii.getBlockNum()+" "+ebaii.getBlk());
						ioManager.FseekAndWriteFully(ebaii.getBlockNum(), ebaii.getBlk());
						ioManager.Fforce();
						ebaii.getBlk().setIncore(false);
					}
		}
		} finally {
	      writeLock.unlock();
		}
	}
	

	/**
	* Find or add the block to in-mem list.  First deallocate the currently
	* used block, get the new block, then allocate it
	* @param tbn The virtual block to retrieve
	* 
	* @exception IOException If low-level access fails
	*/
	public BlockAccessIndex findOrAddBlock(long tbn) throws IOException {
		//readLock.lock();
		//try {
		int tblsp = GlobalDBIO.getTablespace(tbn);
		BlockAccessIndex lbai = findOrAddBlockAccess(tbn);
		assert( tblsp == tablespace ) : "Block retrieveal misdelegated to tablespace block buffer for "+tbn+" "+tblsp+" from:"+tablespace;
		if( DEBUG )
			System.out.println("BlockDBIO.findOrAddBlock tablespace "+tblsp+" pos:"+GlobalDBIO.valueOf(tbn)+" current:"+lbai);
		return lbai;
		//} finally {
		//      readLock.unlock();
		//}
	}
	
	/**
	* getnextblk - read the next chained Datablock
	* @return true if success
	* @exception IOException If we cannot read next block
	*/
	public BlockAccessIndex getnextblk(BlockAccessIndex lbai) throws IOException {
		//readLock.lock();
		//try {
		lbai.decrementAccesses();
		if (lbai.getBlk().getNextblk() == -1L) {
			//if( DEBUG )
			//	System.out.println("MappedBlockBuffer.getnextblk returning with no next block "+lbai);
			return null;
		}
		if( DEBUG )
			System.out.println("MappedBlockBuffer.getnextblk next block fetch:"+lbai);
		return findOrAddBlock(lbai.getBlk().getNextblk());
		//} finally {
	    //  readLock.unlock();
		//}
	}
	/**
	* readn - read n bytes from pool
	* @param buf byte buffer to fill
	* @param numbyte number of bytes to read
	* @return number of bytes read
	* @exception IOException If we cannot acquire next block
	*/
	public int readn(BlockAccessIndex lbai, byte[] buf, int numbyte) throws IOException {
		BlockAccessIndex tblk;
		//if( DEBUG )
		//	System.out.println("MappedBlockBuffer.readn "+lbai+" size:"+numbyte);
		int i = 0, runcount = numbyte, blkbytes;
		// see if we need the next block to start
		// and flag our position
		if (lbai.getByteindex() >= lbai.getBlk().getBytesused())
			if((tblk=getnextblk(lbai)) != null) {
				lbai=tblk;
				return (i != 0 ? i : -1);
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
					return (i != 0 ? i : -1);
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
				return (i != 0 ? i : -1);
			}
		}
	}
	/**
	* readn - read n bytes from pool
	* @param buf byte buffer to fill
	* @param numbyte number of bytes to read
	* @return number of bytes read
	* @exception IOException If we cannot acquire next block
	*/
	public int readn(BlockAccessIndex lbai, ByteBuffer buf, int numbyte) throws IOException {
		BlockAccessIndex tblk;
		int i = 0, runcount = numbyte, blkbytes;
		// see if we need the next block to start
		// and flag our position
		if (lbai.getByteindex() >= lbai.getBlk().getBytesused())
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
	public int readi(BlockAccessIndex lbai) throws IOException {
		BlockAccessIndex tblk;
		// see if we need the next block to start
		// and flag our position
		if (lbai.getByteindex() >= lbai.getBlk().getBytesused()) {
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
	public int writen(BlockAccessIndex lbai, byte[] buf, int numbyte) throws IOException {
		BlockAccessIndex tblk;
		int i = 0, runcount = numbyte, blkbytes;
		// see if we need the next block to start
		// and flag our position
		if (lbai.getByteindex() >= DBPhysicalConstants.DATASIZE)
			if ((tblk=getnextblk(lbai)) == null) {
				lbai = acquireblk(lbai);
			} else {
				lbai = tblk;
			}
		//
		for (;;) {
			blkbytes = DBPhysicalConstants.DATASIZE - lbai.getByteindex();
			if (runcount > blkbytes) {
				runcount -= blkbytes;
				System.arraycopy(
					buf,
					i,
					lbai.getBlk().getData(),
					lbai.getByteindex(),
					blkbytes);
				lbai.setByteindex((short) (lbai.getByteindex() + (short)blkbytes));
				i += blkbytes;
				lbai.getBlk().setBytesused(DBPhysicalConstants.DATASIZE);
				//update control info
				lbai.getBlk().setBytesinuse(DBPhysicalConstants.DATASIZE);
				lbai.getBlk().setIncore(true);
				lbai.getBlk().setInlog(false);
				if ((tblk=getnextblk(lbai)) == null) {
					lbai = acquireblk(lbai);
				} else {
					lbai = tblk;
				}
			} else {
				System.arraycopy(
					buf,
					i,
					lbai.getBlk().getData(),
					lbai.getByteindex(),
					runcount);
				lbai.setByteindex((short) (lbai.getByteindex() + runcount));
				i += runcount;
				if (lbai.getByteindex() > lbai.getBlk().getBytesused()) {
					//update control info
					lbai.getBlk().setBytesused(lbai.getByteindex());
					lbai.getBlk().setBytesinuse(lbai.getBlk().getBytesused());
				}
				lbai.getBlk().setIncore(true);
				lbai.getBlk().setInlog(false);
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
	public int writen(BlockAccessIndex lbai, ByteBuffer buf, int numbyte) throws IOException {
		BlockAccessIndex tblk;
		int i = 0, runcount = numbyte, blkbytes;
		// see if we need the next block to start
		// and flag our position
		// sets the incore to true and the inlog to false on both blocks
		if (lbai.getByteindex() >= DBPhysicalConstants.DATASIZE)
			if((tblk=getnextblk(lbai)) == null) {
				lbai = acquireblk(lbai);
			} else {
				lbai = tblk;
			}
		//
		for (;;) {
			blkbytes = DBPhysicalConstants.DATASIZE - lbai.getByteindex();
			if (runcount > blkbytes) {
				runcount -= blkbytes;
				buf.position(i);
				buf.get(lbai.getBlk().getData(), lbai.getByteindex(), blkbytes);
				lbai.setByteindex((short) (lbai.getByteindex() + (short)blkbytes));
				i += blkbytes;
				lbai.getBlk().setBytesused(DBPhysicalConstants.DATASIZE);
				//update control info
				lbai.getBlk().setBytesinuse(DBPhysicalConstants.DATASIZE);
				lbai.getBlk().setIncore(true);
				lbai.getBlk().setInlog(false);
				if ((tblk=getnextblk(lbai)) == null) {
					lbai = acquireblk(lbai);
				} else {
					lbai = tblk;
				}
			} else {
				buf.position(i);
				buf.get(lbai.getBlk().getData(), lbai.getByteindex(), runcount);
				lbai.setByteindex((short) (lbai.getByteindex() + runcount));
				i += runcount;
				if (lbai.getByteindex() >= lbai.getBlk().getBytesused()) {
					//update control info
					lbai.getBlk().setBytesused(lbai.getByteindex());
					lbai.getBlk().setBytesinuse(lbai.getBlk().getBytesused());
				}
				lbai.getBlk().setIncore(true);
				lbai.getBlk().setInlog(false);
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
	public void writei(BlockAccessIndex lbai, int tbyte) throws IOException {
		BlockAccessIndex tblk;
		// see if we need the next block to start
		// and flag our position
		if (lbai.getByteindex() >= DBPhysicalConstants.DATASIZE)
			if((tblk = getnextblk(lbai)) == null) {
				lbai = acquireblk(lbai);
			} else {
				lbai = tblk;
			}
		if (!lbai.getBlk().isIncore())
			lbai.getBlk().setIncore(true);
		if (lbai.getBlk().isInlog())
			lbai.getBlk().setInlog(false);
		lbai.getBlk().getData()[lbai.getByteindex()] = (byte) tbyte;
		lbai.setByteindex((short) (lbai.getByteindex() + 1));
		if (lbai.getByteindex() > lbai.getBlk().getBytesused()) {
			//update control info
			lbai.getBlk().setBytesused( lbai.getByteindex()) ;
			lbai.getBlk().setBytesinuse(lbai.getBlk().getBytesused());
		}
	}

	/**
	* deleten -  delete n bytes from object / directory
	* @param osize number bytes to delete
	* @return true if success
	* @exception IOException If we cannot write block
	*/
	public boolean deleten(BlockAccessIndex lbai, int osize) throws IOException {
		BlockAccessIndex tblk;
		int runcount = osize;
		if (osize <= 0)
			throw new IOException("Attempt to delete object with size invalid: " + osize);
		long nextblk; //running count of object size,next
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
	
	public void queueRequest(CompletionLatchInterface ior) {
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
					System.out.println("MappedBlockBuffer.run processing request "+ior+" "+this);
				}
				ior.process();
			} catch (IOException e) {
				System.out.println("MappedBlockBuffer exception processing request "+ior+" "+e);
				break;
			}
		}
		
	}

}
