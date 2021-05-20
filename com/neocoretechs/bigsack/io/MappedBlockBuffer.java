package com.neocoretechs.bigsack.io;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.BlockChangeEvent;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.pooled.ObjectDBIO;

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
public class MappedBlockBuffer extends ConcurrentHashMap<Long, SoftReference<BlockAccessIndex>> {
	private static final long serialVersionUID = -5744666991433173620L;
	private static final boolean DEBUG = true;
	private static final boolean NEWNODEPOSITIONDEBUG = false;
	private ObjectDBIO globalIO;
	private IoManagerInterface ioManager;
	private int tablespace;
	private final Set<BlockChangeEvent> mObservers = Collections.newSetFromMap(new ConcurrentHashMap<BlockChangeEvent, Boolean>());
  
	private static int cacheHit = 0; // cache hit rate
	private static int cacheMiss = 0;
	
	private static final int CLEAN_UP_PERIOD_IN_SEC = 5;
	/**
	 * Construct the buffer for this tablespace and link the global IO manager
	 * @param ioManager Manager such as MultiThreadedIOManager or ClusterIOManager
	 * @param tablespace
	 * @throws IOException If we try to add an active block the the freechain
	 */
	public MappedBlockBuffer(IoManagerInterface ioManager, int tablespace) throws IOException {
		super(ioManager.getIO().getMAXBLOCKS()/DBPhysicalConstants.DTABLESPACES);
		this.globalIO = ioManager.getIO();
		this.ioManager = ioManager;
		this.tablespace = tablespace;
		Thread cleanerThread = new Thread(() -> {
	            while (!Thread.currentThread().isInterrupted()) {
	                try {
	                    Thread.sleep(CLEAN_UP_PERIOD_IN_SEC * 1000);
	                    entrySet().removeIf(entry -> Optional.ofNullable((SoftReference<BlockAccessIndex>)(entry.getValue())).map(SoftReference::get).map(BlockAccessIndex::isExpired).orElse(false));
	                } catch (InterruptedException e) {
	                    Thread.currentThread().interrupt();
	                }
	            }
	        });
	        cleanerThread.setDaemon(true);
	        cleanerThread.start();
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
	 * THERE IS NO CROSS TABLESPACE BLOCK LINKING, the prev and next of a block always refer to 
	 * blocks relative to the file in the same tablespace. All other addresses are Vblocks, such as PageLSN.
	 * No setting of block in BlockAccessIndex, no initial read reading through ioManager.addBlockAccessnoRead
	 * @param tblk The block for us to link to, relative to beginning of tablespace, NOT a Vblock
	 * @return The BlockAccessIndex
	 * @exception IOException if db not open or can't get block
	 */
	public synchronized SoftReference<BlockAccessIndex> acquireblk(SoftReference<BlockAccessIndex> tblk) throws IOException {
		if( DEBUG )
			System.out.println("MappedBlockBuffer.acquireblk, last good is "+tblk);
		long newblock, newVblock;
		SoftReference<BlockAccessIndex> ablk = tblk;
		// this way puts it all in one tablespace
		//int tbsp = getTablespace(lastGoodBlk.getBlockNum());
		//int tbsp = new Random().nextInt(DBPhysicalConstants.DTABLESPACES);
		// this uses a round robin approach
		newblock = ioManager.getNextFreeBlock(tablespace);
		newVblock = GlobalDBIO.makeVblock(tablespace, newblock);
		// update old block, set it to relative, NOT Vblock
		ablk.get().getBlk().setNextblk(newblock);
		ablk.get().getBlk().setIncore(true);
		ablk.get().getBlk().setInlog(false);
		// new block number for BlockAccessIndex set in addBlockAccessNoRead
		// it expects a Vblock
		SoftReference<BlockAccessIndex> dblk = addBlockAccessNoRead(new Long(newVblock));
		dblk.get().getBlk().resetBlock();
		// Set previous to relative block of last good
		dblk.get().getBlk().setPrevblk(GlobalDBIO.getBlock(tblk.get().getBlockNum()));
		dblk.get().getBlk().setIncore(true);
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
		BlockAccessIndex dblk = addBlockAccessNoRead(new Long(newblock)).get();
		// set prev, next to -1, reset bytesused, byteinuse to 0
		dblk.getBlk().resetBlock();
		if( DEBUG )
			System.out.println("MappedBlockBuffer.stealblk, returning "+dblk);
		return dblk;
	}


	/**
	 * Reset each block in the map and put them to the free block map
	 */
	public synchronized void forceBufferClear() {
		Enumeration<SoftReference<BlockAccessIndex>> it = elements();
		while(it.hasMoreElements()) {
			SoftReference<BlockAccessIndex> bai = (SoftReference<BlockAccessIndex>) it.nextElement();
			bai.get().resetBlock(true); // reset and clear access latch
		}
		clear();
	}
		
	/**
	* seek_fwd - long seek forward from current spot. If we change blocks, notify the observers
	* @param offset offset from current
	* @exception IOException If we cannot acquire next block
	*/
	public synchronized boolean seek_fwd(SoftReference<BlockAccessIndex> tbai, long offset) throws IOException {
		long runcount = offset;
		SoftReference<BlockAccessIndex> lbai = tbai;
		do {
			if (runcount >= (lbai.get().getBlk().getBytesused() - lbai.get().getByteindex())) {
				runcount -= (lbai.get().getBlk().getBytesused() - lbai.get().getByteindex());
				lbai = getnextblk(lbai);
				if(lbai.get().getBlk().getNextblk() == -1)
					return false;
			} else {
				lbai.get().setByteindex((short) (lbai.get().getByteindex() + runcount));
				runcount = 0;
			}
		} while (runcount > 0);
		return true;
	
	}
	/**
	* Determine location of new node, store in new_node_pos.
	* Attempts to cluster entries in used blocks near insertion point relative to other entries.
	* Choose a random tablespace, then find a key that has that tablespace, then cluster there.
	* @param locs The array of previous entries to check for block space
	* @param index The index of the target in array, such that we dont check that
	* @param nkeys The total keys in use to check in array
	* @return The Optr pointing to the new node position
	* @exception IOException If we cannot get block for new node
	*/
	protected static Optr getNewInsertPosition(ObjectDBIO sdbio, Optr[] locs, int index, int nkeys, int bytesNeeded ) throws IOException {
		long blockNum = -1L;
		BlockAccessIndex ablk = null;
		short bytesUsed = 0;
		int tbsp = new Random().nextInt(DBPhysicalConstants.DTABLESPACES);  
		for(int i = 0; i < nkeys; i++) {
			if(i == index) continue;
			if( !locs[i].equals(Optr.emptyPointer) && GlobalDBIO.getTablespace(locs[i].getBlock()) == tbsp ) {
				ablk = sdbio.findOrAddBlock(locs[i].getBlock());
				assert(!ablk.getBlk().isKeypage()) : "Attempt to insert to keyPage "+ablk;
				short bytesAvailable = (short) (DBPhysicalConstants.DATASIZE - ablk.getBlk().getBytesused());
				if( bytesAvailable >= bytesNeeded || bytesAvailable == DBPhysicalConstants.DATASIZE) {
					// eligible
					blockNum = ablk.getBlockNum();
					bytesUsed = ablk.getBlk().getBytesused();
					break;
				}
				ablk.decrementAccesses();
			}
		}
		boolean stolen = false;
		// come up empty?
		if (blockNum == -1L) {
			ablk = sdbio.stealblk();
			blockNum = ablk.getBlockNum();
			bytesUsed = ablk.getBlk().getBytesused();
			stolen = true;
		}
		assert( !ablk.getBlk().isKeypage() ) : "Attempt to obtain new insert position on key page:"+ablk+" "+ablk.getBlk();
		if( NEWNODEPOSITIONDEBUG )
			System.out.println("MappedBlockBuffer.getNewNodePosition "+GlobalDBIO.valueOf(blockNum)+" Used bytes:"+bytesUsed+" stolen:"+stolen+" called by keys(target="+index+" in use="+nkeys+") locs:"+Arrays.toString(locs));
		return new Optr(blockNum, bytesUsed);
	}

	/**
	 * Commit all outstanding blocks in the buffer.
	 * @throws IOException
	 */
	public synchronized void commitBufferFlush(RecoveryLogManager rlm) throws IOException {
		Enumeration<SoftReference<BlockAccessIndex>> elbn = this.elements();
		while (elbn.hasMoreElements()) {
				SoftReference<BlockAccessIndex> ebaii = (elbn.nextElement());
				if( ebaii.get().getAccesses() > 1 )
					throw new IOException("****COMMIT BUFFER access "+ebaii.get().getAccesses()+" for buffer "+ebaii);
				if(ebaii.get().getBlk().isIncore() && ebaii.get().getBlk().isInlog())
					throw new IOException("****COMMIT BUFFER block in core and log simultaneously! "+ebaii);
				if (ebaii.get().getAccesses() < 2) {
					if(ebaii.get().getBlk().isIncore() && !ebaii.get().getBlk().isInlog()) {
						//writeLog(ebaii); 
						// will set incore, inlog, and push to raw store via applyChange of Loggable
						if( DEBUG )
							System.out.println("MappedBlockBuffer.commitBufferFlush of block "+ebaii);
						rlm.writeLog(ebaii.get());
					}
					ebaii.get().decrementAccesses();
					ebaii.get().getBlk().resetBlock();
				}
		}
	}
	/**
	 * Commit all outstanding blocks in the buffer, bypassing the log subsystem. Should be used with forethought
	 * @throws IOException
	 */
	public synchronized void directBufferWrite() throws IOException {
		Enumeration<SoftReference<BlockAccessIndex>> elbn = this.elements();
		if(DEBUG) System.out.println("MappedBlockBuffer.direct buffer write");
		while (elbn.hasMoreElements()) {
					SoftReference<BlockAccessIndex> ebaii = (elbn.nextElement());
					if (ebaii.get().getAccesses() == 0 && ebaii.get().getBlk().isIncore() ) {
						if(DEBUG)
							System.out.println("MappedBlockBuffer.directBufferWrite fully writing "+ebaii.get().getBlockNum()+" "+ebaii.get().getBlk());
						ioManager.FseekAndWriteFully(ebaii.get().getBlockNum(), ebaii.get().getBlk());
						ioManager.Fforce();
						ebaii.get().getBlk().setIncore(false);
					}
		}
	}
	
	/**
	* Find or add the block to in-mem cache. If we dont get a cache hit we go to 
	* deep store to bring it in.
	* @param bn The virtual block to retrieve
	* @return The BlockAccessIndex of the retrieved block, latched
	* @exception IOException If low-level access fails
	*/
	public synchronized SoftReference<BlockAccessIndex> findOrAddBlock(long bn) throws IOException {
		if( DEBUG ) {
			System.out.println("MappedBlockBuffer.findOrAddBlock "+GlobalDBIO.valueOf(bn)+" "+this);
		}
		Long Lbn = new Long(bn);
		SoftReference<BlockAccessIndex> bai = getUsedBlock(bn);
		if( DEBUG ) {
			System.out.println("MappedBlockBuffer.findOrAddBlock "+GlobalDBIO.valueOf(bn)+" got block "+bai+" "+this);
		}
		if (bai != null) {
			if( bai.get().getAccesses() == 0 )
				bai.get().addAccess();
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
	private SoftReference<BlockAccessIndex> addBlockAccess(Long Lbn) throws IOException {
		// make sure we have open slots
		if( DEBUG ) {
			System.out.println("MappedBlockBuffer.addBlockAccess "+GlobalDBIO.valueOf(Lbn)+" "+this);
		}
		SoftReference<BlockAccessIndex> bai = new SoftReference<BlockAccessIndex>(new BlockAccessIndex(true));
		// ups access, set blockindex 0
		bai.get().setBlockNumber(Lbn);
		ioManager.FseekAndRead(Lbn, bai.get().getBlk());
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
	public synchronized SoftReference<BlockAccessIndex> addBlockAccessNoRead(Long Lbn) throws IOException {
		if( DEBUG ) {
			System.out.println("MappedBlockBuffer.addBlockAccessNoRead "+GlobalDBIO.valueOf(Lbn)+" "+this);
		}
		// make sure we have open slots
		SoftReference<BlockAccessIndex> bai = new SoftReference<BlockAccessIndex>(new BlockAccessIndex(true));
		// ups the access latch, set byteindex to 0
		bai.get().setBlockNumber(Lbn);
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
	 * @throws IOException 
	 */
	public synchronized SoftReference<BlockAccessIndex> getUsedBlock(long loc) throws IOException {
		if( DEBUG )
			System.out.println("MappedBlockBuffer.getusedBlock Calling for USED BLOCK "+GlobalDBIO.valueOf(loc)+" "+loc);
		SoftReference<BlockAccessIndex> bai = get(loc);
		if( bai == null ) {
			++cacheMiss;
			bai = addBlockAccess(loc);
		} else {
			++cacheHit;
			bai.get().setByteindex((short) 0);
		}
		return bai;

	}
	
	/**
	* getnextblk - read the next chained Datablock based on the next block value of passed, previous block.
	* We will notify all observers of BlockChangeEvent when that happens.
	* THERE IS NO CROSS TABLESPACE BLOCK CHAINING, so the next block address pointed to is NOT a Vblock.
	* We construct one, then call the findOrAddBlock.
	* @param labi The BlockAccessIndex that contains next block pointer
	* @return The BlockAccessIndex of the next block in the chain, or null if there is no more
	* @exception IOException If a low level read fail occurs
	*/
	public synchronized SoftReference<BlockAccessIndex> getnextblk(SoftReference<BlockAccessIndex> tblk) throws IOException {
		tblk.get().decrementAccesses();
		if (tblk.get().getBlk().getNextblk() == -1L) {
			//if( DEBUG )
			//	System.out.println("MappedBlockBuffer.getnextblk returning with no next block "+lbai);
			return null;
		}
		if( DEBUG )
			System.out.println("MappedBlockBuffer.getnextblk next block fetch:"+tblk);
		SoftReference<BlockAccessIndex> nextBlk = findOrAddBlock(GlobalDBIO.makeVblock(tablespace, tblk.get().getBlk().getNextblk()));
		if( nextBlk != null )
			notifyObservers(nextBlk.get());
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
	public synchronized int readn(SoftReference<BlockAccessIndex> lbai, byte[] buf, int offs, int numbyte) throws IOException {
		SoftReference<BlockAccessIndex> tblk;
		int i = offs, runcount = numbyte, blkbytes;
		// see if we need the next block to start
		// and flag our position
		if (lbai.get().getByteindex() >= lbai.get().getBlk().getBytesused()-1)
			if((tblk=getnextblk(lbai)) != null) {
				lbai=tblk;
			} else {
				return -1;
			}
		for (;;) {
			blkbytes = lbai.get().getBlk().getBytesused() - lbai.get().getByteindex();
			if (runcount > blkbytes) {
				runcount -= blkbytes;
				System.arraycopy(
					lbai.get().getBlk().getData(),
					lbai.get().getByteindex(),
					buf,
					i,
					blkbytes);
				lbai.get().setByteindex((short) (lbai.get().getByteindex() + (short)blkbytes));
				i += blkbytes;
				if ((tblk=getnextblk(lbai)) != null) {
					lbai=tblk;
				} else {
					return (i != 0 ? (i-offs) : -1);
				}
			} else {
				System.arraycopy(
					lbai.get().getBlk().getData(),
					lbai.get().getByteindex(),
					buf,
					i,
					runcount);
				lbai.get().setByteindex((short) (lbai.get().getByteindex() + runcount));
				i += runcount;
				return (i != 0 ? (i-offs) : -1);
			}
		}
	}
	
	public synchronized int readn(SoftReference<BlockAccessIndex> lbai, byte[] buf, int numbyte) throws IOException {
		return readn(lbai, buf, 0, numbyte);
	}
	/**
	* readn - read n bytes from pool
	* @param buf byte buffer to fill
	* @param numbyte number of bytes to read
	* @return number of bytes read
	* @exception IOException If we cannot acquire next block
	*/
	public synchronized int readn(SoftReference<BlockAccessIndex> lbai, ByteBuffer buf, int numbyte) throws IOException {
		SoftReference<BlockAccessIndex> tblk;
		int i = 0, runcount = numbyte, blkbytes;
		// see if we need the next block to start
		// and flag our position
		if (lbai.get().getByteindex() >= lbai.get().getBlk().getBytesused()-1)
			if((tblk=getnextblk(lbai)) != null) {
				lbai=tblk;
			} else {
				return (i != 0 ? i : -1);
			}
		for (;;) {
			blkbytes = lbai.get().getBlk().getBytesused() - lbai.get().getByteindex();
			if (runcount > blkbytes) {
				runcount -= blkbytes;
				buf.position(i);
				buf.put(lbai.get().getBlk().getData(), lbai.get().getByteindex(), blkbytes);
				lbai.get().setByteindex((short) (lbai.get().getByteindex() + (short)blkbytes));
				i += blkbytes;
				if((tblk=getnextblk(lbai)) != null) {
					lbai=tblk;
				} else {
					return (i != 0 ? i : -1);
				}
			} else {
				buf.position(i);
				buf.put(lbai.get().getBlk().getData(), lbai.get().getByteindex(), runcount);
				lbai.get().setByteindex((short) (lbai.get().getByteindex() + runcount));
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
	public synchronized int readi(SoftReference<BlockAccessIndex> lbai) throws IOException {
		SoftReference<BlockAccessIndex> tblk;
		// see if we need the next block to start
		// and flag our position
		if (lbai.get().getByteindex() >= lbai.get().getBlk().getBytesused()-1) {
			if((tblk=getnextblk(lbai)) == null) {
				return -1;
			}
			lbai = tblk;
		}
		int ret = lbai.get().getBlk().getData()[lbai.get().getByteindex()] & 255;
		lbai.get().setByteindex((short) (lbai.get().getByteindex() + 1));
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
	public synchronized int writen(SoftReference<BlockAccessIndex> lbai, byte[] buf, int numbyte) throws IOException {
		SoftReference<BlockAccessIndex> tblk = null;
		SoftReference<BlockAccessIndex> ablk = null;
		int i = 0, runcount = numbyte, blkbytes;
		// see if we need the next block to start
		// and flag our position, tblk has passed block or a new acquired one
		if (lbai.get().getByteindex() >= DBPhysicalConstants.DATASIZE) {
			if ((tblk=getnextblk(lbai)) == null) { // no room in passed block, no next block, acquire one
				tblk = acquireblk(lbai);
			}
		} else { // we have some room in the passed block
			tblk = lbai;
		}
		// Iterate, reducing the byte count in buffer by room in each block
		for (;;) {
			blkbytes = DBPhysicalConstants.DATASIZE - tblk.get().getByteindex();
			if(DEBUG)
				System.out.printf("Writing %d to tblk:%s%n",blkbytes, tblk);
			if (runcount > blkbytes) {  //overflow block
				runcount -= blkbytes;
				System.arraycopy(
					buf,
					i,
					tblk.get().getBlk().getData(),
					tblk.get().getByteindex(),
					blkbytes);
				tblk.get().setByteindex((short) (tblk.get().getByteindex() + (short)blkbytes));
				i += blkbytes;
				tblk.get().getBlk().setBytesused(DBPhysicalConstants.DATASIZE);
				//update control info
				tblk.get().getBlk().setBytesinuse(DBPhysicalConstants.DATASIZE);
				tblk.get().getBlk().setIncore(true);
				tblk.get().getBlk().setInlog(false);
				if((ablk=getnextblk(tblk)) == null) { // no linked block to write into? get one
					ablk = acquireblk(tblk);
				}
				tblk = ablk;
				// now tblk has next block in chain or new acquired block
			} else { // we can fit the remainder of buffer in this block
				System.arraycopy(
					buf,
					i,
					tblk.get().getBlk().getData(),
					tblk.get().getByteindex(),
					runcount);
				tblk.get().setByteindex((short) (tblk.get().getByteindex() + runcount));
				i += runcount;
				if (tblk.get().getByteindex() > tblk.get().getBlk().getBytesused()) {
					//update control info
					tblk.get().getBlk().setBytesused(tblk.get().getByteindex());
					tblk.get().getBlk().setBytesinuse(tblk.get().getBlk().getBytesused());
				}
				tblk.get().getBlk().setIncore(true);
				tblk.get().getBlk().setInlog(false);
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
	public synchronized int writen(SoftReference<BlockAccessIndex> lbai, ByteBuffer buf, int numbyte) throws IOException {
		SoftReference<BlockAccessIndex> tblk = null;
		SoftReference<BlockAccessIndex> ablk = null;
		int i = 0, runcount = numbyte, blkbytes;
		// sets the incore to true and the inlog to false on both blocks
		// see if we need the next block to start
		// and flag our position, tblk has passed block or a new acquired one
		if (lbai.get().getByteindex() >= DBPhysicalConstants.DATASIZE) {
			if ((tblk=getnextblk(lbai)) == null) { // no room in passed block, no next block, acquire one
				tblk = acquireblk(lbai);
			}
		} else { // we have some room in the passed block
			tblk = lbai;
		}
		//
		for (;;) {
			blkbytes = DBPhysicalConstants.DATASIZE - tblk.get().getByteindex();
			if(DEBUG)
				System.out.printf("Writing %d to tblk:%s buffer:%s%n",blkbytes, tblk, buf);
			if (runcount > blkbytes) {
				runcount -= blkbytes;
				buf.position(i);
				buf.get(tblk.get().getBlk().getData(), tblk.get().getByteindex(), blkbytes);
				tblk.get().setByteindex((short) (tblk.get().getByteindex() + (short)blkbytes));
				i += blkbytes;
				tblk.get().getBlk().setBytesused(DBPhysicalConstants.DATASIZE);
				//update control info
				tblk.get().getBlk().setBytesinuse(DBPhysicalConstants.DATASIZE);
				tblk.get().getBlk().setIncore(true);
				tblk.get().getBlk().setInlog(false);
				if ((ablk=getnextblk(tblk)) == null) {
					ablk = acquireblk(tblk);
				}
				tblk = ablk;
			} else {
				buf.position(i);
				buf.get(tblk.get().getBlk().getData(), tblk.get().getByteindex(), runcount);
				tblk.get().setByteindex((short) (tblk.get().getByteindex() + runcount));
				i += runcount;
				if (tblk.get().getByteindex() >= tblk.get().getBlk().getBytesused()) {
					//update control info
					tblk.get().getBlk().setBytesused(tblk.get().getByteindex());
					tblk.get().getBlk().setBytesinuse(tblk.get().getBlk().getBytesused());
				}
				tblk.get().getBlk().setIncore(true);
				tblk.get().getBlk().setInlog(false);
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
	public synchronized void writei(SoftReference<BlockAccessIndex> lbai, int tbyte) throws IOException {
		SoftReference<BlockAccessIndex> tblk;
		// see if we need the next block to start
		// and flag our position
		if (lbai.get().getByteindex() >= DBPhysicalConstants.DATASIZE) {
			if ((tblk=getnextblk(lbai)) == null) { // no room in passed block, no next block, acquire one
				tblk = acquireblk(lbai);
			}
		} else { // we have some room in the passed block
			tblk = lbai;
		}
		if (!tblk.get().getBlk().isIncore())
			tblk.get().getBlk().setIncore(true);
		if (tblk.get().getBlk().isInlog())
			tblk.get().getBlk().setInlog(false);
		tblk.get().getBlk().getData()[tblk.get().getByteindex()] = (byte) tbyte;
		tblk.get().setByteindex((short) (tblk.get().getByteindex() + 1));
		if (tblk.get().getByteindex() > tblk.get().getBlk().getBytesused()) {
			//update control info
			tblk.get().getBlk().setBytesused( tblk.get().getByteindex()) ;
			tblk.get().getBlk().setBytesinuse(tblk.get().getBlk().getBytesused());
		}
	}

	/**
	* deleten -  delete n bytes from object / directory. Item may span a block so we
	* adjust the pointers across block boundaries if necessary. Numerous sanity checks along the way.
	* @param osize number bytes to delete
	* @return true if success
	* @exception IOException If we cannot write block, or we attempted to seek past the end of a chain, or if the high water mark and total bytes used did not ultimately agree.
	*/
	protected synchronized void deleten(SoftReference<BlockAccessIndex> lbai, int osize) throws IOException {
		//System.out.println("MappedBlockBuffer.deleten:"+lbai+" size:"+osize);
		SoftReference<BlockAccessIndex> tblk;
		int runcount = osize;
		if (osize <= 0)
			throw new IOException("Attempt to delete object with size invalid: " + osize);
		//
		// Handle the case where the entry we want to delete can be contained within one block
		// we are not altering the high water mark because the entry falls between the beginning and high water
		// and there may be another entry between it and high water
		//
		if( (((int)lbai.get().getByteindex()) + runcount) < ((int)lbai.get().getBlk().getBytesused())) {
			lbai.get().getBlk().setBytesinuse((short) (((int)(lbai.get().getBlk().getBytesinuse()) - runcount)) ); // reduce total bytes being used by delete amount
			// assertion did everything make sense at the end?
			if(lbai.get().getBlk().getBytesinuse() < 0)
				throw new IOException(this.toString() + " "+lbai+" negative bytesinuse from runcount:"+runcount+" delete size:"+osize);
			lbai.get().getBlk().setIncore(true);
			lbai.get().getBlk().setInlog(false);
			return;
		}
		//
		// The following case works for all contiguous chunks, however,it DOES NOT work for non contiguous chunks
		// where an entry is between the one to be deleted and high water mark. as in byteindex = 64 and osiz = 32
		// bytesused = 128 high water and bytesinuse = 64, 2 32 byte entries. in that case bspan = 96, dspan = 64
		// bytesused comes out 64 and bytesinuse comes out 0, and the entry after the one we want gone disappears.
		// that case should have been handled above.
		//
		do {
			//
			int bspan = ((int)lbai.get().getByteindex()) + runcount; // current delete amount plus start of delete
			int dspan = ((int)lbai.get().getBlk().getBytesused()) - ((int)lbai.get().getByteindex()); // (high water mark bytesused - index) total available to delete this page
			if( bspan < dspan ) { // If the total we want to delete plus start, does not exceed total this page, set to delete remaining runcount
				dspan = runcount;
			} else {
				// reduce bytesused by total this page, set high water mark back since we exceeded it
				lbai.get().getBlk().setBytesused( (short) (((int)(lbai.get().getBlk().getBytesused()) - dspan)) );
			}
			//System.out.println("runcount="+runcount+" dspan="+dspan+" bspan="+bspan);
			runcount = runcount - dspan; //reduce runcount by total available to delete this page
			lbai.get().getBlk().setBytesinuse((short) (((int)(lbai.get().getBlk().getBytesinuse()) - dspan)) ); // reduce total bytes being used by delete amount
			//
			// assertion did everything make sense at the end?
			if(lbai.get().getBlk().getBytesinuse() < 0)
				throw new IOException(this.toString() + " "+lbai+" negative bytesinuse from runcount:"+runcount+" delete size:"+osize);
			if(lbai.get().getBlk().getBytesused() < 0)
				throw new IOException(this.toString() + " "+lbai+" negative bytesused from runcount "+runcount+" delete size:"+osize);
			// high water mark 0, but bytes used for data is not, something went horribly wrong
			if(lbai.get().getBlk().getBytesinuse() == 0) { //if total bytes used is 0, reset high water mark to 0 to eventually reclaim block
				lbai.get().getBlk().setBytesused((short)0);
				lbai.get().setByteindex((short)0);	
			}
			//
			lbai.get().getBlk().setIncore(true);
			lbai.get().getBlk().setInlog(false);
			if(runcount > 0) { // if we have more to delete
				tblk = getnextblk(lbai);
				// another sanity check
				if(tblk == null)
					throw new IOException(
						"Attempted delete past end of block chain for "+ osize + " bytes total, with remaining runcount "+runcount+" in "+ lbai);
				// we have to unlink this from the next block
				lbai.get().getBlk().setNextblk(-1L);
				lbai = tblk;
				lbai.get().setByteindex((short) 0);// start at the beginning of the next block to continue delete, or whatever
			}
		} while( runcount > 0); // while we still have more to delete
	}
	

	public String toString() {
		return "MappedBlockBuffer tablespace "+tablespace+" blocks:"+this.size()+" cache hit="+cacheHit+" miss="+cacheMiss;
	}
	
	
	


}
