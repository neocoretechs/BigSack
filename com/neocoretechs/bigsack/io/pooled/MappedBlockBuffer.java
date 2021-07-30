package com.neocoretechs.bigsack.io.pooled;

import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Optional;
import java.util.Set;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.RecoveryLogManager;

/**
 * The MappedBlockBuffer is the buffer pool for each tablespace of each db.<p/>
 * The class functions as the used block list for BlockAccessIndex elements that represent our
 * in memory pool of disk blocks of size MAXBLOCKS. Its construction involves keeping track of the list of
 * free blocks as well to move items between the two and expiring soft reference blocks in the active list. <p/>
 * THERE IS NO CROSS TABLESPACE BLOCK LINKING. The tablespace specific classes deal with physical blocks and
 * the {@link BlockAccessIndex} always has a logical block number, so conversion for logical to physical must be noted.<p/>
 * There are one of these, per tablespace, per database, and act on a particular tablespace.
 * <p/>
 * This class knows about the IOWorker for the tablespace. It has no knowledge of the blockstreams and recovery manager etc.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2015,2021
 *
 */
public class MappedBlockBuffer extends AbstractMap {
	private static final long serialVersionUID = -5744666991433173620L;
	private static final boolean DEBUG = false;
	private GlobalDBIO globalIO;
	private IoInterface ioWorker;
	private int tablespace;
	private final Set<BlockChangeEvent> mObservers = Collections.newSetFromMap(new ConcurrentHashMap<BlockChangeEvent, Boolean>());
	private LinkedHashMap<Long, BlockAccessIndex> freeBlockList = new LinkedHashMap<Long, BlockAccessIndex>(DBPhysicalConstants.DBUCKETS);
	/** The internal HashMap that will hold the SoftReference. */
	private final ConcurrentHashMap usedBlockList = new ConcurrentHashMap();
	/** The number of "hard" references to hold internally. */
	private final int HARD_SIZE = 100;
	/** The FIFO list of hard references, order of last access. */
	private final LinkedList hardCache = new LinkedList();
	/** Reference queue for cleared SoftReference objects. */
	private final ReferenceQueue queue = new ReferenceQueue();

	private static int cacheHit = 0; // cache hit rate
	private static int cacheMiss = 0;
	
	private static final int CLEAN_UP_PERIOD_IN_SEC = 5;
	/**
	 * Construct the buffer for this tablespace and link the global IO manager
	 * @param ioManager Manager such as MultiThreadedIOManager
	 * @param tablespace
	 * @throws IOException If we try to add an active block the the freechain
	 */
	public MappedBlockBuffer(IoInterface ioWorker, int tablespace) throws IOException {
		this.globalIO = ((IOWorker)ioWorker).getGlobalDBIO();
		this.ioWorker = ioWorker;
		this.tablespace = tablespace;
		((IOWorker)ioWorker).setFreeBlockList(freeBlockList);
		Thread cleanerThread = new Thread(() -> {
	            while (!Thread.currentThread().isInterrupted()) {
	                try {
	                    Thread.sleep(CLEAN_UP_PERIOD_IN_SEC * 1000);
	                    //List<Entry<Long,SoftReference<BlockAccessIndex>>> expiredList=entrySet().stream()
	                    	 //.filter(pair -> pair.getValue().get().isExpired())
	                    	 //--.map(Map.Entry::getKey)
	                    	 //.collect(Collectors.toList());
	                    //List<Long> expiredKeys = expiredList.stream().map(Map.Entry::getKey).collect(Collectors.toList());
	                    //this.keySet().removeAll(expiredKeys);
	                    //entrySet().removeIf(entry -> Optional.ofNullable((SoftReference<BlockAccessIndex>)(entry.getValue())).map(SoftReference::get).map(BlockAccessIndex::isExpired).orElse(false));
	                    usedBlockList.entrySet().removeIf(entry -> !(Optional.ofNullable(((Entry<Long, SoftReference>) entry).getValue()).isPresent()));
	                } catch (InterruptedException e) {
	                    Thread.currentThread().interrupt();
	                }
	            }
	        });
	        cleanerThread.setDaemon(true);
	        cleanerThread.start();
	}
	
	@Override
	public Object get(Object key) {
		Object result = null;
	    // We get the SoftReference represented by that key
	    SoftReference soft_ref = (SoftReference) usedBlockList.get(key);
	    if (soft_ref != null) {
	      // From the SoftReference we get the value, which can be
	      // null if it was not in the map, or it was removed in
	      // the processQueue() method defined below
	      result = soft_ref.get();
	      if (result == null) {
	        // If the value has been garbage collected, remove the
	        // entry from the HashMap.
	        usedBlockList.remove(key);
	        soft_ref = null;
	      } else {
	        // We now add this object to the beginning of the hard
	        // reference queue.  One reference can occur more than
	        // once, because lookups of the FIFO queue are slow, so
	        // we don't want to search through it each time to remove
	        // duplicates.
	        hardCache.addFirst(result);
	        if (hardCache.size() > HARD_SIZE) {
	          // Remove the last entry if list longer than HARD_SIZE
	          hardCache.removeLast();
	        }
	      }
	    }
	    return soft_ref;
	  }
	
	/** 
	 * Define subclass of SoftReference which contains
	 * not only the value but also the key to make it easier to find
	 * the entry in the HashMap after it's been garbage collected.
	*/
	private static class SoftValue extends SoftReference {
	    private final Object key; // always make data member final
	    private SoftValue(Object k, Object key, ReferenceQueue q) {
	      super(k, q);
	      this.key = key;
	    }
	}

	/**
	* Process the ReferenceQueue and remove garbage
	* collected SoftValue objects from the HashMap by looking them
	* up using the SoftValue.key data member. 
	*/
	private void processQueue() {
	    SoftValue sv;
	    while ((sv = (SoftValue)queue.poll()) != null) {
	      BlockAccessIndex bai = (BlockAccessIndex) sv.get();
	      if(bai.getAccesses() > 0) {
	    	  // re-enqueue
	    	  System.out.println("SoftReference process queue, re-enqueue:"+sv.key);
	    	  put(sv.key, bai);
	      } else
	    	  usedBlockList.remove(sv.key);
	    }
	}

	@Override
	public SoftReference remove(Object key) {
	  processQueue(); // throw out garbage collected values first
	  return (SoftReference) usedBlockList.remove(key);
	}
	  
	@Override
	public void clear() {
	  hardCache.clear();
	  processQueue(); // throw out garbage collected values
	  usedBlockList.clear();
	}
	  
	@Override
	public int size() {
		processQueue(); // throw out garbage collected values first
		return usedBlockList.size();
	}
	  
	@Override
	public Set<Entry<Long, SoftReference>> entrySet() {
		processQueue();
		return usedBlockList.entrySet();
	}
	
	public void addBlockChangeObserver(BlockChangeEvent bce) { mObservers.add(bce); }
	
	@Override
	/**
	 * Put the key and value to cache. Start the expiration counter beforehand.
	 */
	public Object put(Object key, Object value) {
		if(tablespace != GlobalDBIO.getTablespace(((BlockAccessIndex)value).getBlockNum()) || (Long)key != GlobalDBIO.getBlock(((BlockAccessIndex)value).getBlockNum()))
			throw new RuntimeException("Key:"+key+" does not match corresponding block:"+(BlockAccessIndex)value);
		 // Here we put the key, value pair into the HashMap using a SoftValue object.
		 processQueue(); // throw out garbage collected values first
		 ((BlockAccessIndex)value).startExpired();
		 return usedBlockList.put((Long) key, new SoftValue(value, key, queue));
	}
	/**
	 * Put the extracted key and value to cache. notify block change event observers. Start the expiration counter beforehand.
	 */
	public Object put(BlockAccessIndex value) {
		Long key = new Long(GlobalDBIO.getBlock(value.getBlockNum()));
		processQueue(); // throw out garbage collected values first
		value.startExpired();
		return usedBlockList.put(key, new SoftValue(value, key, queue));
	}
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
    
	public synchronized GlobalDBIO getGlobalIO() { return globalIO;}
	/**
	 * acquireblk - get block from unused chunk or create a new blockchain. this method links a previous block<br>
	 * return acquired block<p/>
	 * Add a block to table of blocknums and block access index.<p/>
	 * THERE IS NO CROSS TABLESPACE BLOCK LINKING. The tablespace specific classes deal with physical blocks and
	 * the {@link BlockAccessIndex} always has a logical block number<p/> The prev and next of a block always refer to 
	 * blocks relative to the file in the same tablespace. All other addresses are Vblocks, such as PageLSN.
	 * No setting of block in BlockAccessIndex, no initial read reading through ioManager.addBlockAccessnoRead
	 * @param previousBlk The block for us to link to, relative to beginning of tablespace, NOT a Vblock
	 * @return The BlockAccessIndex
	 * @exception IOException if db not open or can't get block
	 */
	public synchronized BlockAccessIndex acquireNewBlk(BlockAccessIndex previousBlk) throws IOException {
		if( DEBUG )
			System.out.println("MappedBlockBuffer.acquireblk, last good is tablespace:"+tablespace+" block:"+previousBlk);
		BlockAccessIndex ablk = previousBlk;
		// this way puts it all in one tablespace
		//int tbsp = getTablespace(lastGoodBlk.getBlockNum());
		//int tbsp = new Random().nextInt(DBPhysicalConstants.DTABLESPACES);
		// this uses a round robin approach
		long newblock = ((IOWorker)ioWorker).getNextFreeBlock();
		// update old block, set it to relative, NOT Vblock
		ablk.getBlk().setNextblk(GlobalDBIO.getBlock(newblock));
		ablk.getBlk().setIncore(true);
		ablk.getBlk().setInlog(false);
		BlockAccessIndex newBai = addBlockAccessNoRead(newblock);
		newBai.resetBlock(true);
		// Set previous to relative block of last good
		newBai.getBlk().setPrevblk(GlobalDBIO.getBlock(previousBlk.getBlockNum()));
		newBai.getBlk().setIncore(true);
		if( DEBUG )
			System.out.println("MappedBlockBuffer.acquireblk returning from:"+ablk+" to:"+newBai);
		return newBai;
	}

	
	/**
	 * Reset each block in the map and put them to the free block map
	 */
	public synchronized void forceBufferClear() {
		Enumeration<SoftReference<BlockAccessIndex>> it = usedBlockList.elements();
		while(it.hasMoreElements()) {
			SoftReference<BlockAccessIndex> bai = (SoftReference<BlockAccessIndex>) it.nextElement();
			bai.get().resetBlock(true); // reset and clear access latch
		}
		clear();
	}

	/**
	 * Search for the numbered block {@link BlockAccessIndex}
	 * If it exists on the free list put it to the cache then return the block.<p/>
	 * Attempt to bring it in from deep store if read is true, regardless, put it on the active list if it wasnt there already.
	 * @param lbn the block number to retrieve
	 * @param read to read the block or just create the entry
	 * @return the BlockAccessIndex in list or brought in from deep store
	 * @throws IOException
	 */
	private synchronized BlockAccessIndex getBlock(Long lbn, boolean read) throws IOException {
		// If we requested a specific block, see if it exists on the free list first
		BlockAccessIndex bai = freeBlockList.remove(lbn);
		if(bai != null) {
			put(bai);
			return bai;
		}
		SoftReference sbai = (SoftReference) get(lbn);
		if( sbai == null ) {
			bai = new BlockAccessIndex(globalIO, true);
			bai.setBlockNumber(GlobalDBIO.makeVblock(tablespace, lbn));
			if(read)
				ioWorker.FseekAndRead(lbn, bai.getBlk());
			put(bai);
		} else {
			bai = (BlockAccessIndex) sbai.get();
		}
		bai.startExpired();
		return bai;
	}
	
	public LinkedHashMap<Long, BlockAccessIndex> getFreeBlockList() {
		return freeBlockList;
	}
	
	public synchronized void putFreeBlock(BlockAccessIndex bai) {
		freeBlockList.put(GlobalDBIO.getBlock(bai.getBlockNum()), bai);
	}
	
	public synchronized int sizeFreeBlockList() {
		return freeBlockList.size();
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
	 * Commit all outstanding blocks in the buffer. Iterate the elements in 'this' and write to the undo log
	 * those with < 2 accesses whose datablock 'isIncore' and not 'isInLog'. After writing, decrement accesses to 0
	 * and set the byteindex to 0. The calling of {@link RecoveryLogManager} writeLog will reset incode and set inlog and
	 * call the applyChange method of {@link Loggable} implementation. 
	 * @throws IOException
	 */
	public synchronized void commitBufferFlush(RecoveryLogManager rlm) throws IOException {
		for(Object ebaii : usedBlockList.values()) {
				BlockAccessIndex bai = (BlockAccessIndex) ((SoftReference)ebaii).get();
				if( bai.getAccesses() > 1 )
					throw new IOException("****COMMIT BUFFER access "+bai.getAccesses()+" for buffer "+bai);
				if(bai.getBlk().isIncore() && bai.getBlk().isInlog())
					throw new IOException("****COMMIT BUFFER block in core and log simultaneously! "+bai);
				if(DEBUG)
					System.out.printf("%s.commitBufferFlush prospective block:%s%n", this.getClass().getName(),bai);
				if (bai.getAccesses() < 2) {
					if(bai.getBlk().isIncore() && !bai.getBlk().isInlog()) {
						// will set incore, inlog, and push to raw store via applyChange of Loggable
						if( DEBUG )
							System.out.printf("%s.commitBufferFlush of block:%s%n",this.getClass().getName(),bai);
						rlm.writeLog(bai);
					}
					bai.decrementAccesses();
					bai.setByteindex((short) 0);
				}
		}
	}
	/**
	 * Commit all outstanding blocks in the buffer, bypassing the log subsystem. Should be used with forethought
	 * @throws IOException
	 */
	public synchronized void directBufferWrite() throws IOException {
		Enumeration<SoftReference<BlockAccessIndex>> elbn = usedBlockList.elements();
		if(DEBUG) System.out.println("MappedBlockBuffer.direct buffer write");
		while (elbn.hasMoreElements()) {
					SoftReference<BlockAccessIndex> ebaii = (elbn.nextElement());
					if (ebaii.get().getAccesses() == 0 && ebaii.get().getBlk().isIncore() ) {
						if(DEBUG)
							System.out.println("MappedBlockBuffer.directBufferWrite fully writing "+GlobalDBIO.getBlock(ebaii.get().getBlockNum())+" "+ebaii.get().getBlk());
						ioWorker.FseekAndWriteFully(GlobalDBIO.getBlock(ebaii.get().getBlockNum()), ebaii.get().getBlk());
						ioWorker.Fforce();
						ebaii.get().getBlk().setIncore(false);
					}
		}
	}
	
	/**
	* Get block from free list, puts in used list of block access index, seek and read the contents of the block.
	* Comes here when we can't find blocknum in table in findOrAddBlock.
	* New instance of BlockAccessIndex causes allocation
	* @param Lbn block number to add
	* @exception IOException if new dblock cannot be created
	*/
	private BlockAccessIndex addBlockAccess(Long Lbn) throws IOException {
		// make sure we have open slots
		if( DEBUG ) {
			System.out.println("MappedBlockBuffer.addBlockAccess "+Lbn+" "+this);
		}
		BlockAccessIndex bai = getBlock(Lbn, true);
		if( bai.getAccesses() == 0 )
			bai.addAccess();
		bai.setByteindex((short) 0);
		if( DEBUG ) {
				System.out.println("MappedBlockBuffer.addBlockAccess "+Lbn+" returning after freeBL take "+bai+" "+this);
		}
		return bai;
	}
	/**
	 * If we have to steal a block and bring it in from the freechain, put it to the used list here and up the access
	 * @param bai
	 * @return
	 * @throws IOException
	 */
	public BlockAccessIndex addBlockAccess(BlockAccessIndex bai) throws IOException {
		if( DEBUG ) {
			System.out.printf("%s.addBlockAccess %s%n",this.getClass().getName(), bai);
		}	
		put(bai);
		if( bai.getAccesses() == 0 )
			bai.addAccess();
		bai.setByteindex((short) 0);
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
		BlockAccessIndex bai = getBlock(Lbn, false);
		// up the access latch, set byteindex to 0
		if( bai.getAccesses() == 0 )
			bai.addAccess();
		bai.setByteindex((short) 0);
		put(Lbn, bai);
		if( DEBUG ) {
			System.out.println("MappedBlockBuffer.addBlockAccessNoRead "+Lbn+" returning after freeBL take "+bai+" "+this);
		}
		return bai;
	}
	
	
	/**
	* Find or add the block to in-mem cache. If we dont get a cache hit we go to 
	* deep store to bring it in.
	* @param loc The block number of a presumed pool resident block
	* @return the BlockAccessIndex with the block or null if its not in the cache
	* @throws IOException 
	*/
	public synchronized BlockAccessIndex findOrAddBlock(Long loc) throws IOException {
		if( DEBUG )
			System.out.println("MappedBlockBuffer.findOrAddBlock Calling for BLOCK "+loc+" in tablespace "+tablespace);
		SoftReference sbai = (SoftReference) get(loc);
		BlockAccessIndex bai;
		if( sbai == null ) {
			++cacheMiss;
			bai = addBlockAccess(loc);
		} else {
			++cacheHit;
			bai = (BlockAccessIndex)sbai.get();
			bai.setByteindex((short) 0);
			if( bai.getAccesses() == 0 )
				bai.addAccess();
		}
		notifyObservers(bai);
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
	public synchronized BlockAccessIndex getnextblk(BlockAccessIndex tblk) throws IOException {
		tblk.decrementAccesses();
		if (tblk.getBlk().getNextblk() == -1L) {
			//if( DEBUG )
			//	System.out.println("MappedBlockBuffer.getnextblk returning with no next block "+lbai);
			return null;
		}
		if( DEBUG )
			System.out.println("MappedBlockBuffer.getnextblk next block fetch:"+tblk);
		BlockAccessIndex nextBlk = findOrAddBlock(tblk.getBlk().getNextblk());
		if( DEBUG )
			System.out.println("MappedBlockBuffer.getnextblk next block fetch retrieved:"+(nextBlk == null ? "<null>" : nextBlk));
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
		BlockAccessIndex tblk = null;
		BlockAccessIndex ablk = null;
		int i = 0, runcount = numbyte, blkbytes;
		// see if we need the next block to start
		// and flag our position, tblk has passed block or a new acquired one
		if (lbai.getByteindex() >= DBPhysicalConstants.DATASIZE) {
			if ((tblk=getnextblk(lbai)) == null) { // no room in passed block, no next block, acquire one
				tblk = acquireNewBlk(lbai);
			}
		} else { // we have some room in the passed block
			tblk = lbai;
		}
		// Iterate, reducing the byte count in buffer by room in each block
		for (;;) {
			blkbytes = DBPhysicalConstants.DATASIZE - tblk.getByteindex();
			if(DEBUG)
				System.out.printf("Writing %d to tblk:%s%n",blkbytes, tblk);
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
				if((ablk=getnextblk(tblk)) == null) { // no linked block to write into? get one
					ablk = acquireNewBlk(tblk);
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
		BlockAccessIndex tblk = null;
		BlockAccessIndex ablk = null;
		int i = 0, runcount = numbyte, blkbytes;
		// sets the incore to true and the inlog to false on both blocks
		// see if we need the next block to start
		// and flag our position, tblk has passed block or a new acquired one
		if (lbai.getByteindex() >= DBPhysicalConstants.DATASIZE) {
			if ((tblk=getnextblk(lbai)) == null) { // no room in passed block, no next block, acquire one
				tblk = acquireNewBlk(lbai);
			}
		} else { // we have some room in the passed block
			tblk = lbai;
		}
		//
		for (;;) {
			blkbytes = DBPhysicalConstants.DATASIZE - tblk.getByteindex();
			if(DEBUG)
				System.out.printf("Writing %d to tblk:%s buffer:%s%n",blkbytes, tblk, buf);
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
					ablk = acquireNewBlk(tblk);
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
				tblk = acquireNewBlk(lbai);
			}
		} else { // we have some room in the passed block
			tblk = lbai;
		}
		if (!tblk.getBlk().isIncore())
			tblk.getBlk().setIncore(true);
		tblk.getBlk().getData()[tblk.getByteindex()] = (byte) tbyte;
		tblk.setByteindex((short) (tblk.getByteindex() + 1));
		if (tblk.getByteindex() > tblk.getBlk().getBytesused()) {
			//update control info
			tblk.getBlk().setBytesused( tblk.getByteindex()) ;
			tblk.getBlk().setBytesinuse(tblk.getBlk().getBytesused());
		}
	}

	/**
	* deleten -  delete n bytes from object / directory. Item may span a block so we
	* adjust the pointers across block boundaries if necessary. Numerous sanity checks along the way.
	* @param osize number bytes to delete
	* @return true if success
	* @exception IOException If we cannot write block, or we attempted to seek past the end of a chain, or if the high water mark and total bytes used did not ultimately agree.
	*/
	protected synchronized void deleten(BlockAccessIndex lbai, int osize) throws IOException {
		//System.out.println("MappedBlockBuffer.deleten:"+lbai+" size:"+osize);
		BlockAccessIndex tblk;
		int runcount = osize;
		if (osize <= 0)
			throw new IOException("Attempt to delete object with size invalid: " + osize);
		//
		// Handle the case where the entry we want to delete can be contained within one block
		// we are not altering the high water mark because the entry falls between the beginning and high water
		// and there may be another entry between it and high water
		//
		if( (((int)lbai.getByteindex()) + runcount) < ((int)lbai.getBlk().getBytesused())) {
			lbai.getBlk().setBytesinuse((short) (((int)(lbai.getBlk().getBytesinuse()) - runcount)) ); // reduce total bytes being used by delete amount
			// assertion did everything make sense at the end?
			if(lbai.getBlk().getBytesinuse() < 0)
				throw new IOException(this.toString() + " "+lbai+" negative bytesinuse from runcount:"+runcount+" delete size:"+osize);
			lbai.getBlk().setIncore(true);
			lbai.getBlk().setInlog(false);
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
			int bspan = ((int)lbai.getByteindex()) + runcount; // current delete amount plus start of delete
			int dspan = ((int)lbai.getBlk().getBytesused()) - ((int)lbai.getByteindex()); // (high water mark bytesused - index) total available to delete this page
			if( bspan < dspan ) { // If the total we want to delete plus start, does not exceed total this page, set to delete remaining runcount
				dspan = runcount;
			} else {
				// reduce bytesused by total this page, set high water mark back since we exceeded it
				lbai.getBlk().setBytesused( (short) (((int)(lbai.getBlk().getBytesused()) - dspan)) );
			}
			//System.out.println("runcount="+runcount+" dspan="+dspan+" bspan="+bspan);
			runcount = runcount - dspan; //reduce runcount by total available to delete this page
			lbai.getBlk().setBytesinuse((short) (((int)(lbai.getBlk().getBytesinuse()) - dspan)) ); // reduce total bytes being used by delete amount
			//
			// assertion did everything make sense at the end?
			if(lbai.getBlk().getBytesinuse() < 0)
				throw new IOException(this.toString() + " "+lbai+" negative bytesinuse from runcount:"+runcount+" delete size:"+osize);
			if(lbai.getBlk().getBytesused() < 0)
				throw new IOException(this.toString() + " "+lbai+" negative bytesused from runcount "+runcount+" delete size:"+osize);
			// high water mark 0, but bytes used for data is not, something went horribly wrong
			if(lbai.getBlk().getBytesinuse() == 0) { //if total bytes used is 0, reset high water mark to 0 to eventually reclaim block
				lbai.getBlk().setBytesused((short)0);
				lbai.setByteindex((short)0);	
			}
			//
			lbai.getBlk().setIncore(true);
			lbai.getBlk().setInlog(false);
			if(runcount > 0) { // if we have more to delete
				tblk = getnextblk(lbai);
				// another sanity check
				if(tblk == null)
					throw new IOException(
						"Attempted delete past end of block chain for "+ osize + " bytes total, with remaining runcount "+runcount+" in "+ lbai);
				// we have to unlink this from the next block
				lbai.getBlk().setNextblk(-1L);
				lbai = tblk;
				lbai.setByteindex((short) 0);// start at the beginning of the next block to continue delete, or whatever
			}
		} while( runcount > 0); // while we still have more to delete
	}
	
	@Override
	public String toString() {
		return "MappedBlockBuffer tablespace "+tablespace+" blocks:"+usedBlockList.size()+" cache hit="+cacheHit+" miss="+cacheMiss;
	}


}
