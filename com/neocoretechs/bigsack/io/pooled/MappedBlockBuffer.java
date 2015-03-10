package com.neocoretechs.bigsack.io.pooled;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import com.neocoretechs.bigsack.DBPhysicalConstants;
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
	private ArrayList<BlockAccessIndex> freeBL; // free block list
	private BlockDBIOInterface globalIO;
	private int tablespace;
	private int minBufferSize = 10; // minimum number of buffers to reclaim on flush attempt
	private BlockingQueue<CompletionLatchInterface> requestQueue;
	private static int POOLBLOCKS;
	private static int QUEUEMAX = 256; // max requests before blocking
	private static int cacheHit = 0; // cache hit rate
	private static int cacheMiss = 0;
	/**
	 * Construct the buffer for this tablespace and link the global IO manager
	 * @param globalIO
	 * @param tablespace
	 * @throws IOException If we try to add an active block the the freechain
	 */
	public MappedBlockBuffer(GlobalDBIO globalIO, int tablespace) throws IOException {
		super(globalIO.getMAXBLOCKS()/DBPhysicalConstants.DTABLESPACES);
		POOLBLOCKS = globalIO.getMAXBLOCKS()/DBPhysicalConstants.DTABLESPACES;
		this.globalIO = (BlockDBIOInterface) globalIO;
		this.tablespace = tablespace;
		this.freeBL = new ArrayList<BlockAccessIndex>(POOLBLOCKS); // free blocks
		// populate with blocks, they're all free for now
		for (int i = 0; i < POOLBLOCKS; i++) {
			freeBL.add(new BlockAccessIndex(globalIO));
		}
		minBufferSize = POOLBLOCKS/10; // we need at least one
		requestQueue = new ArrayBlockingQueue<CompletionLatchInterface>(QUEUEMAX, true); // true maintains FIFO order
	}
	
	public BlockDBIOInterface getIOManager() { return globalIO; }
	/**
	 * Get an element from free list 0 and remove and return it
	 * @return
	 */
	public synchronized BlockAccessIndex take() {
			return freeBL.remove(0);

	}
	
	public synchronized void put(BlockAccessIndex bai) { 
			//if( GlobalDBIO.valueOf(bai.getBlockNum()).equals("Tablespace_1_114688"))
			//	System.out.println("PUTTING Tablespace_1_114688");
			freeBL.add(bai); 
	}
	
	public synchronized void forceBufferClear() {
		Enumeration<BlockAccessIndex> it = elements();
		while(it.hasMoreElements()) {
				BlockAccessIndex bai = (BlockAccessIndex) it.nextElement();
				//if( GlobalDBIO.valueOf(bai.getBlockNum()).equals("Tablespace_1_114688"))
				//	System.out.println("CLEARING Tablespace_1_114688");
				bai.resetBlock();
				put(bai);
		}
		clear();
	}
	
	public synchronized BlockAccessIndex getUsedBlock(long loc) {
		if( DEBUG )
			System.out.println("MappedBlockBuffer.getusedBlock Calling for USED BLOCK "+GlobalDBIO.valueOf(loc)+" "+loc);
		//if( GlobalDBIO.valueOf(loc).equals("Tablespace_1_114688"))
		//	System.out.println("GETTING USED Tablespace_1_114688 "+get(loc));
		BlockAccessIndex bai = get(loc);
		if( bai == null ) ++cacheMiss;
		else ++cacheHit;
		return bai;
	}
	
	/**
	* Get from free list, puts in used list of block access index.
	* Comes here when we can't find blocknum in table in findOrAddBlock.
	* New instance of BlockAccessIndex causes allocation
	* @param Lbn block number to add
	* @exception IOException if new dblock cannot be created
	*/
	public synchronized BlockAccessIndex addBlockAccess(Long Lbn) throws IOException {
		// make sure we have open slots
		if( DEBUG ) {
			System.out.println("MappedBlockBuffer.addBlockAccess "+GlobalDBIO.valueOf(Lbn)+" "+this);
		}
		//if( GlobalDBIO.valueOf(Lbn).equals("Tablespace_1_114688"))
		//	System.out.println("addBlockAccess Tablespace_1_114688");
		checkBufferFlush();
		BlockAccessIndex bai = take();
		((GlobalDBIO) globalIO).getIOManager().FseekAndRead(Lbn, bai.getBlk());
		bai.setBlockNumber(Lbn);
		//if( GlobalDBIO.valueOf(Lbn).equals("Tablespace_1_114688"))
		//	System.out.println("addBlockAccess Tablespace_1_114688:"+bai+" "+bai.getBlk().blockdump());
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
		checkBufferFlush();
		//if( GlobalDBIO.valueOf(Lbn).equals("Tablespace_1_114688"))
		//	System.out.println("putting Tablespace_1_114688");
		BlockAccessIndex bai = take();
		bai.setBlockNumber(Lbn);
		put(Lbn, bai);//put(bai, null);
		if( DEBUG ) {
			System.out.println("MappedBlockBuffer.addBlockAccessNoRead "+GlobalDBIO.valueOf(Lbn)+" returning after freeBL take "+bai+" "+this);
		}
		return bai;
	}
	
	public synchronized BlockAccessIndex findOrAddBlockAccess(long bn) throws IOException {
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
	public synchronized void checkBufferFlush() throws IOException {
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
				if (ebaii.getAccesses() == 0) {
					if(ebaii.getBlk().isIncore() && !ebaii.getBlk().isInlog()) {
						((GlobalDBIO)globalIO).getUlog().writeLog(ebaii); // will set incore, inlog, and push to raw store via applyChange of Loggable
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
					if (ebaii.getAccesses() == 1) {
						if(ebaii.getBlk().isIncore() && !ebaii.getBlk().isInlog()) {
							if( DEBUG )
								System.out.println("MappedBlockBuffer.checkBufferFlush set to write pool entry to log "+ebaii);
							((GlobalDBIO)globalIO).getUlog().writeLog(ebaii); // will set incore, inlog, and push to raw store via applyChange of Loggable
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
	}
	/**
	 * Commit all outstanding blocks in the buffer.
	 * @throws IOException
	 */
	public synchronized void commitBufferFlush() throws IOException {
		Enumeration<BlockAccessIndex> elbn = this.elements();
		while (elbn.hasMoreElements()) {
				BlockAccessIndex ebaii = (elbn.nextElement());
				if( ebaii.getAccesses() > 1 )
					throw new IOException("****COMMIT BUFFER access "+ebaii.getAccesses()+" for buffer "+ebaii);
				if(ebaii.getBlk().isIncore() && ebaii.getBlk().isInlog())
					throw new IOException("****COMMIT BUFFER block in core and log simultaneously! "+ebaii);
				if (ebaii.getAccesses() < 2) {
					if(ebaii.getBlk().isIncore() && !ebaii.getBlk().isInlog()) {
						((GlobalDBIO)globalIO).getUlog().writeLog(ebaii); // will set incore, inlog, and push to raw store via applyChange of Loggable
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
							System.out.println("fully writing "+ebaii.getBlockNum()+" "+ebaii.getBlk());
						((GlobalDBIO)globalIO).getIOManager().FseekAndWriteFully(ebaii.getBlockNum(), ebaii.getBlk());
						((GlobalDBIO)globalIO).getIOManager().Fforce();
						ebaii.getBlk().setIncore(false);
					}
		}
	}
	/**
	 * Check the free block list for 0 elements. This is a demand response method guaranteed to give us a free block
	 * if 0, begin a search for an element that has 0 accesses
	 * if its in core and not yet in log, write through
	 * @throws IOException
	 */
	public synchronized void freeupBlock() throws IOException {
		if( freeBL.size() == 0 ) {
			Enumeration<BlockAccessIndex> elbn = this.elements();
			while (elbn.hasMoreElements()) {
				BlockAccessIndex ebaii = (elbn.nextElement());
				if( ebaii.getAccesses() > 1 )
					throw new IOException("****COMMIT BUFFER access "+ebaii.getAccesses()+" for buffer "+ebaii);
				if(ebaii.getBlk().isIncore() && ebaii.getBlk().isInlog())
					throw new IOException("****COMMIT BUFFER block in core and log simultaneously! "+ebaii);
				//if( GlobalDBIO.valueOf(ebaii.getBlockNum()).equals("Tablespace_1_114688"))
				//	System.out.println("freeup block Tablespace_1_114688 "+ebaii);
				if (ebaii.getAccesses() == 0) {
						if(ebaii.getBlk().isIncore() && !ebaii.getBlk().isInlog()) {
							((GlobalDBIO)globalIO).getUlog().writeLog(ebaii); // will set incore, inlog, and push to raw store via applyChange of Loggable
							//throw new IOException("Accesses 0 but incore true " + ebaii);
						}
						// Dont toss block at 0,0. its our BTree root and we will most likely need it soon
						if( ebaii.getBlockNum() == 0L )
							continue;
						remove(ebaii.getBlockNum());
						//if( GlobalDBIO.valueOf(ebaii.getBlockNum()).equals("Tablespace_1_114688"))
						//	System.out.println("FREEUP BLOCK Tablespace_1_114688");
						freeBL.add(ebaii);
						return;
				}
			}
			throw new IOException("Cannot free new block from pool, increase pool size");
		}
	}
	
	public synchronized String toString() {
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
