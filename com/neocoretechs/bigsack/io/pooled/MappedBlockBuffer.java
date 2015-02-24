package com.neocoretechs.bigsack.io.pooled;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.request.cluster.CompletionLatchInterface;

/**
 * The class functions as the used block list for BlockAccessIndex elements that represent our
 * in memory pool of disk blocks. Its construction involves keeping track of the list of
 * free blocks as well to move items between the two. 
 * Create the request with the appropriate instance of 'this' MappedBlockBuffer to call back
 * methods here. Then the completion latch for that request is counted down.
 * In the Master IO, the completion latch of the request is monitored for the proper number
 * of counts.
 * @author jg
 *
 */
public class MappedBlockBuffer extends ConcurrentHashMap<Long, BlockAccessIndex> implements Runnable {
	private static final long serialVersionUID = -5744666991433173620L;
	private static final boolean DEBUG = false;
	private boolean shouldRun = true;
	private ArrayList<BlockAccessIndex> freeBL; // free block list
	private GlobalDBIO globalIO;
	private int tablespace;
	private int minBufferSize = 10; // minimum number of buffers to reclaim on flush attempt
	private BlockingQueue<CompletionLatchInterface> requestQueue;
	private static int POOLBLOCKS;
	private static int QUEUEMAX = 256; // max requests before blocking
	/**
	 * Construct the buffer for this tablespace and link the global IO manager
	 * @param globalIO
	 * @param tablespace
	 */
	public MappedBlockBuffer(GlobalDBIO globalIO, int tablespace) {
		super(globalIO.getMAXBLOCKS()/DBPhysicalConstants.DTABLESPACES);
		POOLBLOCKS = globalIO.getMAXBLOCKS()/DBPhysicalConstants.DTABLESPACES;
		this.globalIO = globalIO;
		this.tablespace = tablespace;
		this.freeBL = new ArrayList<BlockAccessIndex>(POOLBLOCKS ); // free blocks
		// populate with blocks, they're all free for now
		for (int i = 0; i < globalIO.getMAXBLOCKS(); i++) {
			freeBL.add(new BlockAccessIndex(globalIO));
		}
		minBufferSize = POOLBLOCKS/10; // we need at least one
		requestQueue = new ArrayBlockingQueue<CompletionLatchInterface>(QUEUEMAX, true); // true maintains FIFO order
	}
	/**
	 * Get an element from free list 0 and remove and return it
	 * @return
	 */
	public BlockAccessIndex take() {
		synchronized(freeBL) {
			return freeBL.remove(0);
		}
	}
	
	public void put(BlockAccessIndex bai) { 
		synchronized(freeBL) {
			freeBL.add(bai); 
		}
	}
	
	public synchronized void forceBufferClear() {
		Enumeration<BlockAccessIndex> it = elements();
		while(it.hasMoreElements()) {
				BlockAccessIndex bai = (BlockAccessIndex) it.nextElement();
				bai.resetBlock();
				put(bai);
		}
		clear();
	}
	
	public BlockAccessIndex getUsedBlock(long loc) {
		return get(loc);
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
			System.out.println("MappedBlockBuffer.addBlockAccess "+GlobalDBIO.valueOf(Lbn));
		}
		checkBufferFlush();
		BlockAccessIndex bai = take();
		bai.setBlockNum(Lbn.longValue());
		put(Lbn, bai);
		if( DEBUG ) {
				System.out.println("MappedBlockBuffer.addBlockAccess "+GlobalDBIO.valueOf(Lbn)+" returning after freeBL take "+bai);
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
		// make sure we have open slots
		checkBufferFlush();
		BlockAccessIndex bai = take();
		bai.setTemplateBlockNumber(Lbn.longValue());
		put(Lbn, bai);//put(bai, null);
		return bai;
	}
	
	public synchronized BlockAccessIndex findOrAddBlockAccess(long bn) throws IOException {
		if( DEBUG ) {
			System.out.println("MappedBlockBuffer.findOrAddBlockAccess "+GlobalDBIO.valueOf(bn));
		}
		Long Lbn = new Long(bn);
		BlockAccessIndex bai = getUsedBlock(bn);
		if( DEBUG ) {
			System.out.println("MappedBlockBuffer.findOrAddBlockAccess "+GlobalDBIO.valueOf(bn)+" got block "+bai);
		}
		if (bai != null) {
			return bai;
		}
		// didn't find it, we must add
		return addBlockAccess(Lbn);
	}
	/**
	* Toss out pool blocks not in use, ignore those bound for write
	* we will try to allocate at least 1 free block if size is 0, if we cant, throw exception
	* If we must sweep buffer, try to fee up at least 1/10 of total
	* We collect the elements and then transfer them from used to free block list
	* 
	*/
	public synchronized void checkBufferFlush() throws IOException {
			int bufSize = this.size();
			if( bufSize < POOLBLOCKS )
				return;
			Enumeration<BlockAccessIndex> elbn = this.elements();
			int numGot = 0;
			BlockAccessIndex[] found = new BlockAccessIndex[minBufferSize];// our candidates
			while (elbn.hasMoreElements()) {
				BlockAccessIndex ebaii = (elbn.nextElement());
				if( DEBUG )
					System.out.println("MappedBlockBuffer.checkBufferFlush Block buffer "+ebaii);
				if (ebaii.getAccesses() == 0) {
					if(ebaii.getBlk().isIncore() && !ebaii.getBlk().isInlog()) {
						globalIO.getUlog().writeLog(ebaii); // will set incore, inlog, and push to raw store via applyChange of Loggable
						//throw new IOException("Accesses 0 but incore true " + ebaii);
					}
					// Dont toss block at 0,0. its our BTree root and we will most likely need it soon
					if( ebaii.getBlockNum() == 0L )
						continue;
					found[numGot] = ebaii;
					if( ++numGot == minBufferSize ) {
						break;
					}
				}
			}
			if( numGot == 0 )
				throw new IOException("Unable to free up blocks in buffer pool");
			
			for(int i = 0; i < numGot; i++) {
				if( found[i] != null ) {
					this.remove(found[i].getBlockNum());
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
					if (ebaii.getAccesses() == 0) {
						if(ebaii.getBlk().isIncore() && !ebaii.getBlk().isInlog()) {
							globalIO.getUlog().writeLog(ebaii); // will set incore, inlog, and push to raw store via applyChange of Loggable
						}
						freeBL.add(ebaii);
					}
		}
		clear();
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
						if( DEBUG)System.out.println("fully writing "+ebaii.getBlockNum()+" "+ebaii.getBlk());
							globalIO.getIOManager().FseekAndWriteFully(ebaii.getBlockNum(), ebaii.getBlk());
							globalIO.getIOManager().Fforce();
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
				if (ebaii.getAccesses() == 0) {
						if(ebaii.getBlk().isIncore() && !ebaii.getBlk().isInlog()) {
							globalIO.getUlog().writeLog(ebaii); // will set incore, inlog, and push to raw store via applyChange of Loggable
							//throw new IOException("Accesses 0 but incore true " + ebaii);
						}
						// Dont toss block at 0,0. its our BTree root and we will most likely need it soon
						if( ebaii.getBlockNum() == 0L )
							continue;
						remove(ebaii.getBlockNum());
						freeBL.add(ebaii);
						return;
				}
			}
			throw new IOException("Cannot free new block from pool, increase poool size");
		}
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
				ior.process();
			} catch (IOException e) {
				System.out.println("MappedBlockBuffer exception processing request "+ior+" "+e);
				break;
			}
		}
		
	}

}
