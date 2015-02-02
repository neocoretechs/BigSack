package com.neocoretechs.bigsack.io.pooled;

import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

/**
 * The class functions as the used block list for BlockAccessIndex elements that represent our
 * in memory pool of disk blocks. Its construction involves keeping track of the list of
 * free blocks as well to move items between the two. 
 * @author jg
 *
 */
public class MappedBlockBuffer extends Vector<BlockAccessIndex>  {
	
	private static final long serialVersionUID = -5744666991433173620L;
	private static final boolean DEBUG = false;
	private boolean shouldRun = true;
	private Vector<BlockAccessIndex> freeBL; 
	private GlobalDBIO globalIO;
	private BlockAccessIndex tmpBai; // general utility
	private int minBufferSize = 10;

	public MappedBlockBuffer(GlobalDBIO globalIO) {
		this.globalIO = globalIO;
		this.freeBL = new Vector<BlockAccessIndex>(this.globalIO.getMAXBLOCKS()); // free blocks
		// populate with blocks, they're all free for now
		for (int i = 0; i < this.globalIO.getMAXBLOCKS(); i++) {
			freeBL.addElement(new BlockAccessIndex(globalIO));
		}
		// set up locally buffered thread instances
		tmpBai = new BlockAccessIndex(this.globalIO);
		minBufferSize = globalIO.getMAXBLOCKS()/10; // we need at least one
	}
	/**
	 * Get an element from free list 0 and remove and return it
	 * @return
	 */
	public synchronized BlockAccessIndex take() {
		BlockAccessIndex bai = (freeBL.elementAt(0));
		freeBL.removeElementAt(0);
		return bai;
	}
	
	public synchronized void put(BlockAccessIndex bai) { freeBL.add(bai); }
	
	public synchronized void forceBufferClear() {
		Iterator<BlockAccessIndex> it = iterator();
		while(it.hasNext()) {
				BlockAccessIndex bai = (BlockAccessIndex) it.next();
				bai.resetBlock();
				put(bai);
		}
		clear();
	}
	
	public synchronized BlockAccessIndex getUsedBlock(long loc) {
			tmpBai.setTemplateBlockNumber(loc);
			int idex = indexOf(tmpBai);//.ceilingKey(tmpBai);
			if(idex == -1)
					return null;
			return get(idex);
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
		add(bai);
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
		add(bai);//put(bai, null);
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
			if( bufSize < globalIO.getMAXBLOCKS() )
				return;
			Iterator<BlockAccessIndex> elbn = this.iterator();
			int numGot = 0;
			BlockAccessIndex[] found = new BlockAccessIndex[minBufferSize];// our candidates
			while (elbn.hasNext()) {
				BlockAccessIndex ebaii = (elbn.next());
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
					this.remove(found[i]);
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
		Iterator<BlockAccessIndex> elbn = this.iterator();
		while (elbn.hasNext()) {
					BlockAccessIndex ebaii = (elbn.next());
					if (ebaii.getAccesses() == 0) {
						if(ebaii.getBlk().isIncore() && !ebaii.getBlk().isInlog()) {
							globalIO.getUlog().writeLog(ebaii); // will set incore, inlog, and push to raw store via applyChange of Loggable
						}
						elbn.remove();
						freeBL.add(ebaii);
					}
		}
	}
	/**
	 * Commit all outstanding blocks in the buffer, bypassing the log subsystem. Should be used with forethought
	 * @throws IOException
	 */
	public synchronized void directBufferWrite() throws IOException {
		Iterator<BlockAccessIndex> elbn = this.iterator();
		if(DEBUG) System.out.println("MappedBlockBuffer.direct buffer write");
		while (elbn.hasNext()) {
					BlockAccessIndex ebaii = (elbn.next());
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
			Iterator<BlockAccessIndex> elbn = this.iterator();
			while (elbn.hasNext()) {
				BlockAccessIndex ebaii = (elbn.next());
				if (ebaii.getAccesses() == 0) {
						if(ebaii.getBlk().isIncore() && !ebaii.getBlk().isInlog()) {
							globalIO.getUlog().writeLog(ebaii); // will set incore, inlog, and push to raw store via applyChange of Loggable
							//throw new IOException("Accesses 0 but incore true " + ebaii);
						}
						// Dont toss block at 0,0. its our BTree root and we will most likely need it soon
						if( ebaii.getBlockNum() == 0L )
							continue;
						elbn.remove();
						freeBL.add(ebaii);
						return;
				}
			}
			throw new IOException("Cannot free new block from pool, increase poool size");
		}
	}
	

}
