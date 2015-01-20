package com.neocoretechs.bigsack.io.pooled;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

/**
 * The class functions as the used block list for BlockAccessIndex elements that represent our
 * in memory pool of disk blocks. Its construction involves keeping track of the list of
 * free blocks as well to move items between the two. 
 * @author jg
 *
 */
public class MappedBlockBuffer extends Vector<BlockAccessIndex> implements Runnable  {
	
	private static final long serialVersionUID = -5744666991433173620L;
	private static final boolean DEBUG = false;
	private boolean shouldRun = true;
	private List<BlockAccessIndex> freeBL;
	private GlobalDBIO globalIO;
	
	public MappedBlockBuffer(GlobalDBIO globalIO, List<BlockAccessIndex> freeBL) {
		this.globalIO = globalIO;
		this.freeBL = freeBL;
	}
	/**
	* Toss out pool blocks not in use, ignore those bound for write
	* we will try to allocate 2 free blocks for every block used
	* We collect the elements and then transfer thenm from used to free block list
	* outside of the iterator to prevent conc mod ex
	*/
	private synchronized void checkBufferFlush() throws IOException {
			Iterator<BlockAccessIndex> elbn = this.iterator();
			int numToGet = 2; // we need at least one
			int numGot = 0;
			BlockAccessIndex[] found = new BlockAccessIndex[numToGet];// our candidates
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
					found[numGot] = ebaii;
					if( ++numGot == numToGet ) {
						break;
					}
				}
			}
			for(int i = 0; i < numToGet; i++) {
				if( found[i] != null ) {
					this.remove(found[i]);
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
	
	/**
	 * Our thread will wait until a free block removal signals processing, which
	 * will result in an attempt to free up 2 unused blocks in the buffer
	 */
	@Override
	public void run() {
		while(shouldRun ) {
			synchronized(this) {
				if(freeBL.size() > 0 ) {
					try {
						this.wait();
					} catch(InterruptedException ie) {}
				}
				if( freeBL.size() < globalIO.getMAXBLOCKS()/10 ) {
					try {
						checkBufferFlush();
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			}
	    }
	}

}
