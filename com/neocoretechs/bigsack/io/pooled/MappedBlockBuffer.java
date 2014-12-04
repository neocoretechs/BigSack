package com.neocoretechs.bigsack.io.pooled;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import com.neocoretechs.bigsack.io.MultithreadedIOManager;
/**
 * The class functions as the used block list for BlockAccessIndex elements that represent our
 * in memory pool of disk blocks. Its construction involves keeping track of the list of
 * free blocks as well to move items between the two. The implementation is treemap to 
 * allow for more complex retrieval of block subsets for better buffer management but at this point
 * the usage is rather trivial and overengineered.
 * @author jg
 *
 */
public class MappedBlockBuffer extends TreeMap<BlockAccessIndex, Object> implements Runnable  {
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
	*/
	private void checkBufferFlush() throws IOException {
			Iterator<BlockAccessIndex> elbn = this.keySet().iterator();
			int numToGet = 2; // we need at least one
			int numGot = 0;
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
					if( ++numGot == numToGet )
						return;
				}
			}
	}
	/**
	 * Commit all outstanding blocks in the buffer.
	 * @param globalIO
	 * @param freeBL
	 * @throws IOException
	 */
	public synchronized void commitBufferFlush() throws IOException {
		Iterator<BlockAccessIndex> elbn = this.keySet().iterator();
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
	 * @param globalIO
	 * @param freeBL
	 * @throws IOException
	 */
	public synchronized void directBufferWrite() throws IOException {
		Iterator<BlockAccessIndex> elbn = this.keySet().iterator();
		if(DEBUG) System.out.println("direct buffer write");
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
			Iterator<BlockAccessIndex> elbn = this.keySet().iterator();
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
