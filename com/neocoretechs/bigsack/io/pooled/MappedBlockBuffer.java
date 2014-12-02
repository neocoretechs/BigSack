package com.neocoretechs.bigsack.io.pooled;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import com.neocoretechs.bigsack.io.MultithreadedIOManager;

public class MappedBlockBuffer extends TreeMap<BlockAccessIndex, Object>  {
	private static final long serialVersionUID = -5744666991433173620L;
	private static final boolean DEBUG = false;
	/**
	* Toss out pool blocks not in use, iterate through and compute random
	* chance of keeping block
	*/
	public void checkBufferFlush(GlobalDBIO globalIO, List<BlockAccessIndex> freeBL) throws IOException {
		if (freeBL.size() == 0) {
			Iterator<BlockAccessIndex> elbn = this.keySet().iterator();
			boolean clearedOne = false; // we need at least one
			// odds of not tossing any given block are 1 in (iOdds+1)
			int iOdds = 2;
			Random bookie = new Random();
			while (elbn.hasNext()) {
				BlockAccessIndex ebaii = (elbn.next());
				if (ebaii.getAccesses() == 0) {
					if(ebaii.getBlk().isIncore() && !ebaii.getBlk().isInlog()) {
						globalIO.getUlog().writeLog(ebaii); // will set incore, inlog, and push to raw store via applyChange of Loggable
						//throw new IOException(
						//	"Accesses 0 but incore true " + ebaii);
					}
					// Dont toss block at 0,0. its our BTree root and we will most likely need it soon
					if( ebaii.getBlockNum() == 0L )
						continue;
					if (!clearedOne)
						clearedOne = true;
					else if (bookie.nextInt(iOdds + 1) >= iOdds) {
						continue;
					}
					elbn.remove();
					//
					freeBL.add(ebaii);
				}
			}
		}
	}
	/**
	 * Commit all outstanding blocks in the buffer.
	 * @param globalIO
	 * @param freeBL
	 * @throws IOException
	 */
	public void commitBufferFlush(GlobalDBIO globalIO, List<BlockAccessIndex> freeBL) throws IOException {
		Iterator<BlockAccessIndex> elbn = this.keySet().iterator();
		while (elbn.hasNext()) {
					BlockAccessIndex ebaii = (elbn.next());
					if (ebaii.getAccesses() == 0) {
						if(ebaii.getBlk().isIncore() && !ebaii.getBlk().isInlog()) {
							globalIO.getUlog().writeLog(ebaii); // will set incore, inlog, and push to raw store via applyChange of Loggable
						}
						elbn.remove();
						//
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
	public void directBufferWrite(GlobalDBIO globalIO) throws IOException {
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

}
