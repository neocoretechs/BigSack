package com.neocoretechs.bigsack.io.pooled;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

public class MappedBlockBuffer extends TreeMap<BlockAccessIndex, Object>  {
	private static final long serialVersionUID = -5744666991433173620L;
	/**
	* Toss out pool blocks not in use, iterate through and compute random
	* chance of keeping block
	*/
	public void checkBufferFlush(List<BlockAccessIndex> freeBL) throws IOException {
		if (freeBL.size() == 0) {
			Iterator<BlockAccessIndex> elbn = this.keySet().iterator();
			boolean clearedOne = false; // we need at least one
			// odds of not tossing any given block are 1 in (iOdds+1)
			int iOdds = 2;
			Random bookie = new Random();
			while (elbn.hasNext()) {
				BlockAccessIndex ebaii = (elbn.next());
				if (ebaii.getAccesses() == 0) {
					if (ebaii.getBlk().isIncore())
						throw new IOException(
							"Accesses 0 but incore true " + ebaii);
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

}
