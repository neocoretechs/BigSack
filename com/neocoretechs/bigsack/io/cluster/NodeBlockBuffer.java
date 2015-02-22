package com.neocoretechs.bigsack.io.cluster;

import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;

import com.neocoretechs.bigsack.io.pooled.Datablock;

public final class NodeBlockBuffer  {
	private static int NODEPOOLBLOCKS = 10000;
	private ConcurrentHashMap<Long, Datablock> blockBuffer;
	
	public NodeBlockBuffer() {
		blockBuffer = new ConcurrentHashMap<Long,Datablock>(NODEPOOLBLOCKS);
	}
	
	public void put(Long ptr, Datablock dblk) {
		if( blockBuffer.size() >= NODEPOOLBLOCKS ) {
			// must toss one first
			Enumeration<Long> e = blockBuffer.keys();
			Long rec = -1L;
			while(e.hasMoreElements()) {
				rec = e.nextElement();
				if( rec != 0L ) {
					Datablock tblk = blockBuffer.get(rec);
					// if it is still waiting for outstanding write, dont dump it
					if( !tblk.isIncore() )
						break;
				}
			}
			assert( rec != -1L ) : "NodeBlockBuffer unable to clear buffer slot for new block, buffer full.";
			blockBuffer.remove(rec);
		}
		blockBuffer.put(ptr, dblk);
	}
	
	public Datablock get(Long ptr) {
		return blockBuffer.get(ptr);
	}
}
