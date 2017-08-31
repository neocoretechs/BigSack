package com.neocoretechs.bigsack.io.pooled;
/**
 * Observer for subsystems that need to be notified when the buffer pool acquires a new block. 
 * An example of this is when we are deserializing multiple block payloads and the BlockStream block cursor
 * needs a new pool block 'slid under' the current data stream, such as those used by DBInput and DBOutput.
 * We decouple the susbsystem so that the MappedBlockBuffer doesnt need to know about the BlockStream or the
 * recovery log, for instance.
 * @author jg
 *
 */
public interface BlockChangeEvent {
	public void blockChanged(int tablespace, BlockAccessIndex bai);
}
