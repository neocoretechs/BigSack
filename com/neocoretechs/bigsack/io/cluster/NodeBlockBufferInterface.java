package com.neocoretechs.bigsack.io.cluster;
/**
 * For those worker nodes providing tablespace access, we have a way to
 * retrieve the per tablespace buffer
 * @author jg
 *
 */
public interface NodeBlockBufferInterface {
	public NodeBlockBuffer getBlockBuffer();
}
