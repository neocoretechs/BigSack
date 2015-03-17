package com.neocoretechs.bigsack.io.stream;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.MappedBlockBuffer;
/**
 * Buffered input stream to deliver DB block data via via stream for
 * deserialization reads. We issue requests to underlying io managers via the passed
 * InputStream whihc we assume to be a DBInputStream that has access to the
 * underlying IO manager
 * @author jg
 *
 */
public class DBBufferedInputStream extends BufferedInputStream {
	private MappedBlockBuffer blockBuffer;
	private BlockAccessIndex lbai;
	/**
	 * InputStream is considered to be a DBInputStream which is attached to underlying session and IO manager
	 * @param arg0
	 */
	public DBBufferedInputStream(InputStream arg0) {
		super(arg0);
		this.blockBuffer = ((DBInputStream)in).sdbio;
		this.lbai = ((DBInputStream)in).lbai;
	}
	
	
	//Reads bytes from this byte-input stream into the specified byte array, starting at the given offset.
	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return blockBuffer.readn(lbai, b, len);
	}
	
	@Override
	public int read()  throws IOException {
		return blockBuffer.readi(lbai);
	}

	@Override
	public long skip(long len) throws IOException {
		//((DBInputStream)in).sdbio.objseek(len);	
		return len;
	}
	
	public void reset() {
	}
}
