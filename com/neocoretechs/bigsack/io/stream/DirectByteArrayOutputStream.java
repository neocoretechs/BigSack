package com.neocoretechs.bigsack.io.stream;

import java.io.ByteArrayOutputStream;
/**
 * Implementation of ByteArrayOutputStream that does NOT copy the backing store
 * @author jg
 *
 */
public final class DirectByteArrayOutputStream  extends ByteArrayOutputStream {
		  public DirectByteArrayOutputStream() {
		  }

		  public DirectByteArrayOutputStream(int size) {
		    super(size);
		  }

		  public int getCount() {
		    return count;
		  }

		  public byte[] getBuf() {
		    return buf;
		  }
}
