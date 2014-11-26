package com.neocoretechs.bigsack.io.stream;

import java.io.ByteArrayOutputStream;

public class DirectByteArrayOutputStream  extends ByteArrayOutputStream {
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
