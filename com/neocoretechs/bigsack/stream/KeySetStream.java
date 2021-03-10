package com.neocoretechs.bigsack.stream;

import java.io.IOException;

import com.neocoretechs.bigsack.btree.BTreeMain;
import com.neocoretechs.bigsack.iterator.KeySetIterator;

public class KeySetStream extends SackStream {

	public KeySetStream(KeySetIterator esi) {
		super(esi);
	}

	public KeySetStream(BTreeMain bTree) throws IOException {
		this(new KeySetIterator(bTree));
	}


}
