package com.neocoretechs.bigsack.stream;

import java.io.IOException;

import com.neocoretechs.bigsack.iterator.KeySetIterator;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;

public class KeySetStream extends SackStream {

	public KeySetStream(KeySetIterator esi) {
		super(esi);
	}

	public KeySetStream(KeyValueMainInterface kvMain) throws IOException {
		this(new KeySetIterator(kvMain));
	}


}
