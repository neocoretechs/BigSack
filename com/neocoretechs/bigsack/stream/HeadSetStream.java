package com.neocoretechs.bigsack.stream;

import java.io.IOException;

import com.neocoretechs.bigsack.iterator.HeadSetIterator;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;

public class HeadSetStream extends SackStream {

	public HeadSetStream(HeadSetIterator esi) {
		super(esi);
	}

	public HeadSetStream(Comparable tkey, KeyValueMainInterface kvMain) throws IOException {
		this(new HeadSetIterator(tkey, kvMain));
	}


}
