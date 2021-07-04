package com.neocoretechs.bigsack.stream;

import java.io.IOException;

import com.neocoretechs.bigsack.iterator.HeadSetKVIterator;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;

public class HeadSetKVStream extends SackStream {

	public HeadSetKVStream(HeadSetKVIterator esi) {
		super(esi);
	}

	public HeadSetKVStream(Comparable tkey, KeyValueMainInterface kvMain) throws IOException {
		this(new HeadSetKVIterator(tkey, kvMain));
	}


}
