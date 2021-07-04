package com.neocoretechs.bigsack.stream;


import java.io.IOException;

import com.neocoretechs.bigsack.iterator.TailSetKVIterator;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;

public class TailSetKVStream extends SackStream {

	public TailSetKVStream(TailSetKVIterator esi) {
		super(esi);
	}

	public TailSetKVStream(Comparable fkey, KeyValueMainInterface kvMain) throws IOException {
		this(new TailSetKVIterator(fkey, kvMain));
	}


}
