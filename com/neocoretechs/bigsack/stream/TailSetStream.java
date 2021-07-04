package com.neocoretechs.bigsack.stream;

import java.io.IOException;

import com.neocoretechs.bigsack.iterator.TailSetIterator;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;

public class TailSetStream extends SackStream {

	public TailSetStream(TailSetIterator esi) {
		super(esi);
	}

	public TailSetStream(Comparable fkey, KeyValueMainInterface kvMain) throws IOException {
		this(new TailSetIterator(fkey, kvMain));
	}


}
