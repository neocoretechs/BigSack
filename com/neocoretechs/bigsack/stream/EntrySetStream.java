package com.neocoretechs.bigsack.stream;

import java.io.IOException;

import com.neocoretechs.bigsack.iterator.EntrySetIterator;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;

public class EntrySetStream extends SackStream {

	public EntrySetStream(EntrySetIterator esi) {
		super(esi);
	}

	public EntrySetStream(KeyValueMainInterface kvMain) throws IOException {
		this(new EntrySetIterator(kvMain));
	}

}
