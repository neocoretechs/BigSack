package com.neocoretechs.bigsack.stream;

import java.io.IOException;

import com.neocoretechs.bigsack.iterator.SubSetKVIterator;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;

public class SubSetKVStream extends SackStream {

	public SubSetKVStream(SubSetKVIterator esi) {
		super(esi);
	}

	public SubSetKVStream(Comparable fkey, Comparable tkey, KeyValueMainInterface kvMain) throws IOException {
		this(new SubSetKVIterator(fkey, tkey, kvMain));
	}

}
