package com.neocoretechs.bigsack.stream;

import java.io.IOException;

import com.neocoretechs.bigsack.iterator.SubSetIterator;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;

public class SubSetStream extends SackStream {

	public SubSetStream(SubSetIterator esi) {
		super(esi);
	}
	
	public SubSetStream(Comparable fkey, Comparable tkey, KeyValueMainInterface kvMain) throws IOException {
		this(new SubSetIterator(fkey, tkey, kvMain));
	}
}
