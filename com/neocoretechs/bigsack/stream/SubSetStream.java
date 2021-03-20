package com.neocoretechs.bigsack.stream;

import java.io.IOException;

import com.neocoretechs.bigsack.btree.BTreeMain;
import com.neocoretechs.bigsack.iterator.SubSetIterator;

public class SubSetStream extends SackStream {

	public SubSetStream(SubSetIterator esi) {
		super(esi);
	}
	
	public SubSetStream(Comparable fkey, Comparable tkey, BTreeMain bTree) throws IOException {
		this(new SubSetIterator(fkey, tkey, bTree));
	}
}
