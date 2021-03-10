package com.neocoretechs.bigsack.stream;

import java.io.IOException;

import com.neocoretechs.bigsack.btree.BTreeMain;
import com.neocoretechs.bigsack.iterator.SubSetKVIterator;

public class SubSetKVStream extends SackStream {

	public SubSetKVStream(SubSetKVIterator esi) {
		super(esi);
	}

	public SubSetKVStream(Comparable fkey, Comparable tkey, BTreeMain bTree) throws IOException {
		this(new SubSetKVIterator(fkey, tkey, bTree));
	}

}
