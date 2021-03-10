package com.neocoretechs.bigsack.stream;

import java.io.IOException;

import com.neocoretechs.bigsack.btree.BTreeMain;
import com.neocoretechs.bigsack.iterator.EntrySetIterator;

public class EntrySetStream extends SackStream {

	public EntrySetStream(EntrySetIterator esi) {
		super(esi);
	}

	public EntrySetStream(BTreeMain bTree) throws IOException {
		this(new EntrySetIterator(bTree));
	}

}
