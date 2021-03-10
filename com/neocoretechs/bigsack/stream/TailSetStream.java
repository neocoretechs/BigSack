package com.neocoretechs.bigsack.stream;

import java.io.IOException;

import com.neocoretechs.bigsack.btree.BTreeMain;
import com.neocoretechs.bigsack.iterator.TailSetIterator;

public class TailSetStream extends SackStream {

	public TailSetStream(TailSetIterator esi) {
		super(esi);
	}

	public TailSetStream(Comparable fkey, BTreeMain bTree) throws IOException {
		this(new TailSetIterator(fkey, bTree));
	}


}
