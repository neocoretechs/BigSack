package com.neocoretechs.bigsack.stream;


import java.io.IOException;

import com.neocoretechs.bigsack.btree.BTreeMain;
import com.neocoretechs.bigsack.iterator.TailSetKVIterator;

public class TailSetKVStream extends SackStream {

	public TailSetKVStream(TailSetKVIterator esi) {
		super(esi);
	}

	public TailSetKVStream(Comparable fkey, BTreeMain bTree) throws IOException {
		this(new TailSetKVIterator(fkey, bTree));
	}


}
