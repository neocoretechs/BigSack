package com.neocoretechs.bigsack.stream;

import java.io.IOException;

import com.neocoretechs.bigsack.btree.BTreeMain;
import com.neocoretechs.bigsack.iterator.HeadSetKVIterator;

public class HeadSetKVStream extends SackStream {

	public HeadSetKVStream(HeadSetKVIterator esi) {
		super(esi);
	}

	public HeadSetKVStream(Comparable tkey, BTreeMain bTree) throws IOException {
		this(new HeadSetKVIterator(tkey, bTree));
	}


}
