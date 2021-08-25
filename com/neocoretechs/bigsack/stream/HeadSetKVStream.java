package com.neocoretechs.bigsack.stream;

import java.io.IOException;

import com.neocoretechs.bigsack.iterator.HeadSetKVIterator;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
/**
 * Java 8 stream extensions for BigSack delivery of ordered persistent datasets.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 *
 */
public class HeadSetKVStream extends SackStream {

	public HeadSetKVStream(HeadSetKVIterator esi) {
		super(esi);
	}

	public HeadSetKVStream(Comparable tkey, KeyValueMainInterface kvMain) throws IOException {
		this(new HeadSetKVIterator(tkey, kvMain));
	}


}
