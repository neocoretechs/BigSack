package com.neocoretechs.bigsack.stream;

import java.io.IOException;

import com.neocoretechs.bigsack.iterator.SubSetKVIterator;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
/**
 * Java 8 stream extensions for BigSack delivery of ordered persistent datasets.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 *
 */
public class SubSetKVStream extends SackStream {

	public SubSetKVStream(SubSetKVIterator esi) {
		super(esi);
	}

	public SubSetKVStream(Comparable fkey, Comparable tkey, KeyValueMainInterface kvMain) throws IOException {
		this(new SubSetKVIterator(fkey, tkey, kvMain));
	}

}
