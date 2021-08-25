package com.neocoretechs.bigsack.stream;

import java.io.IOException;

import com.neocoretechs.bigsack.iterator.EntrySetIterator;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
/**
 * Java 8 stream extensions for BigSack delivery of ordered persistent datasets.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 *
 */
public class EntrySetStream extends SackStream {

	public EntrySetStream(EntrySetIterator esi) {
		super(esi);
	}

	public EntrySetStream(KeyValueMainInterface kvMain) throws IOException {
		this(new EntrySetIterator(kvMain));
	}

}
