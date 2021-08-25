package com.neocoretechs.bigsack.stream;

import java.io.IOException;

import com.neocoretechs.bigsack.iterator.KeySetIterator;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
/**
 * Java 8 stream extensions for BigSack delivery of ordered persistent datasets.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 *
 */
public class KeySetStream extends SackStream {

	public KeySetStream(KeySetIterator esi) {
		super(esi);
	}

	public KeySetStream(KeyValueMainInterface kvMain) throws IOException {
		this(new KeySetIterator(kvMain));
	}


}
