package com.neocoretechs.bigsack.stream;

import java.io.IOException;

import com.neocoretechs.bigsack.iterator.TailSetIterator;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
/**
 * Java 8 stream extensions for BigSack delivery of ordered persistent datasets.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 *
 */
public class TailSetStream extends SackStream {

	public TailSetStream(TailSetIterator esi) {
		super(esi);
	}

	public TailSetStream(Comparable fkey, KeyValueMainInterface kvMain) throws IOException {
		this(new TailSetIterator(fkey, kvMain));
	}


}
