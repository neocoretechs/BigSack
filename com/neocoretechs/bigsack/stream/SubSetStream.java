package com.neocoretechs.bigsack.stream;

import java.io.IOException;

import com.neocoretechs.bigsack.iterator.SubSetIterator;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
/**
 * Java 8 stream extensions for BigSack delivery of ordered persistent datasets.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 *
 */
public class SubSetStream extends SackStream {

	public SubSetStream(SubSetIterator esi) {
		super(esi);
	}
	
	public SubSetStream(Comparable fkey, Comparable tkey, KeyValueMainInterface kvMain) throws IOException {
		this(new SubSetIterator(fkey, tkey, kvMain));
	}
}
