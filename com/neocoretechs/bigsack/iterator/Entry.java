package com.neocoretechs.bigsack.iterator;

import java.io.Serializable;
/**
 * Serializable wrapper for Map.Entry
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 *
 */
public class Entry implements java.util.Map.Entry<Comparable, Object>, Serializable {
	private static final long serialVersionUID = 4761413076980149698L;
	Comparable key;
	Object value;
	public Entry(Comparable key, Object value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public Comparable getKey() {
		return key;
	}

	@Override
	public Object getValue() {
		return value;
	}

	@Override
	public Object setValue(Object value) {
		this.value = value;
		return value;
	}
	
	@Override
	public String toString() {
		return String.format("<%s,%s>%n", key, value);
	}

}
