package com.neocoretechs.bigsack.keyvaluepages;

/**
 * Represents the result of a K/V search. Page, index, and whether that index is at a key that
 * is a direct match to the target key.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2015
 *
 */
public final class KeySearchResult {
	public KeyPageInterface page = null;
	public boolean atKey = false;
	public int insertPoint = 0;
	/**
	 * Form a free search result unattached to a key page

	 * @param i KeyPageInterface insert point index
	 * @param b true if key already exists and index points to it
	 */
	public KeySearchResult(int i, boolean b) {
		insertPoint = i;
		atKey = b;
	}
	/**
	 * Form a search result attached to a key page
	 * @param sourcePage The key page object
	 * @param i  insert point index
	 * @param b true if key already exists and index points to it
	 */
	public KeySearchResult(KeyPageInterface sourcePage, int i, boolean b) {
		this(i,b);
		page = sourcePage;
	}
	
	public KeyValue<Comparable, Object> getKeyValue() {
		return page.getKeyValueArray(insertPoint);
	}
	
	public String toString() {
		return "KeySearchResult atKey:"+atKey+" insert point:"+insertPoint+" page:"+page;
	}
}
