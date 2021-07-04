package com.neocoretechs.bigsack.keyvaluepages;

/**
 * Represents the result of a K/V search. Page, index, and whether that index is at a key that
 * is a direct match to the target key.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2015
 *
 */
public final class KeySearchResult {
	public KeyPageInterface page = null;
	public long pageId = -1;
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
	/**
	 * Alternate search object delivering id of {@link KeyPageInterface};
	 * @param pageId
	 * @param i
	 * @param b
	 */
	public KeySearchResult(long pageId, int i, boolean b) {
		this(i,b);
		this.pageId = pageId;
	}
	
	public String toString() {
		return "KeySearchResult atKey:"+atKey+" insert point:"+insertPoint+" page:"+page+" pageId:"+pageId;
	}
}
