package com.neocoretechs.bigsack.btree;

public final class TreeSearchResult {
	BTreeKeyPage page = null;
	public TreeSearchResult(int i, boolean b) {
		insertPoint = i;
		atKey = b;
	}
	public TreeSearchResult(BTreeKeyPage sourcePage, int i, boolean b) {
		this(i,b);
		page = sourcePage;
	}
	public boolean atKey = false;
	public int insertPoint = 0;
}
