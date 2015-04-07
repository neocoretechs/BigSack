package com.neocoretechs.bigsack.btree;

public final class TreeSearchResult {
	public TreeSearchResult(int i, boolean b) {
		insertPoint = i;
		atKey = b;
	}
	public boolean atKey = false;
	public int insertPoint = 0;
}
