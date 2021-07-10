package com.neocoretechs.bigsack.hashmap;

import java.io.IOException;

import com.neocoretechs.bigsack.keyvaluepages.RootKeyPageInterface;

public interface PageStreamIF<T> {
	public int item(RootKeyPageInterface page, int count, int limit) throws IOException;
}
