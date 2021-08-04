package com.neocoretechs.bigsack.hashmap;

import java.io.IOException;

import com.neocoretechs.bigsack.keyvaluepages.KeyPageInterface;
import com.neocoretechs.bigsack.keyvaluepages.RootKeyPageInterface;

public interface PageSearchIF<T> {
	public KeyPageInterface item(RootKeyPageInterface page) throws IOException;
}
