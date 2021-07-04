package com.neocoretechs.bigsack.io.stream;

import java.io.IOException;

import com.neocoretechs.bigsack.keyvaluepages.RootKeyPageInterface;

public interface PageIteratorIF<P extends RootKeyPageInterface> {
	public void item(P page) throws IOException;
}
