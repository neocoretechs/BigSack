package com.neocoretechs.bigsack.keyvaluepages;

import java.io.IOException;

public interface NodeInterface<K extends Comparable, V> {

	KeyValueMainInterface getKeyValueMain();

	KeyValue<K, V> getKeyValueArray(int index);

	void setPageId(long pageId);
	
	long getPageId();

	NodeInterface<K, V> getChild(int index);

	NodeInterface getChildNoread(int index);

	void setKeyValueArray(int index, KeyValue<K, V> bTKey);

	void setChild(int index, NodeInterface<K, V> bTNode);

	int getNumKeys();

	void setNumKeys(int numKeys);

	String toString();

	int getTablespace();

	void setPage(KeyPageInterface page) throws IOException;

	void initKeyValueArray(int index);

}