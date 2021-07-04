package com.neocoretechs.bigsack.keyvaluepages;

/**
 * Interface KVIteratorIF. Interface to facilitate Key/Value retrieval.
 */
public interface KVIteratorIF <K extends Comparable, V> {
    public boolean item(K key, V value);
}
