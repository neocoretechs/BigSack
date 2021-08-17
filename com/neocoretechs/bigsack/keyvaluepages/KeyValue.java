package com.neocoretechs.bigsack.keyvaluepages;

import java.io.IOException;

import com.neocoretechs.bigsack.io.Optr;

/**
 * Class representing a key/value pair with associated state and deep store pointers.<p/>
 * Attempt to maintain state is only used where patently obvious, in getKey and getValue, where key or value is null, pointer is not empty
 * and state is mustRead. The default state of a new entry is mustRead, as the assumption is the deep store page will populate the pointer fields.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 */
public class KeyValue<K extends Comparable, V> {
	public enum synchStates {
		mustRead,
		mustWrite,
		mustReplace,
		mustDelete,
		upToDate
	}
	public synchStates keyState = synchStates.mustRead;
	public synchStates valueState = synchStates.mustRead;
	private K mKey;
    private V mValue;
    private Optr keyOptr = Optr.emptyPointer;
    private Optr valueOptr = Optr.emptyPointer;
    private NodeInterface<K,V> node;
    
    /**
     * Constructor that creates a blank key to prepare to receive pointers
     * to facilitate retrieval.
     */
    public KeyValue(NodeInterface<K,V> node) {
    	this.node = node;
    }

    public KeyValue(K key, V value, NodeInterface<K,V> node) {
    	this.node = node;
        mKey = key;
        mValue = value;
    }
    
    public K getmKey() throws IOException {
    	if(keyState == synchStates.mustRead && mKey == null && !keyOptr.equals(Optr.emptyPointer)) {
    		mKey = (K) node.getKeyValueMain().getKey(keyOptr);
    		keyState = synchStates.upToDate;
    	}
		return mKey;
	}

	public void setmKey(K mKey) {
		this.mKey = mKey;
	}

	public V getmValue() throws IOException {
	   	if(valueState == synchStates.mustRead && mValue == null && !valueOptr.equals(Optr.emptyPointer)) {
    		mValue = (V) node.getKeyValueMain().getValue(keyOptr);
    	}
		return mValue;
	}

	public void setmValue(V mValue) {
		this.mValue = mValue;
	}

	public Optr getKeyOptr() {
		return keyOptr;
	}

	public void setKeyOptr(Optr keyId) {
		this.keyOptr = keyId;
	}

	public Optr getValueOptr() {
		return valueOptr;
	}

	public void setValueOptr(Optr valueId) {
		this.valueOptr = valueId;
	}
    
    @Override
    public String toString() {
    	return String.format("Key=%s%n Value=%s%nKeyId=%s ValueId=%s keyUpdated=%s valueUpdated=%s%n", (mKey == null ? "null" : mKey), (mValue == null ? "null" : mValue), keyOptr, valueOptr, keyState, valueState);
    }
}
