package com.neocoretechs.bigsack.keyvaluepages;

import java.io.IOException;

import com.neocoretechs.bigsack.io.Optr;

/**
 * Class KeyValue
 */
public class KeyValue<K extends Comparable, V> {
	private boolean keyUpdated = false;
	private boolean valueUpdated = false;
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
    
    public void setNewNode(NodeInterface<K,V> newNode) {
    	this.node = newNode;
    }
    
    public K getmKey() throws IOException {
    	if(!keyUpdated && mKey == null && !keyOptr.equals(Optr.emptyPointer)) {
    		mKey = (K) node.getKeyValueMain().getKey(keyOptr);
    	}
		return mKey;
	}

	public void setmKey(K mKey) {
		this.mKey = mKey;
	}

	public V getmValue() throws IOException {
	   	if(!valueUpdated && mValue == null && !valueOptr.equals(Optr.emptyPointer)) {
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
    
    public boolean getKeyUpdated() {
    	return keyUpdated;
    }
    
    public void setKeyUpdated(boolean isUpdated) {
    	keyUpdated = isUpdated;
    }
    public boolean getValueUpdated() {
    	return valueUpdated;
    }
    
    public void setValueUpdated(boolean isUpdated) {
    	valueUpdated = isUpdated;
    }
    @Override
    public String toString() {
    	return String.format("Key=%s%n Value=%s%nKeyId=%s ValueId=%s keyUpdated=%b valueUpdated=%b%n", (mKey == null ? "null" : mKey), (mValue == null ? "null" : mValue), keyOptr, valueOptr, keyUpdated, valueUpdated);
    }
}
