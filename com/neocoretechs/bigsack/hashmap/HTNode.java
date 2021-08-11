package com.neocoretechs.bigsack.hashmap;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import com.neocoretechs.bigsack.io.Optr;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.BlockStream;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.keyvaluepages.KVIteratorIF;
import com.neocoretechs.bigsack.keyvaluepages.KeyPageInterface;
import com.neocoretechs.bigsack.keyvaluepages.KeyValue;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
import com.neocoretechs.bigsack.keyvaluepages.NodeInterface;

/**
 * Class HTNode. In this context it represents the collision space of k/v pairs with the same hashkey value.
 * @author Jontahan Groff Copyright (C) NeoCoreTechs 2021
 */
public class HTNode<K extends Comparable, V> implements NodeInterface<K, V> {
	public static boolean DEBUG = false;
    private KeyValue<K, V> mKeys[] = new KeyValue[HMapKeyPage.MAXKEYS];
    protected long pageId = -1L;
    private boolean updated = false;
    protected boolean needsRead = true;
    protected int tablespace = -1;
    protected KeyValueMainInterface keyValueMain;
    protected KeyPageInterface page = null;
    protected NodeInterface<K, V> nextPage = null; // child
    private int numKeys = 0;

    public HTNode(KeyValueMainInterface hMapMain, long pageId) {
       this.keyValueMain = hMapMain;
       this.pageId = pageId;
       this.tablespace = GlobalDBIO.getTablespace(pageId);
    }
    
    /**
     * setPage is called when this is constructed with a page, so if the page has data, it is loaded to this new node.
     * @param page
     * @throws IOException
     */
    public HTNode(KeyPageInterface page) throws IOException {
    	setPage(page);
    }
    
	@Override
	/**
	 * We can use this method to set up the node on construction, or change its contents later with a new page.<p/>
	 * Conversely, if this node is presented with a blank page and has data, it will load it to the blank page.
	 */
    public void setPage(KeyPageInterface page) throws IOException {
    	this.page = page;
        this.keyValueMain = page.getKeyValueMain();
        this.pageId = page.getPageId();
        this.tablespace = GlobalDBIO.getTablespace(pageId);
        if(page.getNumKeys() > 0) { // if page has data, load it to this
        	loadNode();
        } else {
        	if(numKeys > 0) { // page has no data, but if this node has data, load it to the page
        		loadPage();
        	}
        }
    }
	
	/**
	 * Set up to load the newly presented page with the data in this node.<p/>
	 * Transfer the data we currently have. If pointers are empty, acquire the position.
	 * If pointers are valid, transfer them and assume existing pages are written.
	 * @throws IOException 
	 */
	private void loadPage() throws IOException {
		if(page.getNumKeys() > 0)
			throw new IOException("Attempt to overwrite page "+page+" with node "+this);
		page.setNode(this);
		// set everything to updated, forcing a load to the page
		for(int i = 0; i < numKeys; i++) {
			mKeys[i].setKeyUpdated(true);
		}
		((HMapKeyPage)page).putPage();
	}
	/**
	 * Set up to Load this node with data from the page.<p/> 
	 * @param page
	 * @throws IOException
	 */
	private void loadNode() throws IOException {
		if(numKeys > 0)
			throw new IOException("Attempt to overwrite node "+this+" with page "+page);
		DataInputStream dis = GlobalDBIO.getBlockInputStream(page.getBlockAccessIndex());
		if(dis.available() == 0) {
			System.out.printf("%s.loadNode nothing avail for page %s%n", this.getClass().getName(),page);
			return;
		}
		numKeys = (int) dis.readLong();
		System.out.printf("%s.loadNode loading %d keys for page %s%n", this.getClass().getName(),numKeys,page);
		for(int i = 0; i < numKeys; i++) {
			KeyValue<K,V> keyValue = new KeyValue<K,V>(this);
			long block = dis.readLong();
			short offset = dis.readShort();
			keyValue.setKeyOptr(new Optr(block,offset));
			block = dis.readLong();
			offset = dis.readShort();
			keyValue.setValueOptr(new Optr(block,offset));
			mKeys[i] = keyValue;
		}
		long blockNextPage = dis.readLong();
		if(blockNextPage != -1L)
			nextPage = new HTNode<K,V>(keyValueMain, blockNextPage);
	}
	
    /**
	 * @return the mCurrentKeyNum
	 */
	protected int getmCurrentKeyNum() {
		return numKeys;//mCurrentKeyNum;
	}

	@Override
	public KeyValueMainInterface getKeyValueMain() {
		return keyValueMain;
	}

	public int getTablespace() {
		return tablespace;
	}
	
	@Override
	public void initKeyValueArray(int index) {
		if(index >= numKeys)
			numKeys = index+1;
		if(mKeys[index] == null)
			mKeys[index] = new KeyValue<K,V>(this);
	}
	
    @Override
	public KeyValue<K, V> getKeyValueArray(int index) {
        // pre-create blank keys to receive key Ids so we can assign the page pointers and offsets
        // for subsequent retrieval
    	//if(mKeys[0] == null)
    	if(numKeys == 0 || index >= numKeys)
    		return null;
    	return mKeys[index];
    }
    
    @Override
	public long getPageId() { 
    	return pageId; 
    }
    
    @Override
    public void setPageId(long pageId) {
    	this.pageId = pageId;
    }
    
    @Override
	public NodeInterface<K, V> getChild(int index) {
    	throw new RuntimeException("Method not applicable to this key/value store");
    }
    
	@Override
	public NodeInterface getChildNoread(int index) {
		throw new RuntimeException("Method not applicable to this key/value store");
	}
	
    @Override
	public void setKeyValueArray(int index, KeyValue<K,V> kvKey) {
    	if(index >= numKeys) {
    		numKeys = index+1;
    	}
    	mKeys[index] = kvKey;
    }
    
    @Override
	public void setChild(int index, NodeInterface<K,V> bTNode) {
    	throw new RuntimeException("Method not applicable to this key/value store");
    }   
    
    @Override
	public int getNumKeys() {
    	return numKeys;//mCurrentKeyNum;
    }
    
    @Override
	public void setNumKeys(int numKeys) {
        this.numKeys = numKeys;
    } 
	 //
    // List all the items in the tree
    //
    public void list(KVIteratorIF<K, V> iterImpl) throws IOException {
        if (getmCurrentKeyNum() < 1) {
            return;
        }
        if (iterImpl == null) {
            return;
        }
        listEntriesInOrder(this, iterImpl);
    }

    public KeyValue get(Object object) throws IOException {
    	KVIteratorIF iterImpl = new KVIteratorIF() {
			@Override
			public boolean item(Comparable key, Object value) {
				if(value.equals(object))
					return true;
				return false;
			}		
    	};
    	return retrieveEntriesInOrder(this, iterImpl);
    }
    //
    // Recursively loop to list out the keys and their values
    // Return true if it should continues listing out futher
    // Return false if it is done
    //
    private boolean listEntriesInOrder(HTNode<K, V> treeNode, KVIteratorIF<K, V> iterImpl) throws IOException {
        if ((treeNode == null) ||
            (treeNode.getmCurrentKeyNum() == 0)) {
            return false;
        }

        boolean bStatus;
        KeyValue<K, V> keyVal;
        int currentKeyNum = treeNode.getmCurrentKeyNum();
        for (int i = 0; i < currentKeyNum; ++i) {
            //listEntriesInOrder((HTNode<K, V>) HTNode.getChildNodeAtIndex(treeNode, i), iterImpl);

            keyVal = treeNode.getKeyValueArray(i);
            if(keyVal == null)
            	return false;
            bStatus = iterImpl.item(keyVal.getmKey(), keyVal.getmValue());
            if (!bStatus) {
                return false;
            }

  
        }

        return true;
    }

    private KeyValue<K, V> retrieveEntriesInOrder(HTNode<K, V> treeNode, KVIteratorIF<K, V> iterImpl) throws IOException {
        if ((treeNode == null) ||
            (treeNode.getmCurrentKeyNum() == 0)) {
            return null;
        }
        boolean bStatus;
        KeyValue<K, V> keyVal;
        int currentKeyNum = treeNode.getmCurrentKeyNum();
        for (int i = 0; i < currentKeyNum; ++i) {
            //retrieveEntriesInOrder((HTNode<K, V>) HTNode.getChildNodeAtIndex(treeNode, i), iterImpl);
            keyVal = treeNode.getKeyValueArray(i);
            if(keyVal == null)
            	return null;
            bStatus = iterImpl.item(keyVal.getmKey(), keyVal.getmValue());
            if (bStatus) {
                return keyVal;
            }
        }

        return null;
    }
 
	/**
	 * Create a unique list of blocks that have already been populated with values from this node in order to possibly
	 * cluster new entries more efficiently.
	 * @return The list of unique blocks containing entries for this node.
	 */
	public ArrayList<Long> aggregatePayloadBlocks() {
		ArrayList<Long> blocks = new ArrayList<Long>();
		int i = 0;
		for(; i < getNumKeys(); i++) {
			if(getKeyValueArray(i) != null) { 
				if(getKeyValueArray(i).getKeyOptr() != null && 
					!blocks.contains(getKeyValueArray(i).getKeyOptr().getBlock()) &&
					!getKeyValueArray(i).getKeyOptr().equals(Optr.emptyPointer) ) {
						blocks.add(getKeyValueArray(i).getKeyOptr().getBlock());
				}
				if(getKeyValueArray(i).getValueOptr() != null && 
					!blocks.contains(getKeyValueArray(i).getValueOptr().getBlock()) &&
					!getKeyValueArray(i).getValueOptr().equals(Optr.emptyPointer) ) {
						blocks.add(getKeyValueArray(i).getKeyOptr().getBlock());
				}
			}
		}
		return blocks;
	}
 
	@Override
	public String toString() {
		return String.format("%s keys=%d tablespace=%d pageId=%s updated=%b needsRead=%b has next page=%b%n", 
				this.getClass().getName(), numKeys,tablespace,GlobalDBIO.valueOf(pageId),isUpdated(),needsRead, (nextPage != null));
	}

	/**
	 * @return the updated
	 */
	public boolean isUpdated() {
		return updated;
	}

	/**
	 * @param updated the updated to set
	 */
	public void setUpdated(boolean updated) {
		this.updated = updated;
	}
}
