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
	public static boolean DEBUG = true;
    private KeyValue<K, V> mKeys[] = new KeyValue[HMapKeyPage.MAXKEYS];
    protected long pageId = -1L;
    protected boolean updated = false;
    protected boolean needsRead = true;
    protected int tablespace = -1;
    protected KeyValueMainInterface hMapMain;
    protected KeyPageInterface page = null;
    protected HTNode<K,V> nextPage = null; // child
    private int numKeys = 0;

    public HTNode(KeyValueMainInterface hMapMain, long pageId) {
       this.hMapMain = hMapMain;
       this.pageId = pageId;
       this.tablespace = GlobalDBIO.getTablespace(pageId);
    }
    
    /**
     * setPage is called when this is constructed with a page, so if the page has data, it is loaded to this new node.
     * @param page
     * @throws IOException
     */
    public HTNode(KeyPageInterface page) throws IOException {
    	this.page = page;
    	setPage(page);
    }
    
	@Override
	/**
	 * We can use this method to set up the node on construction, or change its contents later with a new page.<p/>
	 * Conversely, if this node is presented with a blank page and has data, it will load it to the blank page.
	 */
    public void setPage(KeyPageInterface page) throws IOException {
    	this.page = page;
        this.hMapMain = page.getKeyValueMain();
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
		if(dis.available() == 0)
			return;
		numKeys = (int) dis.readLong();
		for(int i = 0; i < numKeys; i++) {
			KeyValue<Comparable,Object> keyValue = new KeyValue<Comparable,Object>((NodeInterface<Comparable, Object>) this);
			long block = dis.readLong();
			short offset = dis.readShort();
			keyValue.setKeyOptr(new Optr(block,offset));
			block = dis.readLong();
			offset = dis.readShort();
			keyValue.setValueOptr(new Optr(block,offset));
		}
		long blockNextPage = dis.readLong();
		if(blockNextPage != -1L)
			nextPage = new HTNode<K,V>(hMapMain, blockNextPage);
	}
	
    /**
	 * @return the mCurrentKeyNum
	 */
	protected int getmCurrentKeyNum() {
		return numKeys;//mCurrentKeyNum;
	}

	@Override
	public KeyValueMainInterface getKeyValueMain() {
		return hMapMain;
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
    	return mKeys[index];//mKeys[index];
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
    /**
	 * Serialize this page to deep store on a page boundary.
	 * Key pages are always on page boundaries. The data is written
	 * to the page buffers from the updated node values via the {@link BTreeKeyPage} facade.
	 * The push to deep store takes place at commit time or
	 * when the buffer fills and it becomes necessary to open a spot.
	 * @exception IOException If write fails
	 */
	public synchronized void putNodeToPage(KeyPageInterface page) throws IOException {
		if(!updated) {
			if(DEBUG)
				System.out.printf("%s.putPage page NOT updated:%s%n",this.getClass().getName(),this);
			return;
			//throw new IOException("KeyPageInterface.putPage page NOT updated:"+this);
		}
		if( DEBUG ) 
			System.out.printf("%s.putPage:%s%n",this.getClass().getName(),this);
		// hold accumulated insert pages
		ArrayList<Long> currentPayloadBlocks = aggregatePayloadBlocks() ;
		//.map(Map::values)                  // -> Stream<List<List<String>>>
		//.flatMap(Collection::stream)       // -> Stream<List<String>>
		//.flatMap(Collection::stream)       // -> Stream<String>
		//.collect(Collectors.toSet())       // -> Set<String>
		// Persist each key that is updated to fill the keyIds in the current page
		// Once this is complete we write the page contiguously
		// Write the object serialized keys out to deep store, we want to do this out of band of writing key page
		for(int i = 0; i < HMapKeyPage.MAXKEYS; i++) {
			if( getKeyValueArray(i) != null ) {
				if(getKeyValueArray(i).getKeyUpdated() ) {
					// put the key to a block via serialization and assign KeyIdArray the position of stored key
					page.putKey(i, currentPayloadBlocks);	
				}
				if(getKeyValueArray(i).getValueUpdated()) {
					page.putData(i, currentPayloadBlocks);
				}
			}
		}
		//
		assert (page.getBlockAccessIndex().getBlockNum() != -1L) : " KeyPageInterface unlinked from page pool:"+this;
		// write the page to the current block
		page.getBlockAccessIndex().setByteindex((short) 0);
		// Write to the block output stream
		DataOutputStream bs = GlobalDBIO.getBlockOutputStream(page.getBlockAccessIndex());
		if( DEBUG )
			System.out.println("KeyPageInterface.putPage BlockStream:"+page);
		bs.writeLong(getNumKeys());
		for(int i = 0; i < getNumKeys(); i++) {
			if(getKeyValueArray(i) != null && getKeyValueArray(i).getKeyUpdated() ) { // if set, key was processed by putKey[i]
				bs.writeLong(getKeyValueArray(i).getKeyOptr().getBlock());
				bs.writeShort(getKeyValueArray(i).getKeyOptr().getOffset());
				getKeyValueArray(i).setKeyUpdated(false);
			} else { // skip 
				page.getBlockAccessIndex().setByteindex((short) (page.getBlockAccessIndex().getByteindex()+10));
			}
			// data array
			if(getKeyValueArray(i) != null &&  getKeyValueArray(i).getValueUpdated() ) {
				bs.writeLong(getKeyValueArray(i).getValueOptr().getBlock());
				bs.writeShort(getKeyValueArray(i).getValueOptr().getOffset());
				getKeyValueArray(i).setValueUpdated(false);
			} else {
				// skip the data Id for this index as it was not updated, so no need to write anything
				page.getBlockAccessIndex().setByteindex((short) (page.getBlockAccessIndex().getByteindex()+10));
			}
		}
		// persist next page
		if(nextPage != null)
				bs.writeLong(nextPage.getPageId());
			else
				bs.writeLong(-1L); // empty page
		bs.flush();
		bs.close();
		if( DEBUG ) {
			System.out.println("KeyPageInterface.putPage Added Keypage @:"+this);
		}
		updated = false;
	}

	@Override
	public String toString() {
		return String.format("%s keys=%d tablespace=%d pageId=%s updated=%b needsRead=%b has next page=%b%n", 
				this.getClass().getName(), numKeys,tablespace,GlobalDBIO.valueOf(pageId),updated,needsRead, (nextPage != null));
	}
}
