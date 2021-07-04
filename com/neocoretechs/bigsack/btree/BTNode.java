package com.neocoretechs.bigsack.btree;

import java.io.IOException;
import java.util.ArrayList;

import com.neocoretechs.bigsack.io.Optr;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.keyvaluepages.KeyPageInterface;
import com.neocoretechs.bigsack.keyvaluepages.KeyValue;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
import com.neocoretechs.bigsack.keyvaluepages.NodeInterface;

/**
 * Class BTNode
 */
public class BTNode<K extends Comparable, V> implements NodeInterface<K, V> {
    public final static int MIN_DEGREE          =   (BTreeKeyPage.MAXKEYS/2)+1;
    public final static int LOWER_BOUND_KEYNUM  =   MIN_DEGREE - 1;
    public final static int UPPER_BOUND_KEYNUM  =   BTreeKeyPage.MAXKEYS;

    private boolean mIsLeaf;
    //private int mCurrentKeyNum;
    protected BTree<K,V> bTree;
    private ArrayList<KeyValue<K, V>> mKeys;
    private ArrayList<BTNode<K,V>> mChildren;
    protected long pageId = -1L;
    protected KeyPageInterface page;
    protected boolean updated = true;
    protected boolean needsRead = true;

    public BTNode(BTree<K,V> bTree, boolean mIsLeaf) {
    	this.bTree = bTree;
        this.mIsLeaf = mIsLeaf;
        //mCurrentKeyNum = 0;
        mKeys = new ArrayList<KeyValue<K, V>>();//new KeyValue[UPPER_BOUND_KEYNUM];
        mChildren = new ArrayList<BTNode<K, V>>();//new BTNode[UPPER_BOUND_KEYNUM + 1];
    }
    
    public BTNode(KeyPageInterface page) {
    	this.page = page;
        this.pageId = page.getPageId();
    }
    
	@Override
    public void setPage(KeyPageInterface page) {
    	this.page = page;
        this.pageId = page.getPageId();
    }
	
    public KeyValueMainInterface getKeyValueMain() {
    	return (KeyValueMainInterface) bTree.getKeyValueMain();
    }
 

	/**
     * ONLY USED FROM BTREEKEYPAGE TO SET UP NODE UPON RETRIEVAL, DONT USE IT ANYWHERE ELSE!!
     * @param isLeaf
     */
    public void setmIsLeaf(boolean isLeaf) {
    	mIsLeaf = isLeaf;
    }
    
    /**
     * Rare case where we delete everything and establish a new root.
     * Free all previous references by re-initializing arrays, set pageId to 0.
     */
	public void setAsNewRoot() {
		pageId = 0L;
		mIsLeaf = true;
		mKeys = new ArrayList<KeyValue<K, V>>();//new KeyValue[UPPER_BOUND_KEYNUM];
		mChildren = new ArrayList<BTNode<K, V>>();//new BTNode[UPPER_BOUND_KEYNUM + 1];
	}
	
    @Override
	public KeyValue<K, V> getKeyValueArray(int index) {
        // pre-create blank keys to receive key Ids so we can assign the page pointers and offsets
        // for subsequent retrieval
    	//if(mKeys[0] == null)
    	if(mKeys.isEmpty())
    		return null;
    	return mKeys.get(index);//mKeys[index];
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
        // pre-create blank nodes to receive page Ids when we retrieve a page, so we
        // can assign pageIds to children to retrieve subsequent nodes.
    	if(mChildren.isEmpty()) {
    		return null;
    	}
        long pageId = mChildren.get(index).pageId;//mChildren[index].pageId;
        if(pageId != -1L && mChildren.get(index).needsRead) { //mChildren[index].needsRead) {
			try {
				bTree.getKeyValueMain().getNode(mChildren.get(index), pageId);//mChildren[index], pageId);
				mChildren.get(index).needsRead = false;//mChildren[index].needsRead = false;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
        }
    	return mChildren.get(index);//mChildren[index];
    }
    
	@Override
	public NodeInterface getChildNoread(int index) {
	   	if(mChildren.isEmpty()) {
    		return null;
    	}
		return mChildren.get(index);//mChildren[index];
	}
	
    @Override
	public void setKeyValueArray(int index, KeyValue<K,V> bTKey) {
    	if(index >= mKeys.size()-1)
    		mKeys.ensureCapacity(index+1);
    	mKeys.add(index, bTKey);
    }
    
    @Override
	public void setChild(int index, NodeInterface<K,V> bTNode) {
    	if(index >= mChildren.size()-1) {
    		mChildren.ensureCapacity(index+1);
    	}
    	mChildren.add(index, (BTNode<K, V>) bTNode);
    }
    
    public boolean getIsLeaf() {
    	return mIsLeaf;
    }
    
    @Override
	public int getNumKeys() {
    	return mKeys.size();//mCurrentKeyNum;
    }
    
    @Override
	public void setNumKeys(int numKeys) {
		if(numKeys > mKeys.size())
			mKeys.ensureCapacity(numKeys);
        mKeys = new ArrayList<KeyValue<K, V>>(numKeys);
    }
    
    protected static NodeInterface getChildNodeAtIndex(BTNode btNode, int keyIdx, int nDirection) {
        if (btNode.mIsLeaf) {
            return null;
        }

        keyIdx += nDirection;
        if ((keyIdx < 0) || (keyIdx > btNode.getNumKeys())) {
            return null;
        }
        long pageId = ((BTNode) (btNode.getChildNoread(keyIdx))).pageId;
        if(pageId != -1L)
			try {
				btNode.bTree.getKeyValueMain().getNode(btNode, pageId);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
        return btNode.getChild(keyIdx);
    }

    public static NodeInterface getChildNodeAtIndex(BTNode btNode, int keyIdx) {
        if (btNode.mIsLeaf) {
            return null;
        }

        if ((keyIdx < 0) || (keyIdx > btNode.getNumKeys())) {
            return null;
        }

        return btNode.getChild(keyIdx);
    }

    protected static NodeInterface getLeftChildAtIndex(BTNode btNode, int keyIdx) {
        return getChildNodeAtIndex(btNode, keyIdx, 0);
    }


    protected static NodeInterface getRightChildAtIndex(BTNode btNode, int keyIdx) {
        return getChildNodeAtIndex(btNode, keyIdx, 1);
    }


    protected static NodeInterface getLeftSiblingAtIndex(BTNode parentNode, int keyIdx) {
        return getChildNodeAtIndex(parentNode, keyIdx, -1);
    }


    protected static NodeInterface getRightSiblingAtIndex(BTNode parentNode, int keyIdx) {
        return getChildNodeAtIndex(parentNode, keyIdx, 1);
    }
    
	@Override
	public synchronized String toString() {
		StringBuffer sb = new StringBuffer();
		//sb.append("Page ");
		//sb.append(hashCode());
		sb.append("<<<<<<<<<<BTNode Id:");
		sb.append(GlobalDBIO.valueOf(pageId));
		sb.append(" Numkeys:");
		sb.append(getNumKeys());
		sb.append(" Leaf:");
		sb.append(mIsLeaf);
		sb.append(" Children:");
		sb.append(mChildren.size());//.length);
		sb.append("\r\n");
		
		sb.append("Key/Value Array:\r\n");
		String[] sout = new String[mKeys.size()];
		for (int i = 0; i < mKeys.size() /*keyArray.length*/; i++) {
			KeyValue<K,V> keyval = getKeyValueArray(i);
			if(keyval != null) {
				try {
					if((keyval.getmKey() != null || keyval.getKeyUpdated() || !keyval.getKeyOptr().equals(Optr.emptyPointer)) ||
						(keyval.getmValue() != null || keyval.getValueUpdated() || !keyval.getValueOptr().equals(Optr.emptyPointer))) {
						sout[i] = getKeyValueArray(i).toString()+"\r\n";
					} else {
						sout[i] = null;
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else {
				sout[i] = "KEYVAL ENTRY IS NULL\r\n";
			}
		}
		boolean allEntriesDefault = true;
		for(String s: sout)
			if(s != null) allEntriesDefault = false;
		if(allEntriesDefault) {
			sb.append("ALL ENTRIES DEFAULT\r\n");
		} else {	
			for (int i = 0; i < mKeys.size() /*keyArray.length*/; i++) {
				if(sout[i] != null) {
					sb.append(i+"=");
					sb.append(getKeyValueArray(i));
					sb.append("\r\n");
				}
			}
		}
		sb.append("BTree Child Page Array:\r\n");
		String[] sout2 = new String[mChildren.size()];
		for (int i = 0 ; i < mChildren.size() /*pageArray.length*/; i++) {
				if(getChild(i) != null) {
					sout2[i] = getChild(i).toString()+"\r\n";
				} else {
					sout2[i] = null;
				}
		}
		boolean allChildrenEmpty = true;
		for(String s: sout2)
			if(s != null) allChildrenEmpty = false;
		if(allChildrenEmpty) {
			sb.append("ALL CHILDREN EMPTY\r\n");
		} else {
			int j = 0;
			for (int i = 0 ; i < mChildren.size() /*pageArray.length*/; i++) {
				if(sout2[i] != null) {
					sb.append(i+"=");
					sb.append(sout2[i]);
					if(getChild(i) == null) ++j;
				}
			}
			sb.append("Children Array null for "+j+" members\r\n");
		}
		sb.append(GlobalDBIO.valueOf(pageId));
		sb.append(" >>>>>>>>>>>>>>End ");
		sb.append("\r\n");
		return sb.toString();
	}

	@Override
	public int getTablespace() {
		if(pageId == -1)
			return -1;
		return GlobalDBIO.getTablespace(pageId);
	}

	@Override
	public void initKeyValueArray(int index) {
		//if(index >= numKeys)
			//numKeys = index+1;
		if(mKeys.get(index) == null)
			mKeys.add(index,new KeyValue<K,V>(this));
		
	}

}
