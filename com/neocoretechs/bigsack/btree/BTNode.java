package com.neocoretechs.bigsack.btree;

import java.io.IOException;
import java.util.ArrayList;

import com.neocoretechs.bigsack.hashmap.HMapKeyPage;
import com.neocoretechs.bigsack.hashmap.HTNode;
import com.neocoretechs.bigsack.io.Optr;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.keyvaluepages.KeyPageInterface;
import com.neocoretechs.bigsack.keyvaluepages.KeyValue;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
import com.neocoretechs.bigsack.keyvaluepages.NodeInterface;

/**
 * Class BTNode
 */
public class BTNode<K extends Comparable, V> extends HTNode {
    public final static int MIN_DEGREE          =   (BTreeKeyPage.MAXKEYS/2)+1;
    public final static int LOWER_BOUND_KEYNUM  =   MIN_DEGREE - 1;
    public final static int UPPER_BOUND_KEYNUM  =   BTreeKeyPage.MAXKEYS;

    private boolean mIsLeaf;
    //private int mCurrentKeyNum;
    protected BTreeNavigator<K,V> bTree;
    private KeyValue<K, V> mKeys[] = new KeyValue[BTreeKeyPage.MAXKEYS];
    private NodeInterface<K,V> mChildren[] = new NodeInterface[BTreeKeyPage.MAXKEYS+1];

    public BTNode(BTreeNavigator<K,V> bTree, Long pageId, boolean mIsLeaf) {
    	super(bTree.getKeyValueMain(), pageId);
    	this.bTree = bTree;
        this.mIsLeaf = mIsLeaf;
        //mCurrentKeyNum = 0;
    }
    
    public BTNode(BTreeNavigator<K,V> bTree, KeyPageInterface page, boolean mIsLeaf) throws IOException {
    	super(page.getKeyValueMain(), page.getPageId());
       	this.bTree = bTree;
        this.mIsLeaf = mIsLeaf;
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
		setNumKeys(0);
	    mKeys = new KeyValue[BTreeKeyPage.MAXKEYS];
	    mChildren = new NodeInterface[BTreeKeyPage.MAXKEYS+1];
	}
	
    @Override
	public KeyValue<K, V> getKeyValueArray(int index) {
        // pre-create blank keys to receive key Ids so we can assign the page pointers and offsets
        // for subsequent retrieval
    	//if(mKeys[0] == null)
    	if(getNumKeys() == 0)
    		return null;
    	return mKeys[index];
    }
    
    
    @Override
	public NodeInterface<K, V> getChild(int index) {
    	if(getNumKeys() == 0) {
    		return null;
    	}
    	BTreeRootKeyPage kpi;
        long pageId = mChildren[index].getPageId();
        if(pageId != -1L ) {
			try {
				if(mChildren[index] == null || mChildren[index].getPageId() != pageId) {
					kpi = (BTreeRootKeyPage)bTree.getKeyValueMain().getNode(mChildren[index], pageId);
					mChildren[index] = (NodeInterface<K, V>) kpi.bTNode;
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
        }
    	return mChildren[index];
    }
    
	@Override
	public NodeInterface getChildNoread(int index) {
		return mChildren[index];
	}
	
	@Override
	public void setChild(int index, NodeInterface bTNode) {
    	if(index > getNumKeys()) {
    		setNumKeys(index);
    	}
    	mChildren[index] = (BTNode<K, V>) bTNode;
    }
    
	public void setUpdated(boolean updated) {
		super.setUpdated(updated);
	}
	
	public boolean getUpdated() {
		return isUpdated();
	}
	
    public boolean getIsLeaf() {
    	return mIsLeaf;
    }
    
    @Override
	public int getNumKeys() {
    	return super.getNumKeys();
    }
    
    @Override
	public void setNumKeys(int numKeys) {
    	super.setNumKeys(numKeys);
    }
    
    protected static NodeInterface getChildNodeAtIndex(BTNode btNode, int keyIdx, int nDirection) {
        if (btNode.mIsLeaf) {
            return null;
        }
        keyIdx += nDirection;
        if ((keyIdx < 0) || (keyIdx > btNode.getNumKeys())) {
            return null;
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
		sb.append(mChildren.length);
		sb.append("\r\n");
		
		sb.append("Key/Value Array:\r\n");
		String[] sout = new String[mKeys.length];
		for (int i = 0; i < mKeys.length /*keyArray.length*/; i++) {
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
			for (int i = 0; i < mKeys.length /*keyArray.length*/; i++) {
				if(sout[i] != null) {
					sb.append(i+"=");
					sb.append(getKeyValueArray(i));
					sb.append("\r\n");
				}
			}
		}
		sb.append("BTree Child Page Array:\r\n");
		String[] sout2 = new String[mChildren.length];
		for (int i = 0 ; i < mChildren.length /*pageArray.length*/; i++) {
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
			for (int i = 0 ; i < mChildren.length /*pageArray.length*/; i++) {
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
		if(index >= getNumKeys())
			setNumKeys(index+1);	
	}

}
