package com.neocoretechs.bigsack.btree;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;

import com.neocoretechs.bigsack.hashmap.HMapKeyPage;
import com.neocoretechs.bigsack.hashmap.HTNode;
import com.neocoretechs.bigsack.io.Optr;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.keyvaluepages.KeyPageInterface;
import com.neocoretechs.bigsack.keyvaluepages.KeyValue;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
import com.neocoretechs.bigsack.keyvaluepages.NodeInterface;

/**
 * BTNode is derived from the HMap node structure and includes the child of each key organized
 * as the left child at the same index as the key and the right child at index+1 of the key.
 * The other addition is designation as leaf or non-leaf.the {@link KeyValue} structure is shared
 * as is the contract for {@link KeyPageInterface}. {@link HTNode}.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 */
public class BTNode<K extends Comparable, V> extends HTNode {
    public final static int MIN_DEGREE          =   (BTreeKeyPage.MAXKEYS/2)+1;
    public final static int LOWER_BOUND_KEYNUM  =   MIN_DEGREE - 1;
    public final static int UPPER_BOUND_KEYNUM  =   BTreeKeyPage.MAXKEYS;

    private boolean mIsLeaf;
    //private int mCurrentKeyNum;
    protected BTreeNavigator<K,V> bTree;
    private NodeInterface<K,V> mChildren[] = new NodeInterface[BTreeKeyPage.MAXKEYS+1];
    Long childPages[] = new Long[BTreeKeyPage.MAXKEYS+1];

    public BTNode(BTreeNavigator<K,V> bTree, Long pageId, boolean mIsLeaf) throws IOException {
    	super(bTree.getKeyValueMain(), pageId);
    	this.bTree = bTree;
        this.mIsLeaf = mIsLeaf;
        //mCurrentKeyNum = 0;
        setPage(new BTreeRootKeyPage(bTree.getKeyValueMain(), bTree.getKeyValueMain().getIO().findOrAddBlock(pageId), true));
    }
    
    public BTNode(BTreeNavigator<K,V> bTree, KeyPageInterface page, boolean mIsLeaf) throws IOException {
    	super(page.getKeyValueMain(), page.getPageId());
       	this.bTree = bTree;
       	this.page = page;
        this.mIsLeaf = mIsLeaf;
    }
    
    /**
     * setPage is called when this is constructed with a page, so if the page has data, it is loaded to this new node.
     * @param page
     * @throws IOException
     */
    public BTNode(KeyPageInterface page) throws IOException {
    	super(page.getKeyValueMain(), page.getPageId());
    	setPage(page);
    }
    
	@Override
	/**
	 * We can use this method to set up the node on construction, or change its contents later with a new page.<p/>
	 * Conversely, if this node is presented with a blank page and has data, it will load it to the blank page.
	 */
    public void setPage(KeyPageInterface page) throws IOException {
    	this.page = page;
        if(page.getNumKeys() > 0) { // if page has data, load it to this
        	loadNode();
        } else {
        	if(getNumKeys() > 0) { // page has no data, but if this node has data, load it to the page
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
		for(int i = 0; i < getNumKeys(); i++) {
			getKeyValueArray(i).setKeyUpdated(true);
		}
		((BTreeKeyPage)page).putPage();
	}
	/**
	 * Set up to Load this node with data from the page.<p/> 
	 * @param page
	 * @throws IOException
	 */
	private void loadNode() throws IOException {
		if(getNumKeys() > 0)
			throw new IOException("Attempt to overwrite node "+this+" with page "+page);
		page.readFromDBStream(GlobalDBIO.getBlockInputStream(page.getBlockAccessIndex()));
	}
	
	
    public KeyValueMainInterface getKeyValueMain() {
    	return (KeyValueMainInterface) bTree.getKeyValueMain();
    }
    
	@Override
	public void initKeyValueArray(int index) {
		super.initKeyValueArray(index);
		childPages[index] = -1L;
		childPages[index+1] = -1L;
		mChildren[index] = null;
		mChildren[index+1] = null;
	}
	/**
     * ONLY USED FROM BTREEKEYPAGE TO SET UP NODE UPON RETRIEVAL, DONT USE IT ANYWHERE ELSE!!
     * @param isLeaf
     */
    public void setmIsLeaf(boolean isLeaf) {
    	mIsLeaf = isLeaf;
    	setUpdated(true);
    }
    
    public KeyPageInterface getPage() { 
    	return page; 
    }
    
    /**
     * Rare case where we delete everything and establish a new root.
     * Free all previous references by re-initializing arrays, set pageId to 0.
     */
	public void setAsNewRoot() {
		pageId = 0L;
		mIsLeaf = true;
		for(int i = 0; i < getNumKeys(); i++)
			setKeyValueArray(i, null);
		setNumKeys(0);
	    mChildren = new NodeInterface[BTreeKeyPage.MAXKEYS+1];
	    childPages = new Long[BTreeKeyPage.MAXKEYS+1];
	    setUpdated(true);
	}
	
    
    @Override
	public NodeInterface<K, V> getChild(int index) {
    	if(getNumKeys() == 0) {
    		return null;
    	}
    	BTreeKeyPage kpi;
        if(childPages[index] != -1L ) {
			try {
				if(mChildren[index] == null) {
					BlockAccessIndex bai = bTree.getKeyValueMain().getIO().findOrAddBlock(pageId);
					kpi = new BTreeKeyPage(bTree.getKeyValueMain(), bai, true);
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
    	childPages[index] = mChildren[index].getPageId();
    	setUpdated(true);
    }
	
	public boolean getUpdated() {
		return isUpdated();
	}
	
    public boolean getIsLeaf() {
    	return mIsLeaf;
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
		String[] sout = new String[getNumKeys()];
		for (int i = 0; i < getNumKeys() /*keyArray.length*/; i++) {
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
			for (int i = 0; i < getNumKeys() /*keyArray.length*/; i++) {
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


}
