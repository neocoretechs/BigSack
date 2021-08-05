package com.neocoretechs.bigsack.hashmap;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.Stream.Builder;

import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.stream.PageIteratorIF;
import com.neocoretechs.bigsack.keyvaluepages.ChildRootKeyPageInterface;
import com.neocoretechs.bigsack.keyvaluepages.KVIteratorIF;
import com.neocoretechs.bigsack.keyvaluepages.KeyPageInterface;
import com.neocoretechs.bigsack.keyvaluepages.KeySearchResult;
import com.neocoretechs.bigsack.keyvaluepages.KeyValue;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
import com.neocoretechs.bigsack.keyvaluepages.RootKeyPageInterface;
/**
 * The {@link KeyPageInterface} {@link HMapKeyPage} instance containing the {@link BlockAccessIndex}
 * is the bottom collision space page with actual k/v entries of {@link KeyValue} in the {@link HTNode} instance.
 * The haskey is a 32 bit integer which includes the sign bit, so for our keyspace fanout the tablespaces 
 * comprise the first 3 bits of the key, the rootPage field has initial page with 2 bit keys, 
 * then the childPages array has the 3 pages with 9 bit LSB keys.<p/>
 * The page layout is as follows:<br/>
 * tablespace = hashkeys[0] 3 bits <br/>
 * rootKeys[0-MAXKEYSROOT] ->  rootpage = hashkeys[1] 2 bits <br/>
 * [childPages[0].hashkeys[2]].pageId -> childPages[1] =  hashkeys[2] 9 bits <br/>
 * [childPages[1].hashkeys[3]].pageId -> childPages[2] =  hashkeys[3] 9 bits <br/>
 * [childPages[2].hashkeys[4]].pageId -> keyvaluespage =  hashkeys[4] 9 bits <br/>
 * key/values page.nextPage -> linked list of collision space key pages<br/>
 * @author groff
 *
 */
public class HMapNavigator {
	public static boolean DEBUG = false;
	protected KeyPageInterface keyValuesPage = null;
	protected ChildRootKeyPageInterface[] childPage = new ChildRootKeyPageInterface[3]; //root key + 3 - 9 bit capacity pages for HKey index beyond initial 2 root bits and 3 tablespace bits
	protected RootKeyPageInterface rootPage = null;
	private KeyValueMainInterface hMapMain;
	private Comparable targetKey;
	private int[] hashKeys;
	
	public HMapNavigator(KeyValueMainInterface hMapMain, Comparable targetKey) {
		this.hMapMain = hMapMain;
		this.targetKey = targetKey;
		this.hashKeys = HashKey.computeKey(targetKey.hashCode());
		if(DEBUG) 
			System.out.printf("%s HashKeys: %s for key:%s%n",this.getClass().getName(), Arrays.toString(hashKeys), targetKey);
	}
	
	public HMapNavigator(KeyValueMainInterface hMapMain) {
		this.hMapMain = hMapMain;
		if(DEBUG) 
			System.out.printf("%s constructor%n",this.getClass().getName());
	}
	/**
	 * 
	 * @return the array of indexes for the target key
	 */
	public int[] getHashKeys() {
		return hashKeys;
	}
	/**
	* PageIteratorIF<RootKeyPageInterface> iterxImpl = new PageIteratorIF<RootKeyPageInterface>() {
	*	@Override
	*	public void item(RootKeyPageInterface page) {
	*	}
	* };
	 * @param kvNode
	 * @param iterImpl
	 * @return
	 * @throws IOException
	 */
	 public static void retrievePagesInOrder(KeyValueMainInterface hMapMain, RootKeyPageInterface rootPage, PageIteratorIF<KeyPageInterface> iterImpl) throws IOException {
		 // keypage pointed to by 3rd and last of 9 bit key, which contains node to retrieve collision space items that all share same hashcode
		 PageIteratorIF<RootKeyPageInterface> childIterImpl2 = new PageIteratorIF<RootKeyPageInterface>() {
				@Override
				public void item(RootKeyPageInterface page) throws IOException {
					HMapKeyPage nPage = (HMapKeyPage) page;
					while(nPage != null ) {
						iterImpl.item(nPage); // call back the passed in operator
						nPage = (HMapKeyPage) nPage.nextPage;
					}
				}
		 };
		 // retrieve page for second of the 3 9 bit spaces hashkey[3]
		 PageIteratorIF<RootKeyPageInterface> childIterImpl1 = new PageIteratorIF<RootKeyPageInterface>() {
				@Override
				public void item(RootKeyPageInterface page) throws IOException {
					for(int i = 0; i < page.getNumKeys(); i++) {
						if(page.getPageId(i) != -1L)
							childIterImpl2.item(hMapMain.getIO().getHMapPageFromPool(page.getPageId(i)));
					}
				}
		 };
		 // retrieve page for each of first 9 bit key spaces hashkey[2]
		 PageIteratorIF<RootKeyPageInterface> childIterImpl0 = new PageIteratorIF<RootKeyPageInterface>() {
					@Override
					public void item(RootKeyPageInterface page) throws IOException {
						for(int i = 0; i < page.getNumKeys(); i++) {
							if(page.getPageId(i) != -1L)
								childIterImpl1.item(hMapMain.getIO().getHMapChildRootPageFromPool(page.getPageId(i)));
						}
					}
		 };
		 // retrieve each page of the root indexes for this tablespace, hashkey[1], 2 bits
		 for(int i = 0; i < rootPage.getNumKeys(); i++) {
				if(rootPage.getPageId(i) != -1L ) {				 
					childIterImpl0.item(hMapMain.getIO().getHMapChildRootPageFromPool(rootPage.getPageId(i)));
				}
		 }
	 }
	 /**
	  * Return the first page that contains valid key/value data, which will be the lowest value hash key entry.
	  * Iterate the root pages until the first valid root page entry, then pass that to the method.
	  * @param hMapMain
	  * @param rootPage
	  * @return KeyPageInterface for the first page unless no pages are active in the root page for this tablespace
	  * @throws IOException
	  */
	 public KeyPageInterface firstPage(RootKeyPageInterface rootPage) throws IOException {
		 // keypage pointed to by 3rd and last of 9 bit key, which contains node to retrieve collision space items that all share same hashcode
		 PageSearchIF<RootKeyPageInterface> childIterImpl3 = new PageSearchIF<RootKeyPageInterface>() {
				@Override
				public KeyPageInterface item(RootKeyPageInterface page) throws IOException {
					for(int i = 0; i < page.getNumKeys(); i++) {
						if(page.getPageId(i) != -1L) {
							hashKeys[4] = i;
							keyValuesPage = hMapMain.getIO().getHMapPageFromPool(page.getPageId(i));
							return keyValuesPage;
						}
					}
					return null;
				}
		 };
		 // retrieve page for second of the 3 9 bit spaces hashkey[3]
		 PageSearchIF<RootKeyPageInterface> childIterImpl1 = new PageSearchIF<RootKeyPageInterface>() {
				@Override
				public KeyPageInterface item(RootKeyPageInterface page) throws IOException {
					for(int i = 0; i < page.getNumKeys(); i++) {
						if(page.getPageId(i) != -1L) {
							hashKeys[3] = i;
							childPage[2] = hMapMain.getIO().getHMapChildRootPageFromPool(page.getPageId(i));
							return childIterImpl3.item(childPage[2]);
						}
					}
					return null;
				}
		 };
		 // retrieve page for each of first 9 bit key spaces hashkey[2]
		 PageSearchIF<RootKeyPageInterface> childIterImpl0 = new PageSearchIF<RootKeyPageInterface>() {
					@Override
					public KeyPageInterface item(RootKeyPageInterface page) throws IOException {
						for(int i = 0; i < page.getNumKeys(); i++) {
							if(page.getPageId(i) != -1L) {
								hashKeys[2] = i;
								childPage[1] = hMapMain.getIO().getHMapChildRootPageFromPool(page.getPageId(i));
								return childIterImpl1.item(childPage[1]);
							}
						}
						return null;
					}
		 };
		 hashKeys = new int[5];
		 for(int i = 0; i < hashKeys.length; i++) {
			 hashKeys[i] = -1;
		 }
		 // retrieve each page of the root indexes for this tablespace, hashkey[1], 2 bits
		 for(int i = 0; i < rootPage.getNumKeys(); i++) {
				if(rootPage.getPageId(i) != -1L ) {
					hashKeys[1] = i;
					childPage[0] = hMapMain.getIO().getHMapChildRootPageFromPool(rootPage.getPageId(i));
					return childIterImpl0.item(childPage[0]);
				}
		 }
		 return null;
	 }
	/**
	 * Return the next page in the sequence established by firstPage for the particular root page in the tablespace
	 * @param hMapMain
	 * @param rootPage
	 * @return KeyPageInterface of next page or null if the tablespace is exhausted
	 * @throws IOException
	 */
	 public KeyPageInterface nextPage() throws IOException {
		 // keypage pointed to by 3rd and last of 9 bit key, which contains node to retrieve collision space items that all share same hashcode
		 // retrieve each page of the root indexes for this tablespace, hashkey[1], 2 bits
		 for(int i = hashKeys[4]+1; i < childPage[2].getNumKeys(); i++) {
				if(childPage[2].getPageId(i) != -1L ) {
					hashKeys[4] = i;
					keyValuesPage = hMapMain.getIO().getHMapPageFromPool(childPage[2].getPageId(hashKeys[4]));
					return keyValuesPage;
				}
		 }
		 for(int i = hashKeys[3]+1; i < childPage[1].getNumKeys(); i++) {
				if(childPage[1].getPageId(i) != -1L ) {
					hashKeys[3] = i;
					childPage[2] = hMapMain.getIO().getHMapChildRootPageFromPool(childPage[1].getPageId(hashKeys[3]));
					hashKeys[4] = 0;
					for(int j = hashKeys[4]; j < childPage[2].getNumKeys(); j++) {
							if(childPage[2].getPageId(j) != -1L ) {
								hashKeys[4] = j;
								keyValuesPage = hMapMain.getIO().getHMapPageFromPool(childPage[2].getPageId(hashKeys[4]));
								return keyValuesPage;
							}
					}
				}
		 }
		 for(int i = hashKeys[2]+1; i < childPage[0].getNumKeys(); i++) {
				if(childPage[0].getPageId(i) != -1L ) {
					hashKeys[2] = i;
					childPage[1] = hMapMain.getIO().getHMapChildRootPageFromPool(childPage[0].getPageId(hashKeys[2]));
					hashKeys[3] = 0;
					for(int j = hashKeys[3]; j < childPage[1].getNumKeys(); j++) {
						if(childPage[1].getPageId(j) != -1L ) {
							hashKeys[3] = j;
							childPage[2] = hMapMain.getIO().getHMapChildRootPageFromPool(childPage[1].getPageId(hashKeys[3]));
							hashKeys[4] = 0;
							for(int k = hashKeys[4]; k < childPage[2].getNumKeys(); k++) {
									if(childPage[2].getPageId(k) != -1L ) {
										hashKeys[4] = k;
										keyValuesPage = hMapMain.getIO().getHMapPageFromPool(childPage[2].getPageId(hashKeys[4]));
										return keyValuesPage;
									}
							}
						}
					}
				}
		 }
		 return null;
	 }
	 /**
	  * Stream oriented
	  * @param hMapMain
	  * @param rootPage
	  * @param limit
	  * @return
	  * @throws IOException
	  */
	 public static Stream<KeyValue<Comparable,Object>> retrieveEntriesInOrder(KeyValueMainInterface hMapMain, RootKeyPageInterface rootPage, int limit) throws IOException {
		 Supplier<KeyValue<Comparable,Object>> b = null;
		 // keypage pointed to by 3rd and last of 9 bit key, which contains node to retrieve collision space items that all share same hashcode
		 PageStreamIF<RootKeyPageInterface> childIterImpl3 = new PageStreamIF<RootKeyPageInterface>() {
				@Override
				public int item(RootKeyPageInterface page, int count, int limit) throws IOException {
					for(int i = 0; i < page.getNumKeys(); i++) {
						KeyPageInterface kpi = (KeyPageInterface) page.getPage(i);
						int num = kpi.retrieveEntriesInOrder(b, count, limit); // retrieve collision space linked pages
						count += num;
						if( count >= limit )
							break;
					}
					return count;
				}
		 };
		 // retrieve page for third and last of the 3 9 bit spaces hashkey[4]
		 PageStreamIF<RootKeyPageInterface> childIterImpl2 = new PageStreamIF<RootKeyPageInterface>() {
				@Override
				public int item(RootKeyPageInterface page, int count, int limit) throws IOException {
					for(int i = 0; i < page.getNumKeys(); i++) {
						if(page.getPageId(i) != -1L) {
							count += childIterImpl3.item(hMapMain.getIO().getHMapChildRootPageFromPool(page.getPageId(i)), count, limit);
							if( limit != -1 || count >= limit )
								break;
						}
					}
					return count;
				}
		 };
		 // retrieve page for second of the 3 9 bit spaces hashkey[3]
		 PageStreamIF<RootKeyPageInterface> childIterImpl1 = new PageStreamIF<RootKeyPageInterface>() {
				@Override
				public int item(RootKeyPageInterface page, int count, int limit) throws IOException {
					for(int i = 0; i < page.getNumKeys(); i++) {
						if(page.getPageId(i) != -1L) {
							count += childIterImpl2.item(hMapMain.getIO().getHMapChildRootPageFromPool(page.getPageId(i)), count, limit);
							if( limit != -1 || count >= limit )
								break;
						}
					}
					return count;
				}
		 };
		 // retrieve page for each of first 9 bit key spaces hashkey[2]
		 PageStreamIF<RootKeyPageInterface> childIterImpl0 = new PageStreamIF<RootKeyPageInterface>() {
					@Override
					public int item(RootKeyPageInterface page, int count, int limit) throws IOException {
						for(int i = 0; i < page.getNumKeys(); i++) {
							if(page.getPageId(i) != -1L) {
								count += childIterImpl1.item(hMapMain.getIO().getHMapChildRootPageFromPool(page.getPageId(i)), count, limit);
								if( limit != -1 || count >= limit )
									break;
							}
						}
						return count;
					}
		 };
		 // retrieve each page of the root indexes for this tablespace, hashkey[1], 2 bits
		 int count = 0;
	
		 for(int i = 0; i < rootPage.getNumKeys(); i++) {
				if(rootPage.getPageId(i) != -1L ) {				 
					count += childIterImpl0.item(hMapMain.getIO().getHMapChildRootPageFromPool(rootPage.getPageId(i)), count, limit);
					if( limit != -1 || count >= limit )
						break;
				}
		 }
		 
		 return Stream.generate(b);
	 }
	/**
	 * Create a keypath using available keys or if none exist, create them and populate the key pages.
	 * Once we have a HMapKeyPage we can use the superclass search method to search the collision space.<p/>
	 * If we dont have a top-level pointer, there is nothing below that for any of the pointers below that, so
	 * we check each level of the hierarchy progressively.
	 * tablespace = hashkeys[0] <br/>
	 * root key pages number page -> hashkeys[0] 3 bits tablespace <br/>
	 * child page 0 = hashkeys[1] of page root, 2 bits<br/>
	 * child page 1 = hashkeys[2] of child page 0, 9 bits<br/>
	 * child page 2 = hashkeys[3] of child page 1, 9 bits<br/>
	 * keyvaluepage = hashkeys[4] of child page 2, 9 bits<br/>
	 * @param targetKey The key from which the hashkeys were formed
	 * @param hashkeys The array of values from computeHashKeys that indicate our path through the keyspace
	 * @param bufferPage the key/values page at the bottom of the hierarchy, if our path exists fully, we add to this page. Its the 'last insert page' if it exists, null otherwise.
	 * @return The final HMapKeyPage representing the collision space of actual keys, if the path existed, its the bufferPage we passed in.
	 * @throws IOException
	 */
	public synchronized HMapKeyPage createKeypath(RootKeyPageInterface rootPage) throws IOException {
		//if(DEBUG) {
		//System.out.printf("%s.createKeypath(%s,%s,%s) rootKeys[hashkeys[1]]=%s%n",this.getClass().getName(),targetKey,Arrays.toString(hashkeys),bufferPage,GlobalDBIO.valueOf(rootKeys[hashkeys[1]]));
		//System.out.printf("%s.createKeypath(%s,%s,%s) rootpage=%s%n",this.getClass().getName(),targetKey,Arrays.toString(hashkeys),bufferPage,this);
		//System.out.printf("%s.createKeypath(%s,%s,%s) childPage[0]=%s%n",this.getClass().getName(),targetKey,Arrays.toString(hashkeys),bufferPage,childPages[0]);
		//}
		// check to see if root page (presumed loaded initially) has valid pointer
		KeySearchResult ksr = search(rootPage);
		if(!ksr.atKey) {
			switch(ksr.insertPoint) {
				case 1: // hash key index 1
					childPage[0] = hMapMain.getIO().getHMapChildRootPageFromPool(-1L);
					rootPage.setPageIdArray(hashKeys[1], childPage[0].getPageId(), true);
					rootPage.putPage();
				case 2: // index 2, child Page set from search  or above
					childPage[1] = hMapMain.getIO().getHMapChildRootPageFromPool(-1L);
					childPage[0].setPageIdArray(hashKeys[2], childPage[1].getPageId(), true);
					childPage[0].putPage();
				case 3: // index 3
					childPage[2] = hMapMain.getIO().getHMapChildRootPageFromPool(-1L);
					childPage[1].setPageIdArray(hashKeys[3], childPage[2].getPageId(), true);
					childPage[1].putPage();
				case 4: // index 4
					keyValuesPage = hMapMain.getIO().getHMapPageFromPool(-1L);
					childPage[2].setPageIdArray(hashKeys[4], keyValuesPage.getPageId(), true);
					childPage[2].putPage();
				default:
					break;
			}
		}
		//if(DEBUG)
			//System.out.printf("%s.createKeypath returning keyValuesPage HTNode=%s%n",this.getClass().getName(),((HMapKeyPage)keyValuesPage).hTNode);
		return (HMapKeyPage) keyValuesPage;
	}
	

	/**
	 * Primary goal is to set up pages in root and child to acquire pointers
	 */
	synchronized KeySearchResult search() throws IOException {
		RootKeyPageInterface rootPage = hMapMain.getIO().getHMapRootPageFromPool(hashKeys[0]);
		return search(rootPage);
	}
	
	synchronized KeySearchResult search(RootKeyPageInterface rootPage) throws IOException {
		if(rootPage.getPageId(hashKeys[1]) == -1L) {	
			return new KeySearchResult(1, false);
		} else {
			childPage[0] = hMapMain.getIO().getHMapChildRootPageFromPool(rootPage.getPageId(hashKeys[1]));
			for(int keyNum = 1; keyNum < 3; keyNum++) {
					if(childPage[keyNum-1].getPageId(hashKeys[keyNum+1]) == -1L) {
						KeySearchResult ksr = new KeySearchResult(keyNum+1, false);
						if(DEBUG)
							System.out.printf("%s.search returning keysearchresult=%s%n", this.getClass().getName(),ksr);
						return ksr;
					} else {
						childPage[keyNum] = hMapMain.getIO().getHMapChildRootPageFromPool(childPage[keyNum-1].getPageId(hashKeys[keyNum+1]));
					}
			}
			if(DEBUG) {
				System.out.printf("%s.search root=%s%n", this.getClass().getName(),rootPage);
				for(int i = 0; i < childPage.length; i++)
					System.out.printf("%s.search childPage[%d]=%s%n",this.getClass().getName(),i,childPage[i]);
			}
			if(childPage[2].getPageId(hashKeys[4]) == -1L) {
				KeySearchResult ksr = new KeySearchResult(4, false);
				if(DEBUG)
					System.out.printf("%s.search returning keysearchresult=%s%n", this.getClass().getName(),ksr);
				return ksr;
			} 
			keyValuesPage = hMapMain.getIO().getHMapPageFromPool(childPage[2].getPageId(hashKeys[4]));
			return new KeySearchResult(keyValuesPage, 5, true);
		}
	}

}
