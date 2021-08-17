package com.neocoretechs.bigsack.test;

import java.util.Iterator;
import java.util.Map.Entry;

import com.neocoretechs.bigsack.iterator.KeyValuePair;
import com.neocoretechs.bigsack.session.BigSackAdapter;
import com.neocoretechs.bigsack.session.BufferedTreeMap;
import com.neocoretechs.bigsack.session.SessionManager;
/**
 * This simple test battery tests the BufferedTreeMap and uses small to medium string K/V pairs
 * with insertion, deletion and retrieval.
 * Parameters: Set the database name as the first argument "/users/you/TestDB1" where
 * the directory  "/users/you" must exist and a series of tablespaces and a log directory
 * are created under that. The database files will be named "TestDB1" under "/users/you/log and 
 * /users/you/tablespace0" to "/users/you/tablespace7".
 * Set the name of the properties file in the VM -DBigSack.properties="/users/you/Relatrix/BigSack.properties"
 * The static constant fields in the class control the key generation for the tests
 * In general, the keys and values are formatted according to uniqKeyFmt to produce
 * a series of canonically correct sort order strings for the DB in the range of min to max vals.
 * In general most of the battery1 testing relies on checking order against expected values hence the importance of
 * canonical ordering in the sample strings.
 * Of course, you can substitute any class for the Strings here providing its Comparable 
 * @author jg
 *
 */
public class BatteryBigSack2 {
	static String key = "This is a test"; // holds the base random key string for tests
	static String val = "Of a BigSack K/V pair!"; // holds base random value string
	static String uniqKeyFmt = "%0100d"; // base + counter formatted with this gives equal length strings for canonical ordering
	static int min = 0; // controls range of testing
	static int max = 100000;
	static int numDelete = 100; // for delete test
	static int l3CacheSize = 100; // size of object cache
	/**
	* Analysis test fixture, pass supplemental method payloads on cmdl.
	*/
	public static void main(String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		if (argv.length < 1) {
			 System.out.println("usage: java BatteryBigSack2 <database>");
			System.exit(1);
		}
		BigSackAdapter.setTableSpaceDir(argv[0]);
		BufferedTreeMap session = BigSackAdapter.getBigSackTreeMap(key.getClass());
		 System.out.println("Begin Battery Fire!");
		 // add min to max
		battery1(session, argv);
		// get and verify min to max
		//battery1A(session, argv);
		 // get by value min to max
		//battery1A0(session, argv);
		// count
		//battery1A1(session, argv);
		// first last
		//battery1B(session, argv);
		// keyset, entryset
		//battery1C(session, argv);
		// from/to range
		//battery1D(session, argv);
		// from/to range
		//battery1D1(session, argv);
		// compare to synthetic key
		//battery1E(session, argv);
		//battery1E1(session, argv);
		//battery1F(session, argv);
		//battery1F1(session, argv);
		// overwrite
		//battery1G(session, argv);
		// deletion tests below
		//battery2(session, argv);
		//battery2A(session, argv);
		
		 System.out.println("TEST BATTERY COMPLETE. "+(System.currentTimeMillis()-tims)+" ms.");
		 System.exit(0);
		
	}
	/**
	 * Loads up on key/value pairs
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1(BufferedTreeMap session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		for(int i = max; i > min; i--) {
			session.put(key + String.format(uniqKeyFmt, i), val+String.format(uniqKeyFmt, i));
			if(i%(max/100) == 0) {
				System.out.println("Current index "+i+" 100 added in "+(System.currentTimeMillis()-tims)+"ms.");
				tims = System.currentTimeMillis();
			}
		}
		 System.out.println("BATTERY1 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * Does a simple 'get' of the elements inserted before
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1A(BufferedTreeMap session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		for(int i = min; i < max; i++) {
			Object o = session.get(key + String.format(uniqKeyFmt, i));
			if( !(val+String.format(uniqKeyFmt, i)).equals(o) ) {
				 System.out.println("BATTERY1A FAIL "+o);
				throw new Exception("B1A Fail on get "+i+" with "+o);
			}
		}
		 System.out.println("BATTERY1A SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * Get by value
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1A0(BufferedTreeMap session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		for(int i = min; i < max; i++) {
			Object o = session.getValue(val + String.format(uniqKeyFmt, i));
			if( !(val+String.format(uniqKeyFmt, i)).equals(o) ) {
				 System.out.println("BATTERY1A0 FAIL "+o);
				throw new Exception("B1A0 Fail on get "+i+" with "+o);
			}
		}
		 System.out.println("BATTERY1A0 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * Does a simple count of elements, compare to max
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1A1(BufferedTreeMap session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		
			int o = (int) session.size();
			if( o != max) {
				System.out.println("BATTERY1A1 FAIL count should be "+max+" but came back "+o);
				throw new Exception("BATTERY1A1 FAIL count should be "+max+" but came back "+o);
			}
		 System.out.println("BATTERY1A1 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * See if first/last key/val works
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1B(BufferedTreeMap session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		String f = (String) session.first();
		String l = (String) session.last();
		
		String minval = val + String.format(uniqKeyFmt, min);
		String maxval = val + String.format(uniqKeyFmt, (max-1));
		
		if( !f.equals(minval) || !l.equals(maxval) ) { // max-1 cause we looped it in
				 System.out.println("BATTERY1B FAIL "+f+" -- "+l+" supposed to be "+minval+" -- "+maxval);
				throw new Exception("B1B Fail on Value get with "+f+" -- "+l+" supposed to be "+minval+" -- "+maxval);
		}
		
		String fk = (String) session.firstKey();
		System.out.println("B1B First Key="+fk);
		String lk = (String) session.lastKey();
		System.out.println("B1B Last Key="+lk);
		minval = key + String.format(uniqKeyFmt, min);
		maxval = key + String.format(uniqKeyFmt, (max-1));
		if( !(fk.equals(minval)) || !(lk.equals(maxval)) ) { // looped in so max-1
			 System.out.println("BATTERY1B FAIL "+fk+" -- "+lk+" supposed to be "+minval+" -- "+maxval);
			throw new Exception("B1B Fail on Key get with "+fk+" -- "+lk+" supposed to be "+minval+" -- "+maxval);
		}
		 System.out.println("BATTERY1B SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	
	}
	/**
	 * Hammers on keyset and entryset
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1C(BufferedTreeMap session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		Iterator<?> itk = session.keySet();
		Iterator<?> ite = session.entrySet();
		int ctr = 0;
		while(itk.hasNext()) {
			Object f = itk.next();
			Object nl = ite.next();
			Entry<String,String> le = (Entry<String,String>)nl;
			String l = le.getValue();
			String nkey = key + String.format(uniqKeyFmt, ctr);
			String nval = val + String.format(uniqKeyFmt, ctr);
			if( !f.equals(nkey) || !l.equals(nval)) {
				 System.out.println("BATTERY1C FAIL "+f+" -- "+l+" "+ctr);
				 System.out.println("BATTERY1C FAIL counter reached "+ctr+" not "+max);
				throw new Exception("B1C Fail on get with "+f+" -- "+l+" "+ctr);
			}
			++ctr;
		}
		if( ctr != max ) {
			 System.out.println("BATTERY1C FAIL counter reached "+ctr+" not "+max);
			throw new Exception("B1C FAIL counter reached "+ctr+" not "+max);
		}
		 System.out.println("BATTERY1C SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * headset returns values strictly less than 'to' element
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1D(BufferedTreeMap session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		// headset strictly less than 'to' element
		Iterator<?> itk = session.headMap(key+(max)); // set is strictly less than 'to' element so we use max val
		int ctr = 0;
		while(itk.hasNext()) {
			Object f = itk.next();
			String nval = key + String.format(uniqKeyFmt, ctr);
			if( !f.equals(nval) ) {
				 System.out.println("BATTERY1D FAIL "+f+" -- "+nval);
				 System.out.println("BATTERY1D FAIL counter reached "+ctr+" not "+max);
				throw new Exception("B1D Fail on get with "+f+" -- "+nval);
			}
			++ctr;
		}
		if( ctr != max ) {
			 System.out.println("BATTERY1D FAIL counter reached "+ctr+" not "+max);
			throw new Exception("B1D FAIL counter reached "+ctr+" not "+max);
		}
		 System.out.println("BATTERY1D SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * Subset returns persistent collection iterator 'from' element inclusive, 'to' element exclusive
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1E(BufferedTreeMap session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		// from element inclusive to element exclusive
		// notice how we use base key to set lower bound as the partial unformed key is least possible value
		Iterator<?> itk = session.subMap(key, key+String.format(uniqKeyFmt, max)); // 'to' exclusive so we use max val
		int ctr = 0;
		while(itk.hasNext()) {
			Object f = itk.next();
			String nval = key + String.format(uniqKeyFmt, ctr);
			if( !f.equals(nval) ) {
				 System.out.println("BATTERY1E FAIL "+f+" -- "+nval);
				 System.out.println("BATTERY1E FAIL counter reached "+ctr+" not "+max);
				throw new Exception("B1E Fail on get with "+f+" -- "+nval);
			}
			++ctr;
		}
		if( ctr != max ) {
			 System.out.println("BATTERY1E FAIL counter reached "+ctr+" not "+max);
			throw new Exception("B1E FAIL counter reached "+ctr+" not "+max);
		}
		 System.out.println("BATTERY1E SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * Tailset returns persistent collection iterator greater or equal to 'from' element
	 * Notice how we use a partial key match here to delineate the start of the set
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1F(BufferedTreeMap session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		// subset strictly less than 'to' element
		Iterator<?> itk = session.tailMap(key); // >= from, so try partial bare key here
		int ctr = 0;
		while(itk.hasNext()) {
			Object f = itk.next();
			String nval = key + String.format(uniqKeyFmt, ctr);
			if( !f.equals(nval) ) {
				 System.out.println("BATTERY1F FAIL "+f+" -- "+nval);
				 System.out.println("BATTERY1F FAIL counter reached "+ctr+" not "+max);
				throw new Exception("B1F Fail on get with "+f+" -- "+nval);
			}
			++ctr;
		}
		if( ctr != max ) {
			 System.out.println("BATTERY1F FAIL counter reached "+ctr+" not "+max);
			throw new Exception("B1F FAIL counter reached "+ctr+" not "+max);
		}
		 System.out.println("BATTERY1F SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * headset returns values strictly less than 'to' element
	 * We are checking VALUE returns against expected key from KV iterator
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1D1(BufferedTreeMap session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		// headset strictly less than 'to' element
		Iterator<?> itk = session.headMapKV(key+String.format(uniqKeyFmt, max)); // set is strictly less than 'to' element so we use max val
		//System.out.println("BATTERY1D1 iterator:"+itk);
		if( itk == null)
			throw new Exception("BATTERY1D1 FAIL iterator for K/V headmap came back null for "+key+String.format(uniqKeyFmt, max));
		int ctr = 0;
		while(itk.hasNext()) {
			KeyValuePair f = (KeyValuePair) itk.next();
			if( f == null)
				throw new Exception("BATTERY1D1 FAIL K/V pair came back null for iterator.next() for "+key+String.format(uniqKeyFmt, max));
			System.out.println("BATTERY1D1 iterator result:"+f);
			String nval = val + String.format(uniqKeyFmt, ctr);
			if( !f.value.equals(nval) ) {
				 System.out.println("BATTERY1D1 FAIL "+f+" -- "+nval);
				 System.out.println("BATTERY1D1 FAIL counter reached "+ctr+" not "+max);
				throw new Exception("B1D1 Fail on get with "+f+" -- "+nval);
			}
			++ctr;
		}
		if( ctr != max ) {
			 System.out.println("BATTERY1D1 FAIL counter reached "+ctr+" not "+max);
			throw new Exception("B1D1 FAIL counter reached "+ctr+" not "+max);
		}
		 System.out.println("BATTERY1D1 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * Subset returns persistent collection iterator 'from' element inclusive, 'to' element exclusive
	 * We are checking VALUE returns against expected key from KV iterator
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1E1(BufferedTreeMap session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		// from element inclusive to element exclusive
		// notice how we use base key to set lower bound as the partial unformed key is least possible value
		Iterator<?> itk = session.subMapKV(key, key+String.format(uniqKeyFmt, max)); // 'to' exclusive so we use max val
		int ctr = 0;
		while(itk.hasNext()) {
			KeyValuePair f = (KeyValuePair) itk.next();
			String nval = val + String.format(uniqKeyFmt, ctr);
			if( !f.value.equals(nval) ) {
				 System.out.println("BATTERY1E1 FAIL "+f+" -- "+nval);
				 System.out.println("BATTERY1E1 FAIL counter reached "+ctr+" not "+max);
				throw new Exception("B1E1 Fail on get with "+f+" -- "+nval);
			}
			++ctr;
		}
		if( ctr != max ) {
			 System.out.println("BATTERY1E1 FAIL counter reached "+ctr+" not "+max);
			throw new Exception("B1E1 FAIL counter reached "+ctr+" not "+max);
		}
		 System.out.println("BATTERY1E1 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * Tailset returns persistent collection iterator greater or equal to 'from' element
	 * Notice how we use a partial key match here to delineate the start of the set
	 * We are checking VALUE returns against expected key from KV iterator
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1F1(BufferedTreeMap session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		// subset strictly less than 'to' element
		Iterator<?> itk = session.tailMapKV(key); // >= from, so try partial bare key here
		int ctr = 0;
		while(itk.hasNext()) {
			KeyValuePair f = (KeyValuePair) itk.next();
			String nval = val + String.format(uniqKeyFmt, ctr);
			if( !f.value.equals(nval) ) {
				 System.out.println("BATTERY1F1 FAIL "+f+" -- "+nval);
				 System.out.println("BATTERY1F1 FAIL counter reached "+ctr+" not "+max);
				throw new Exception("B1F1 Fail on get with "+f+" -- "+nval);
			}
			++ctr;
		}
		if( ctr != max ) {
			 System.out.println("BATTERY1F1 FAIL counter reached "+ctr+" not "+max);
			throw new Exception("B1F1 FAIL counter reached "+ctr+" not "+max);
		}
		 System.out.println("BATTERY1F1 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * Make sure key is replaced by new key
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1G(BufferedTreeMap session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		for(int i = min; i < max; i++) {
			String nkey = key + String.format(uniqKeyFmt, i);
			session.put(nkey, "Overwrite"+String.format(uniqKeyFmt, i));
			Object o = session.get(key + String.format(uniqKeyFmt, i));
			if( !o.equals("Overwrite"+String.format(uniqKeyFmt, i)) ){
				 System.out.println("BATTERY1G FAIL, found "+o+" after replace on iteration "+i+" for target "+nkey);
				throw new Exception("BATTERY1G FAIL, found "+o+" after replace on iteration "+i+" for target "+nkey);
			}
		}
		 System.out.println("BATTERY1G SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * Delete test
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery2(BufferedTreeMap session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		for(int item = 0; item < numDelete; item++) {
			String nkey = key + String.format(uniqKeyFmt, item);
			 System.out.println("Remo: "+nkey);
			session.remove(nkey);
			Object o = session.get(nkey);
			if( o != null ) {
				 System.out.println("BATTERY2 FAIL, found "+o+" after delete on iteration "+item+" for target "+nkey);
				throw new Exception("BATTERY2 FAIL, found "+o+" after delete on iteration "+item+" for target "+nkey);
			}
		}
		 System.out.println("BATTERY2 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * Delete test - random value then verify
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery2A(BufferedTreeMap session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		for(int i = 0; i < numDelete; i++) {
			int item = min + (int)(Math.random() * ((max - min) + 1));
			String nkey = key + String.format(uniqKeyFmt, item);
			 System.out.println("Remo: "+nkey);
			session.remove(nkey);
			Object o = session.get(nkey);
			if( o != null ) {
				 System.out.println("BATTERY2A FAIL, found "+o+" after delete on iteration "+i+" for target "+nkey);
				throw new Exception("BATTERY2A FAIL, found "+o+" after delete on iteration "+i+" for target "+nkey);
			}
		}
		 System.out.println("BATTERY2A SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}


}
