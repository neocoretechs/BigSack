package com.neocoretechs.bigsack.test;

import java.util.Iterator;

import com.neocoretechs.bigsack.iterator.KeyValuePair;
import com.neocoretechs.bigsack.session.BigSackSession;
import com.neocoretechs.bigsack.session.SessionManager;
/**
 * Test battery for Key/Value pairs using session level constructs.
 * At the session level we leave out the level 3 cache (in memory object table cache) and
 * test to the metal of the session. The higher level Buffered and Transactional TreeMaps and TreeSets
 * all use the base session object under test here.
 * Parameters: Set the database name as the first argument "/users/you/TestDB1" where
 * the directory  "/users/you" must exist and a series of tablespaces and a log directory
 * are created under that. The database files will be named "TestDB1" under "/users/you/log and 
 * /users/you/tablespace0" to "/users/you/tablespace7".
 * Set the name of the properties file in the VM -DBigSack.properties="/users/you/Relatrix/BigSack.properties"
 * Yes, this should be a nice JUnit fixture someday.
 * The static constant fields in the class control the key generation for the tests.
 * In general, the keys and values are formatted according to uniqKeyFmt to produce
 * a series of canonically correct sort order strings for the DB in the range of min to max vals.
 * In general most of the testing relies on checking order against expected values hence the importance of
 * canonical ordering in the sample strings.
 * Of course, you can substitute any class for the Strings here providing it implements Comparable 
 * @author jg
 *
 */
public class BatteryBigSack {
	static String key = "This is a test"; // holds the base random key string for tests
	static String val = "Of a BigSack K/V pair!"; // holds base random value string
	static String uniqKeyFmt = "%0100d"; // base + counter formatted with this gives equal length strings for canonical ordering
	static int min = 0; // controls minimum range of test
	static int max = 2000; // controls maximum range of tests
	static int numDelete = 100; // for delete test
	/**
	* Analysis test fixture
	*/
	public static void main(String[] argv) throws Exception {
		if (argv.length == 0 || argv[0].length() == 0) {
			System.out.println("usage: java BatteryBigSack <database>");
			System.exit(1);
		}
		BigSackSession session = SessionManager.Connect(argv[0], null, true);
		battery1(session, argv);
		battery1A(session, argv);
		battery1B(session, argv);
		battery1C(session, argv);
		battery1D(session, argv);
		battery1D1(session, argv);
		battery1E(session, argv);
		battery1E1(session, argv);
		battery1F(session, argv);
		battery1F1(session, argv);
		battery1G(session, argv);
		battery2(session, argv);
		battery2A(session, argv);
		battery3(session, argv);
	
		
		session.Commit();
		System.out.println("TEST BATTERY COMPLETE.");
		
	}
	/**
	 * Loads up on key/value pairs
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1(BigSackSession session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		for(int i = min; i < max; i++) {
			session.put(key + String.format(uniqKeyFmt, i), val+String.format(uniqKeyFmt, i));
		}
		session.Commit();
		System.out.println("BATTERY1 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * Does a simple 'get' of the elements inserted before
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1A(BigSackSession session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		for(int i = min; i < max; i++) {
			Object o = session.get(key + String.format(uniqKeyFmt, i));
			if( !(val+String.format(uniqKeyFmt, i)).equals(o) ) {
				 System.out.println("BATTERY1A FAIL "+o);
				throw new Exception("B1A Fail on get with "+o);
			}
		}
		System.out.println("BATTERY1A SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * See if first/last key/val works
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1B(BigSackSession session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		String f = (String) session.first();
		String l = (String) session.last();
		String minval = val + String.format(uniqKeyFmt, min);
		String maxval = val + String.format(uniqKeyFmt, (max-1));
		if( !f.equals(minval) || !l.equals(maxval) ) { // max-1 cause we looped it in
				System.out.println("BATTERY1B FAIL "+f+" -- "+l);
				throw new Exception("B1B Fail on Value get with "+f+" -- "+l);
		}
		String fk = (String) session.firstKey();
		String lk = (String) session.lastKey();
		minval = key + String.format(uniqKeyFmt, min);
		maxval = key + String.format(uniqKeyFmt, (max-1));
		if( !(fk.equals(minval)) || !(lk.equals(maxval)) ) { // looped in so max-1
			System.out.println("BATTERY1B FAIL "+fk+" -- "+lk);
			throw new Exception("B1B Fail on Key get with "+fk+" -- "+lk);
		}
		 System.out.println("BATTERY1B SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	
	}
	/**
	 * Hammers on keyset and entryset
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1C(BigSackSession session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		Iterator<?> itk = session.keySet();
		Iterator<?> ite = session.entrySet();
		int ctr = 0;
		while(itk.hasNext()) {
			Object f = itk.next();
			Object l = ite.next();
			String nkey = key + String.format(uniqKeyFmt, ctr);
			String nval = val + String.format(uniqKeyFmt, ctr);
			if( !f.equals(nkey) || !l.equals(nval)) {
				 System.out.println("BATTERY1C FAIL "+f+" -- "+l+" "+ctr);
				 System.out.println("BATTERY1C FAIL counter reached "+ctr+" not "+max);
				throw new Exception("B1C Fail on get with "+f+" -- "+l+" "+ctr);
			}
			
			//System.out.println(f+" "+l+" "+nkey+" "+nval);
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
	public static void battery1D(BigSackSession session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		// headset strictly less than 'to' element
		Iterator<?> itk = session.headSet(key+(max)); // set is strictly less than 'to' element so we use max val
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
	public static void battery1E(BigSackSession session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		// from element inclusive to element exclusive
		// notice how we use base key to set lower bound as the partial unformed key is least possible value
		Iterator<?> itk = session.subSet(key, key+String.format(uniqKeyFmt, max)); // 'to' exclusive so we use max val
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
	public static void battery1F(BigSackSession session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		// subset strictly less than 'to' element
		Iterator<?> itk = session.tailSet(key); // >= from, so try partial bare key here
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
	public static void battery1D1(BigSackSession session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		// headset strictly less than 'to' element
		Iterator<?> itk = session.headSetKV(key+(max)); // set is strictly less than 'to' element so we use max val
		int ctr = 0;
		while(itk.hasNext()) {
			KeyValuePair f = (KeyValuePair) itk.next();
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
	public static void battery1E1(BigSackSession session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		// from element inclusive to element exclusive
		// notice how we use base key to set lower bound as the partial unformed key is least possible value
		Iterator<?> itk = session.subSetKV(key, key+String.format(uniqKeyFmt, max)); // 'to' exclusive so we use max val
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
	public static void battery1F1(BigSackSession session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		// subset strictly less than 'to' element
		Iterator<?> itk = session.tailSetKV(key); // >= from, so try partial bare key here
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
	public static void battery1G(BigSackSession session, String[] argv) throws Exception {
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
		session.Commit();
		 System.out.println("BATTERY1G SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * Delete test
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery2(BigSackSession session, String[] argv) throws Exception {
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
	public static void battery2A(BigSackSession session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		for(int i = 0; i < numDelete; i++) {
			int item = min + (int)(Math.random() * ((max - min) + 1));
			String nkey = key + String.format(uniqKeyFmt, item);
			 System.out.println("Remo: "+nkey);
			session.remove(nkey);
			Object o = session.get(nkey);
			if( o != null ) {
				 System.out.println("BATTERY2 FAIL, found "+o+" after delete on iteration "+i+" for target "+nkey);
				throw new Exception("BATTERY2 FAIL, found "+o+" after delete on iteration "+i+" for target "+nkey);
			}
		}
		 System.out.println("BATTERY2 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	public static void battery3(BigSackSession session, String[] argv) throws Exception {
		String key = "Lets try this";
		session.put(key, "A BigSack Rollback!");
		session.Rollback();
		Object o = session.get(key);
		if( o == null )
			 System.out.println("BATTERY2 SUCCESS ");
		else
			 System.out.println("BATTERY2 FAIL");
	}

}
