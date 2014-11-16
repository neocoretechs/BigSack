package com.neocoretechs.bigsack.test;

import java.util.Iterator;

import com.neocoretechs.bigsack.iterator.KeyValuePair;
import com.neocoretechs.bigsack.session.BigSackSession;
import com.neocoretechs.bigsack.session.BufferedTreeMap;
import com.neocoretechs.bigsack.session.BufferedTreeSet;
import com.neocoretechs.bigsack.session.SessionManager;
import com.neocoretechs.bigsack.session.TransactionalTreeSet;
/**
 * Yes, this should be a nice JUnit fixture someday
 * The static constant fields in the class control the key generation for the tests
 * In general, the keys and values are formatted according to uniqKeyFmt to produce
 * a series of canonically correct sort order strings for the DB in the range of min to max vals
 * In general most of the battery1 testing relies on checking order against expected values hence the importance of
 * canonical ordering in the sample strings.
 * Of course, you can substitute any class for the Strings here providing its Comparable 
 * This module hammers the L3 cache thread safe wrappers BufferedTreeSet and BufferedTreeMap
 * @author jg
 *
 */
public class BatteryBigSack3 {
	static String key = "This is a testxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"; // holds the base random key string for tests
	static String val = "Of a BigSack K/V pair!yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy"; // holds base random value string
	static String uniqKeyFmt = "%0100d"; // base + counter formatted with this gives equal length strings for canonical ordering
	static int min = 0;
	static int max = 40;
	static int numDelete = 100; // for delete test
	static int l3CacheSize = 100; // size of object cache
	/**
	* Analysis test fixture
	*/
	public static void main(String[] argv) throws Exception {
		if (argv.length == 0 || argv[0].length() == 0) {
			 System.out.println("usage: java BatteryBigSack3 <database>");
			System.exit(1);
		}
		//BufferedTreeSet session = new BufferedTreeSet(argv[0],l3CacheSize);
		TransactionalTreeSet session = new TransactionalTreeSet(argv[0],l3CacheSize);
		 System.out.println("Begin Battery Fire!");
		battery1E(session, argv);
		//battery1A(session, argv);
		//battery1B(session, argv);
		//battery1D(session, argv);
		//battery1E(session, argv);
		//battery1F(session, argv);
		//battery2(session, argv);
		//battery3(session.getSession(), argv);
		//battery4(session, argv);
		//battery5(session, argv);
		//SessionManager.stopCheckpointDaemon(argv[0]);
		//session.commit();
		System.out.println("TEST BATTERY 3 COMPLETE.");
		
	}
	/**
	 * Loads up on key/value pairs
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1(BufferedTreeSet session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		for(int i = min; i < max; i++) {
			session.add(key + String.format(uniqKeyFmt, i));
		}
		 System.out.println("BATTERY1 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * Does a simple 'get' of the elements inserted before
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1A(BufferedTreeSet session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		for(int i = min; i < max; i++) {
			boolean o = session.contains(key + String.format(uniqKeyFmt, i));
			if( !o ) {
				 System.out.println("BATTERY1A FAIL "+o);
				throw new Exception("B1A Fail on get with "+o);
			}
		}
		 System.out.println("BATTERY1A SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * See if first/last key/val works this can have unintended results for this set so just check it does
	 * not bomb
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1B(BufferedTreeSet session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		String f = (String) session.first();
		String l = (String) session.last();
		 System.out.println("BATTERY1A SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	
	}

	public static void battery1C(TransactionalTreeSet session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		for(int i = min; i < max; i++) {
			session.add(key + String.format(uniqKeyFmt, i));
		}
		session.commit();
		System.out.println("BATTERY1C SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	public static void battery1D(TransactionalTreeSet session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		for(int i = min; i < max; i++) {
			session.add(key + String.format(uniqKeyFmt, i));
			if( i == (max/2) ) session.checkpoint();
		}	
		System.out.println("BATTERY1D SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	public static void battery1E(TransactionalTreeSet session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		for(int i = min; i < max; i++) {
			session.add(key + String.format(uniqKeyFmt, i));
		}
		session.rollback();
		System.out.println("BATTERY1E SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * headset returns values strictly less than 'to' element
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1D(BufferedTreeSet session, String[] argv) throws Exception {
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
	public static void battery1E(BufferedTreeSet session, String[] argv) throws Exception {
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
	public static void battery1F(BufferedTreeSet session, String[] argv) throws Exception {
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
	 * Delete test
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery2(BufferedTreeSet session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		for(int item = 0; item < numDelete; item++) {
			String nkey = key + String.format(uniqKeyFmt, item);
			 System.out.println("Remo: "+nkey);
			session.remove(nkey);
			boolean o = session.contains(nkey);
			if( o ) {
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
		String key = "Lets try this3";
		session.put(key);
		session.Rollback();
		Object o = session.get(key);
		if( o == null )
			 System.out.println("BATTERY3 SUCCESS ");
		else
			 System.out.println("BATTERY3 FAIL "+o+" "+o.getClass().getName());
	}

}
