package com.neocoretechs.bigsack.test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.UUID;

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
	static String uniqKeyFmt = "%0100d"; // base + counter formatted with this gives equal length strings for canonical ordering
	static int min = 0;
	static int max = 1;
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
		battery1(session, argv);
		//battery1A(session, argv);
		//battery1B(session, argv);
		//battery1D(session, argv);
		//battery1E(session, argv);
	
		//SessionManager.stopCheckpointDaemon(argv[0]);
		session.commit();
		System.out.println("TEST BATTERY 3 COMPLETE.");
		
	}
	/**
	 * Loads up on key/value pairs
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1(TransactionalTreeSet session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		for(int i = min; i < max; i++) {
			bigtest b = new bigtest();
			b.init();
			session.add(b);
		}
		 System.out.println("BATTERY1 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * Does a headset from first element, headset retrieve from head to strictly less
	 * then target of the elements inserted before, hence head from first should be 0
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1A(TransactionalTreeSet session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		Iterator<?> it = session.headSet((Comparable) session.first());
		int i = 0;
		while(it.hasNext()) {
			it.next();
			++i;
		}
		if( i != 0 ) {
				System.out.println("BATTERY1A FAIL iterations:"+i+" reported size:"+session.size());
				throw new Exception("BATTERY1A FAIL iterations:"+i+" reported size:"+session.size());
		}
		 System.out.println("BATTERY1A SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * Does a headset from last element, headset retrieve from head to strictly less
	 * then target of the elements inserted before, hence head from last should be count
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery2A(TransactionalTreeSet session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		Iterator<?> it = session.headSet((Comparable) session.last());
		int i = 0;
		while(it.hasNext()) {
			it.next();
			++i;
		}
		if( session.size() != i ) {
				System.out.println("BATTERY2A FAIL iterations:"+i+" reported size:"+session.size());
				throw new Exception("BATTERY2A FAIL iterations:"+i+" reported size:"+session.size());
		}
		 System.out.println("BATTERY2A SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * See if first/last key/val works this can have unintended results for this set so just check it does
	 * not bomb
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1B(TransactionalTreeSet session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		bigtest f = (bigtest) session.first();
		bigtest l = (bigtest) session.last();
		System.out.println("BATTERY1B SUCCESS "+f+","+l+" in "+(System.currentTimeMillis()-tims)+" ms.");
	}

	public static void battery1D(TransactionalTreeSet session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		for(int i = min; i < max; i++) {
			bigtest b = new bigtest();
			b.init();
			session.add(b);
			if( i == (max/2) ) session.checkpoint();
		}	
		System.out.println("BATTERY1D SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	public static void battery1E(TransactionalTreeSet session, String[] argv) throws Exception {
		ArrayList<bigtest> al = new ArrayList<bigtest>();
		long tims = System.currentTimeMillis();
		for(int i = min; i < max; i++) {
			bigtest b = new bigtest();
			b.init();
			session.add(b);
			al.add(b);
		}
		session.rollback();
		boolean success = true;
		// make sure these are not there..
		for(bigtest bt : al) {
			if( session.contains(bt) ) {
				System.out.println("BATTERY1E FAIL found rollback element "+bt);
				success = false;
			}
		}
		System.out.println("BATTERY1E "+(success ? "SUCCESS" : "FAIL")+" in "+(System.currentTimeMillis()-tims)+" ms.");
	}

}
class bigtest implements Serializable, Comparable {
	private static final long serialVersionUID = 8890285185155972693L;
	byte[] b = new byte[32767];
	UUID uuid = null;
	String randomUUIDString;
	public bigtest() {
	}
	public void init() {
		uuid = UUID.randomUUID();
		randomUUIDString = uuid.toString();
	}
	@Override
	public int compareTo(Object arg0) {
		return randomUUIDString.compareTo(((bigtest)arg0).randomUUIDString);
	}
	public String toString() { return randomUUIDString; }
}
