package com.neocoretechs.bigsack.test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Stack;

import com.neocoretechs.bigsack.keyvaluepages.TraversalStackElement;
import com.neocoretechs.bigsack.session.BigSackAdapter;
import com.neocoretechs.bigsack.session.TransactionalTreeSet;
/**
 * Testing of TransactionalTreeSet with large payload key object.
 * The key itself is a monotonically increasing integer controlled by the min and max 
 * range values fields in the class, but the key class also contains a 32k byte array
 * which must be serialized as part of the storage of the key, so it spans multiple database pages.
 * Parameters: Set the database name as the first argument "/users/you/TestDB1" where
 * the directory  "/users/you" must exist and a series of tablespaces and a log directory
 * are created under that. The database files will be named "TestDB1" under "/users/you/log and 
 * /users/you/tablespace0" to "/users/you/tablespace7".
 * Set the name of the properties file in the VM -DBigSack.properties="/users/you/Relatrix/BigSack.properties"
 * Yes, this should be a nice JUnit fixture someday
 * @author jg
 *
 */
public class BatteryBigSack3 {
	static int min = 0; // controls minimum range for the test
	static int max = 1000; // sets maximum range for the tests
	//static int numDelete = 100; // for delete test
	static int l3CacheSize = 100; // size of object cache
	/**
	* Analysis test fixture
	*/
	public static void main(String[] argv) throws Exception {
		if (argv.length == 0 || argv[0].length() == 0) {
			 System.out.println("usage: java BatteryBigSack3 <database>");
			System.exit(1);
		}
		TransactionalTreeSet session = BigSackAdapter.getBigSackTransactionalTreeSet(Class.forName(argv[0]));//new TransactionalTreeSet(argv[0],l3CacheSize);
		 System.out.println("Begin Battery Fire!");
		battery1(session, argv);
		battery1A(session, argv);
		battery2A(session, argv);
		battery3A(session, argv);
		battery3B(session, argv);
		battery1D(session, argv);
		battery1E(session, argv);
		battery3(session, argv);
	
		//SessionManager.stopCheckpointDaemon(argv[0]);
		//session.commit();
		BigSackAdapter.commitTransaction(Class.forName(argv[0]));
		System.out.println("TEST BATTERY 3 COMPLETE.");
		
	}
	/**
	 * Loads up on key/value pairs, performs inser of keys in reverse order for max loading.
	 * keys are expected to be inserted in range min to max-1 regardless
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1(TransactionalTreeSet session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		for(int i = max-1; i >= min; i--) {
			bigtestx b = new bigtestx();
			b.init(i,0);
			long ms = System.currentTimeMillis();
			session.put(b);
			System.out.println("Added "+i+" in "+(System.currentTimeMillis()-ms)+"ms.");
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
		Stack s = new Stack();
		TraversalStackElement tse = new TraversalStackElement(null, 0,0);
		Iterator<?> it = session.headSet((Comparable) session.first(tse,s));
		int i = 0;
		Object o = null;
		while(it.hasNext()) {
			++i;
			o = it.next();
			System.out.println("["+i+"]"+o);
		}
		// if it gets any, its a fail
		if( i != 0 ) {
				System.out.println("BATTERY1A FAIL iterations:"+i+" reported size:"+session.size());
				throw new Exception("BATTERY1A FAIL iterations:"+i+" reported size:"+session.size());
		}
		System.out.println("BATTERY1A SUCCESS "+i+" iterations in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * Does a headset from last element, headset retrieve from head to strictly less
	 * than target, hence head from last should be count-1 with 998 if 1000 records
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery2A(TransactionalTreeSet session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		Stack s = new Stack();
		TraversalStackElement tse = new TraversalStackElement(null, 0,0);
		Iterator<?> it = session.headSet((Comparable) session.last(tse, s));
		int i = 0;
		Object o = null;
		while(it.hasNext()) {
			++i;
			o = it.next();
			System.out.println("["+i+"]"+o);
		}
		if( session.size() != i+1 || (Integer)((bigtestx)o).key != max-2 ) {
				System.out.println("BATTERY2A FAIL iterations:"+(i+1)+" reported size:"+session.size());
				throw new Exception("BATTERY2A FAIL iterations:"+(i+1)+" reported size:"+session.size());
		}
		System.out.println("BATTERY2A SUCCESS "+(i+1)+" iterations in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * Does a tailset from first element, tailset retrieve from element greater or equal to end
	 * collection iterator greater or equal to 'from' element so should return all
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery3A(TransactionalTreeSet session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		Stack s = new Stack();
		TraversalStackElement tse = new TraversalStackElement(null, 0,0);
		Iterator<?> it = session.tailSet((Comparable) session.first(tse,s));
		int i = 0;
		while(it.hasNext()) {
			++i;
			System.out.println("["+i+"]"+it.next());
		}
		if( session.size() != i ) {
				System.out.println("BATTERY3A FAIL iterations:"+(i)+" reported size:"+session.size());
				throw new Exception("BATTERY3A FAIL iterations:"+(i)+" reported size:"+session.size());
		}
		System.out.println("BATTERY3A SUCCESS "+(i)+" iterations in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * Does a tailset from last element, tailset retrieve from element greater or equal to end
	 * collection iterator greater or equal to 'from' element so this should return 1 item, the 999 at end
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery3B(TransactionalTreeSet session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		Stack s = new Stack();
		TraversalStackElement tse = new TraversalStackElement(null, 0,0);
		Iterator<?> it = session.tailSet((Comparable) session.last(tse, s));
		int i = 0;
		Object o = null;
		while(it.hasNext()) {
			++i;
			o = it.next();
			System.out.println("["+i+"]"+o);
		}
		if( 1 != i || (Integer)((bigtestx)o).key != max-1) {
				System.out.println("BATTERY3B FAIL iterations:"+(i)+" reported size:"+session.size());
				throw new Exception("BATTERY3B FAIL iterations:"+(i)+" reported size:"+session.size());
		}
		System.out.println("BATTERY3B SUCCESS "+(i)+" iterations in "+(System.currentTimeMillis()-tims)+" ms.");
	}


	public static void battery1D(TransactionalTreeSet session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		for(int i = min; i < max; i++) {
			bigtestx b = new bigtestx();
			b.init(i, 0);
			session.put(b);
			if( i == (max/2) ) BigSackAdapter.checkpointTransaction(Class.forName(argv[0]));//session.checkpoint();
		}	
		System.out.println("BATTERY1D SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	public static void battery1E(TransactionalTreeSet session, String[] argv) throws Exception {
		ArrayList<bigtestx> al = new ArrayList<bigtestx>();
		long tims = System.currentTimeMillis();
		for(int i = min; i < max; i++) {
			bigtestx b = new bigtestx();
			b.init(i, 0);
			session.put(b);
			al.add(b);
		}
		BigSackAdapter.rollbackTransaction(Class.forName(argv[0]));//session.rollback();
		boolean success = true;
		// make sure these are not there..
		for(bigtestx bt : al) {
			if( session.contains(bt) ) {
				System.out.println("BATTERY1E FAIL found rollback element "+bt);
				success = false;
			}
		}
		System.out.println("BATTERY1E "+(success ? "SUCCESS" : "FAIL")+" in "+(System.currentTimeMillis()-tims)+" ms.");
		if( !success) throw new Exception("BATTERY1E fail");
	}
	
	public static void battery3(TransactionalTreeSet session, String[] argv) throws Exception {
		bigtestx key = new bigtestx();
		key.init(123567, 0);
		session.put(key);
		BigSackAdapter.rollbackTransaction(Class.forName(argv[0]));//session.rollback();
		if( !session.contains(key) )
			 System.out.println("BATTERY3 SUCCESS ");
		else {
			 System.out.println("BATTERY3 FAIL");
			 throw new Exception("BATTERY3 FAIL");
		}
	}

}
