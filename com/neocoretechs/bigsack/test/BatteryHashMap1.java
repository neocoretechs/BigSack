package com.neocoretechs.bigsack.test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Stack;
import java.util.Map.Entry;
import java.util.stream.Stream;

import com.neocoretechs.bigsack.session.SetInterface;
import com.neocoretechs.bigsack.keyvaluepages.TraversalStackElement;
import com.neocoretechs.bigsack.session.BigSackAdapter;
import com.neocoretechs.bigsack.session.BufferedHashSet;
import com.neocoretechs.bigsack.session.TransactionalHashSet;
import com.neocoretechs.bigsack.session.TransactionalTreeSet;
/**
 * Java 8 hashmap stream and functional programming using BigSack test.<p/>
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
 * @author Jonathan Groff (C) NeoCoreTechs 2021
 *
 */
public class BatteryHashMap1 {
	static int min = 0; // controls minimum range for the test
	static int max = 100000;//000; // sets maximum range for the tests
	//static int numDelete = 100; // for delete test
	static int i = 0;
	static Integer zo = null;
	static int payloadSize = 0;
	/**
	* Analysis test fixture
	*/
	public static void main(String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		if (argv.length < 2) {
			 System.out.println("usage: java BatteryHashMap1 <database> payload_size");
			System.exit(1);
		}
		BigSackAdapter.setTableSpaceDir(argv[0]);
		payloadSize = Integer.parseInt(argv[1]);
		//BufferedHashSet session = BigSackAdapter.getBigSackHashSet(bigtestx.class);//new TransactionalTreeSet(argv[0],l3CacheSize);
		TransactionalHashSet session = BigSackAdapter.getBigSackTransactionalHashSet(bigtestx.class);//new TransactionalTreeSet(argv[0],l3CacheSize);
		 System.out.println("Begin Battery Fire!");
		//battery1(session, argv);
		//BigSackAdapter.commitTransaction(bigtestx.class);
		//session = BigSackAdapter.getBigSackHashSet(bigtestx.class);
		//battery1A(session, argv);
		//battery1B(session, argv);
		//battery1E(session, argv);
		battery3(session, argv);
		BigSackAdapter.commitTransaction(bigtestx.class);
		System.out.println("TEST BATTERY BATTERYHASHMAP1 COMPLETE. "+(System.currentTimeMillis()-tims)+" ms.");
		System.exit(0);
		
	}
	/**
	 * Loads up on key/value pairs, performs inser of keys in reverse order for max loading.
	 * keys are expected to be inserted in range min to max-1 regardless
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1(SetInterface session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		for(int i = max-1; i >= min; i--) {
			bigtestx b = new bigtestx();
			b.init(i, payloadSize);
			session.put(b);
		}
		long ms = System.currentTimeMillis();
		System.out.println("Added "+i+" in "+(System.currentTimeMillis()-ms)+"ms.");
		 System.out.println("BATTERY1 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1A(SetInterface session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		i = 0;
		for(; i < max; i++) {
			bigtestx b = new bigtestx();
			b.init(new Integer(i), 0);
			boolean cnts = session.contains(b);
			// if it does not get any, its a fail
			if( !cnts ) {
				System.out.println("BATTERY1A FAIL iterations:"+i);
				throw new Exception("BATTERY1A FAIL iterations:"+i);
			}
			System.out.println("stage 1 "+i);
		}
		++i; // bump it past max
		for(; i < max*2; i++) {
			bigtestx b = new bigtestx();
			b.init(new Integer(i), 0);
			boolean cnts = session.contains(b);
			// if it gets any, its a fail
			if( cnts ) {
				System.out.println("BATTERY1A FAIL iterations:"+i);
				throw new Exception("BATTERY1A FAIL iterations:"+i);
			}
			System.out.println("stage 2 "+i);
		}
		System.out.println("BATTERY1A SUCCESS "+i+" iterations in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	public static void battery1B(SetInterface session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		long size = session.size();
		System.out.println("Size "+size);
		System.out.println("BATTERY1B SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	public static void battery1E(BufferedHashSet session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		i = min;
		boolean addflag;
		for(; i < max; i++) {
			bigtestx b = new bigtestx();
			b.init(i, payloadSize);
			if(session.contains(b)) {
				session.remove(b);
				System.out.printf("Removed %d%n", i);
			} else {
				System.out.printf("DIDNT FIND %d!%n", i);
			}
		}
		BigSackAdapter.commitTransaction(bigtestx.class);
		/*
		session = BigSackAdapter.getBigSackSetTransaction(bigtestx.class);
		for(i = min; i < max; i++) {
			bigtestx b = new bigtestx();
			b.init(i);
			addflag = session.add(b);
		}
		BigSackAdapter.rollbackSet(bigtestx.class);
		*/
		session = BigSackAdapter.getBigSackHashSet(bigtestx.class);
		boolean success = true;
		// make sure these are not there..
		i = min;
		for(; i < max; i++) {
			bigtestx b = new bigtestx();
			b.init(i, payloadSize);
			if( session.contains(b) ) {
				System.out.println("BATTERY1E FAIL found rollback element "+i);
				success = false;
			}
		}
		System.out.println("BATTERY1E "+(success ? "SUCCESS" : "FAIL")+" in "+(System.currentTimeMillis()-tims)+" ms.");
		if( !success) throw new Exception("BATTERY1E fail");
	}
	
	/**
	 * See if first/last key/val works this can have unintended results 
	 * @param session
	 * @param argv
	 * @throws Exception
	 */
	public static void battery2(BufferedHashSet session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		Stack s = new Stack();
		TraversalStackElement tse = new TraversalStackElement(null, 0,0);
		bigtestx f = (bigtestx) session.first(tse,s);
		bigtestx l = (bigtestx) session.last(tse,s);
		System.out.println("BATTERY2 SUCCESS "+f+","+l+" in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	public static void battery3(SetInterface session, String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		bigtestx b = new bigtestx();
		b.init(i, payloadSize);
		TransactionalHashSet ths = (TransactionalHashSet)session;
		Iterator<?> it = ths.iterator();
		i = 0;
		while(it.hasNext()) {
			Object o = it.next();
			//System.out.println(i+" = "+o);
			if(i != (int)((bigtestx)(((Map.Entry)o).getKey())).key)
				throw new Exception(i+" does not match key "+((bigtestx)(((Map.Entry)o).getKey())).key);
			++i;
		}
		System.out.println("BATTERY3 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	public static void batteryHashMap1(BufferedHashSet session, String[] argv) throws Exception {
		bigtestx key = new bigtestx();
		key.init(123567, payloadSize);
		session.put(key);
		BigSackAdapter.rollbackTransaction(bigtestx.class);//session.rollback();
		session = BigSackAdapter.getBigSackHashSet(bigtestx.class);
		if( !session.contains(key) )
			 System.out.println("BATTERYHASHMAP1 SUCCESS ");
		else {
			 System.out.println("BATTERYHASHMAP1 FAIL");
			 throw new Exception("BATTERYHASHMAP1 FAIL");
		}
	}

}

