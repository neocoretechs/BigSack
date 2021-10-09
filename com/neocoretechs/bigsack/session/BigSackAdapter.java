package com.neocoretechs.bigsack.session;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import com.neocoretechs.bigsack.DBPhysicalConstants;


/**
 * This factory class enforces a strong typing for the BigSack using the database naming convention linked to the
 * class name of the class stored there.
 * 
 * The main function of this adapter is to ensure that the appropriate map or set is instantiated.
 * A map or set can be obtained by instance of Comparable to impart ordering.
 * A Buffered map or set has atomic transactions bounded automatically with each insert/delete
 * A transactional map or set requires commit/rollback and can be checkpointed.
 * In either case recovery is in effect to preserve integrity.
 * The database name is the full path of the top level tablespace and log directory, i.e.
 * /home/db/test would create a 'test' database in the /home/db directory. If we are using this strong
 * typing adapter, and were to store a String, the database name would translate to: /home/db/testjava.lang.String.
 * If the config is cluster,
 * the log of master node and tablespace directories on the remote machines. OR if cluster mode, a remote
 * directory can be specified and the local master log first, then remote worker node tablespace directories.
 * This can affect different OS configs for cluster testing and heterogeneous clusters.
 * The class name is translated into the appropriate file name via a simple translation table to give us a
 * database/class/tablespace identifier for each file used.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2014,2015,2021
 *
 */
public class BigSackAdapter {
	private static boolean DEBUG = false;
	private static String tableSpaceDir = "/";
	private static String remoteDir = null;
	private static final char[] ILLEGAL_CHARS = { '[', ']', '!', '+', '=', '|', ';', '?', '*', '\\', '<', '>', '|', '\"', ':' };
	private static final char[] OK_CHARS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E' };

	private static ConcurrentHashMap<String, SetInterface> classToIso = new ConcurrentHashMap<String,SetInterface>();
	
	public static String getTableSpaceDir() {
		return tableSpaceDir;
	}
	public static void setTableSpaceDir(String tableSpaceDir) {
		BigSackAdapter.tableSpaceDir = tableSpaceDir;
	}
	public static String getRemoteDir() {
		return remoteDir;
	}
	public static void setRemoteDir(String tableSpaceDir) {
		BigSackAdapter.remoteDir = tableSpaceDir;
	}
	public static String getDatabaseName(Class clazz) {
		String xClass = translateClass(clazz.getName());
		return tableSpaceDir+xClass;
	}
	public static String getDatabaseName(String clazz) {
		return tableSpaceDir+clazz;
	}
	/**
	 * Get a TreeSet via Comparable instance.
	 * @param clazz The Comparable object that the java class name is extracted from
	 * @return A BufferedTreeSet for the clazz instances.
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static BufferedTreeSet getBigSackTreeSet(Comparable clazz) throws IllegalAccessException, IOException {
		return getBigSackTreeSet(clazz.getClass());
	}
	/**
	 * Get a TreeSet via Java Class type.
	 * @param clazz The Java Class of the intended database
	 * @return The BufferedTreeSet for the clazz type.
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static BufferedTreeSet getBigSackTreeSet(Class clazz) throws IllegalAccessException, IOException {
		String xClass = translateClass(clazz.getName());
		BufferedTreeSet ret = (BufferedTreeSet) classToIso.get(xClass);
		if(DEBUG)
			System.out.println("BigSackAdapter.getBigSackTreeSet About to return designator: "+tableSpaceDir+xClass+" formed from "+clazz.getClass().getName());
		if( ret == null ) {
			ret =  new BufferedTreeSet(tableSpaceDir+xClass, DBPhysicalConstants.BACKINGSTORE, DBPhysicalConstants.DBUCKETS);
			classToIso.put(xClass, ret);
		}
		return ret;
	}
	/**
	 * Get a TreeMap via Comparable instance.
	 * @param clazz The Comparable object that the java class name is extracted from
	 * @return A BufferedTreeMap for the clazz instances.
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static BufferedTreeMap getBigSackTreeMap(Comparable clazz) throws IllegalAccessException, IOException {
		return getBigSackTreeMap(clazz.getClass());
	}
	/**
	 * Get a TreeMap via Java Class type.
	 * @param clazz The Java Class of the intended database
	 * @return The BufferedTreeMap for the clazz type.
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static BufferedTreeMap getBigSackTreeMap(Class clazz) throws IllegalAccessException, IOException {
		String xClass = translateClass(clazz.getName());
		BufferedTreeMap ret = (BufferedTreeMap) classToIso.get(xClass);
		if(DEBUG)
			System.out.println("BigSackAdapter.getBigSackTreeMap About to return designator: "+tableSpaceDir+xClass+" formed from "+clazz.getClass().getName());
		if( ret == null ) {
			ret =  new BufferedTreeMap(tableSpaceDir+xClass, DBPhysicalConstants.BACKINGSTORE, DBPhysicalConstants.DBUCKETS);
			classToIso.put(xClass, ret);
		}
		return ret;
	}
	/**
	 * Get a TransactionalTreeSet via Comparable instance.
	 * @param clazz The Comparable object that the java class name is extracted from
	 * @return A TransactionalTreeSet for the clazz instances.
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static TransactionalTreeSet getBigSackTransactionalTreeSet(Comparable clazz) throws IllegalAccessException, IOException {
		return getBigSackTransactionalTreeSet(clazz.getClass());
	}
	/**
	 * Get a TransactionalTreeSet via Java Class type.
	 * @param clazz The Java Class of the intended database
	 * @return The TransactionalTreeSet for the clazz type.
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static TransactionalTreeSet getBigSackTransactionalTreeSet(Class clazz) throws IllegalAccessException, IOException {
		String xClass = translateClass(clazz.getName());
		TransactionalTreeSet ret = (TransactionalTreeSet) classToIso.get(xClass);
		if(DEBUG)
			System.out.println("BigSackAdapter.getBigSackTransactionalTreeSet About to return designator: "+tableSpaceDir+xClass+" formed from "+clazz.getClass().getName());
		if( ret == null ) {
			ret =  new TransactionalTreeSet(tableSpaceDir+xClass, DBPhysicalConstants.BACKINGSTORE, DBPhysicalConstants.DBUCKETS);
			classToIso.put(xClass, ret);
		}
		return ret;
	}
	/**
	 * Get a TransactionalTreeMap via Comparable instance.
	 * @param clazz The Comparable object that the java class name is extracted from.
	 * @return A TransactionalTreeMap for the clazz instances.
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static TransactionalTreeMap getBigSackTransactionalTreeMap(Comparable clazz) throws IllegalAccessException, IOException {
		return getBigSackTransactionalTreeMap(clazz.getClass());
	}
	/**
	 * Get a TransactionalTreeMap via Java Class type.
	 * @param clazz The Java Class of the intended database.
	 * @return The TransactionalTreeMap for the clazz type.
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static TransactionalTreeMap getBigSackTransactionalTreeMap(Class clazz) throws IllegalAccessException, IOException {
		String xClass = translateClass(clazz.getName());
		TransactionalTreeMap ret = (TransactionalTreeMap) classToIso.get(xClass);
		if(DEBUG)
			System.out.println("BigSackAdapter.getBigSackMapTransaction About to return designator: "+tableSpaceDir+xClass+" formed from "+clazz.getClass().getName());
		if( ret == null ) {
			ret =  new TransactionalTreeMap(tableSpaceDir+xClass, DBPhysicalConstants.BACKINGSTORE, DBPhysicalConstants.DBUCKETS);
			classToIso.put(xClass, ret);
		}
		return ret;
	}
	
	public static BufferedHashSet getBigSackHashSet(Comparable clazz) throws IllegalAccessException, IOException {
		return getBigSackHashSet(clazz.getClass());
	}
	
	public static BufferedHashSet getBigSackHashSet(Class clazz) throws IllegalAccessException, IOException {
		String xClass = translateClass(clazz.getName());
		BufferedHashSet ret = (BufferedHashSet) classToIso.get(xClass);
		if(DEBUG)
			System.out.println("BigSackAdapter.getBigSackHashSet About to return designator: "+tableSpaceDir+xClass+" formed from "+clazz.getClass().getName());
		if( ret == null ) {
			ret =  new BufferedHashSet(tableSpaceDir+xClass, DBPhysicalConstants.BACKINGSTORE, DBPhysicalConstants.DBUCKETS);
			classToIso.put(xClass, ret);
		}
		return ret;
	}
	
	public static TransactionalHashSet getBigSackTransactionalHashSet(Comparable clazz) throws IllegalAccessException, IOException {
		return getBigSackTransactionalHashSet(clazz.getClass());
	}
	
	public static TransactionalHashSet getBigSackTransactionalHashSet(Class clazz) throws IllegalAccessException, IOException {
		String xClass = translateClass(clazz.getName());
		TransactionalHashSet ret = (TransactionalHashSet) classToIso.get(xClass);
		if(DEBUG)
			System.out.println("BigSackAdapter.getBigSackTransactionalHashSet About to return designator: "+tableSpaceDir+xClass+" formed from "+clazz.getClass().getName());
		if( ret == null ) {
			ret =  new TransactionalHashSet(tableSpaceDir+xClass, DBPhysicalConstants.BACKINGSTORE, DBPhysicalConstants.DBUCKETS);
			classToIso.put(xClass, ret);
		}
		return ret;
	}
	
	public static void checkpointTransaction(Class clazz) throws IllegalAccessException, IOException {
		String xClass = translateClass(clazz.getName());
		TransactionInterface ret = (TransactionInterface) classToIso.get(xClass);
		ret.Checkpoint();
	}
	
	public static void commitTransaction(Class clazz) throws IOException {
		String xClass = translateClass(clazz.getName());
		TransactionInterface ret = (TransactionInterface) classToIso.get(xClass);
		ret.Commit();
		classToIso.remove(xClass);
	}
	
	public static void rollbackTransaction(Class clazz) throws IOException {
		String xClass = translateClass(clazz.getName());
		TransactionInterface ret = (TransactionInterface) classToIso.get(xClass);
		ret.Rollback();
		classToIso.remove(xClass);
	}
	
	
	/**
	 * Translate a class name into a legitimate file name with some aesthetics.
	 * @param clazz
	 * @return
	 */
	public static String translateClass(String clazz) {
		//boolean hasReplaced = false; // debug
		StringBuffer sb = new StringBuffer();
		for(int i = 0; i < clazz.length(); i++) {
			char chr = clazz.charAt(i);
			for(int j = 0; j < ILLEGAL_CHARS.length; j++) {
				if( chr == ILLEGAL_CHARS[j] ) {
					chr = OK_CHARS[j];
					//hasReplaced = true;
					break;
				}
			}
			sb.append(chr);
		}
		//if( hasReplaced )
		//	System.out.println("Class name translated from "+clazz+" to "+sb.toString());
		return sb.toString();
	}

}
