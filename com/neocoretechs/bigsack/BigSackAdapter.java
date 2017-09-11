package com.neocoretechs.bigsack;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import com.neocoretechs.bigsack.session.BufferedTreeMap;
import com.neocoretechs.bigsack.session.BufferedTreeSet;
import com.neocoretechs.bigsack.session.TransactionalTreeMap;
import com.neocoretechs.bigsack.session.TransactionalTreeSet;

/**
 * This class enforces a strong typing for the BigSack using the database naming convention linked to the
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
 * @author jg Copyright (C) NeoCoreTechs 2014,2015
 *
 */
public class BigSackAdapter {
	private static boolean DEBUG = false;
	private static String tableSpaceDir = "/";
	private static String remoteDir = null;
	private static final char[] ILLEGAL_CHARS = { '[', ']', '!', '+', '=', '|', ';', '?', '*', '\\', '<', '>', '|', '\"', ':' };
	private static final char[] OK_CHARS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E' };

	private static ConcurrentHashMap<String, TransactionalTreeSet> classToIsoXTreeset = new ConcurrentHashMap<String,TransactionalTreeSet>();
	private static ConcurrentHashMap<String, TransactionalTreeMap> classToIsoXTreemap = new ConcurrentHashMap<String,TransactionalTreeMap>();
	private static ConcurrentHashMap<String, BufferedTreeSet> classToIsoTreeSet = new ConcurrentHashMap<String,BufferedTreeSet>();
	private static ConcurrentHashMap<String, BufferedTreeMap> classToIsoTreemap = new ConcurrentHashMap<String,BufferedTreeMap>();
	
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
	/**
	 * Get a TreeSet via Comparable instance.
	 * @param clazz The Comparable object that the java class name is extracted from
	 * @return A BufferedTreeSet for the clazz instances.
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static BufferedTreeSet getBigSackSet(Comparable clazz) throws IllegalAccessException, IOException {
		return getBigSackSet(clazz.getClass());
	}
	/**
	 * Get a TreeSet via Java Class type.
	 * @param clazz The Java Class of the intended database
	 * @return The BufferedTreeSet for the clazz type.
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static BufferedTreeSet getBigSackSet(Class clazz) throws IllegalAccessException, IOException {
		String xClass = translateClass(clazz.getName());
		BufferedTreeSet ret = classToIsoTreeSet.get(xClass);
		if(DEBUG)
			System.out.println("BigSackAdapter.getBigSackSet About to return designator: "+tableSpaceDir+xClass+" formed from "+clazz.getClass().getName());
		if( ret == null ) {
			ret =  new BufferedTreeSet(tableSpaceDir+xClass, (remoteDir != null ? remoteDir+xClass : null), Props.toInt("L3Cache"));
			classToIsoTreeSet.put(xClass, ret);
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
	public static BufferedTreeMap getBigSackMap(Comparable clazz) throws IllegalAccessException, IOException {
		return getBigSackMap(clazz.getClass());
	}
	/**
	 * Get a TreeMap via Java Class type.
	 * @param clazz The Java Class of the intended database
	 * @return The BufferedTreeMap for the clazz type.
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static BufferedTreeMap getBigSackMap(Class clazz) throws IllegalAccessException, IOException {
		String xClass = translateClass(clazz.getName());
		BufferedTreeMap ret = classToIsoTreemap.get(xClass);
		if(DEBUG)
			System.out.println("BigSackAdapter.getBigSackMap About to return designator: "+tableSpaceDir+xClass+" formed from "+clazz.getClass().getName());
		if( ret == null ) {
			ret =  new BufferedTreeMap(tableSpaceDir+xClass, (remoteDir != null ? remoteDir+xClass : null), Props.toInt("L3Cache"));
			classToIsoTreemap.put(xClass, ret);
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
	public static TransactionalTreeSet getBigSackSetTransaction(Comparable clazz) throws IllegalAccessException, IOException {
		return getBigSackSetTransaction(clazz.getClass());
	}
	/**
	 * Get a TransactionalTreeSet via Java Class type.
	 * @param clazz The Java Class of the intended database
	 * @return The TransactionalTreeSet for the clazz type.
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static TransactionalTreeSet getBigSackSetTransaction(Class clazz) throws IllegalAccessException, IOException {
		String xClass = translateClass(clazz.getName());
		TransactionalTreeSet ret = classToIsoXTreeset.get(xClass);
		if(DEBUG)
			System.out.println("BigSackAdapter.getBigSackSetTransaction About to return designator: "+tableSpaceDir+xClass+" formed from "+clazz.getClass().getName());
		if( ret == null ) {
			ret =  new TransactionalTreeSet(tableSpaceDir+xClass, (remoteDir != null ? remoteDir+xClass : null), Props.toInt("L3Cache"));
			classToIsoXTreeset.put(xClass, ret);
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
	public static TransactionalTreeMap getBigSackMapTransaction(Comparable clazz) throws IllegalAccessException, IOException {
		return getBigSackMapTransaction(clazz.getClass());
	}
	/**
	 * Get a TransactionalTreeMap via Java Class type.
	 * @param clazz The Java Class of the intended database.
	 * @return The TransactionalTreeMap for the clazz type.
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static TransactionalTreeMap getBigSackMapTransaction(Class clazz) throws IllegalAccessException, IOException {
		String xClass = translateClass(clazz.getName());
		TransactionalTreeMap ret = classToIsoXTreemap.get(xClass);
		if(DEBUG)
			System.out.println("BigSackAdapter.getBigSackMapTransaction About to return designator: "+tableSpaceDir+xClass+" formed from "+clazz.getClass().getName());
		if( ret == null ) {
			ret =  new TransactionalTreeMap(tableSpaceDir+xClass, (remoteDir != null ? remoteDir+xClass : null), Props.toInt("L3Cache"));
			classToIsoXTreemap.put(xClass, ret);
		}
		return ret;
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
