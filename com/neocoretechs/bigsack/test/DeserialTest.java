package com.neocoretechs.bigsack.test;

import com.neocoretechs.bigsack.session.SessionManager;
/**
 * Perform an analysis on the database, deserialize object at location
 * @author jg
 *
 */
public class DeserialTest {
	public static void main(String[] args) throws Exception {
		if( args.length < 2) {
			System.out.println("analyzedb <database> <BTree, HMap db type> <tablespace> <block>");
			System.exit(1);
		}
		System.out.println("Proceeding to analyze "+args[0]);
		// init with no recovery
		System.out.println(SessionManager.deserial(args[0], args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3])));

	}
}
