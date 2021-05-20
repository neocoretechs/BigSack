package com.neocoretechs.bigsack.test;

import com.neocoretechs.bigsack.session.SessionManager;
/**
 * Perform an analysis on the database, tablespace by tablespace, acquiring stats
 * on block utilization and checking for strangeness to a degree.
 * @author jg
 *
 */
public class AnalyzeDB {
	public static void main(String[] args) throws Exception {
		if( args.length < 2) {
			System.out.println("analyzedb <database> <true | false verbose>");
			System.exit(1);
		}
		System.out.println("Proceeding to analyze "+args[0]);
		// init with no recovery
		SessionManager.analyze(args[0], args[1].equals("true") ? true : false);
	}
}
