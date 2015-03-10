package com.neocoretechs.bigsack.test;

import com.neocoretechs.bigsack.session.BigSackSession;
import com.neocoretechs.bigsack.session.SessionManager;
/**
 * Perform an analysis on the database, tablespace by tablespace, acquiring stats
 * on block utilization and checking for strangeness to a degree
 * @author jg
 *
 */
public class AnalyzeDB {
	public static void main(String[] args) throws Exception {
		// init with no recovery
		BigSackSession bss = SessionManager.ConnectNoRecovery(args[0], null);
		bss.analyze(true);
	}
}
