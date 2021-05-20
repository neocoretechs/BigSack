package com.neocoretechs.bigsack.test;

import com.neocoretechs.bigsack.session.SessionManager;
/**
 * Perform a backward scan of log files.
 * @author jg
 *
 */
public class AnalyzeLogs {
	public static void main(String[] args) throws Exception {
		// init with no recovery
		SessionManager.AnalyzeLogs(args[0]);
	}
}
