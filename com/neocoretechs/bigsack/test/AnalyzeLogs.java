package com.neocoretechs.bigsack.test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import com.neocoretechs.arieslogger.core.LogInstance;
import com.neocoretechs.arieslogger.core.impl.LogCounter;
import com.neocoretechs.arieslogger.core.impl.LogRecord;
import com.neocoretechs.arieslogger.core.impl.LogToFile;
import com.neocoretechs.arieslogger.core.impl.Scan;
import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.pooled.ObjectDBIO;
import com.neocoretechs.bigsack.session.BigSackSession;
import com.neocoretechs.bigsack.session.SessionManager;

public class AnalyzeLogs {
	public static void main(String[] args) throws Exception {
		// init with no recovery
		BigSackSession bss = SessionManager.ConnectNoRecovery(args[0], null);
		ObjectDBIO gdb = bss.getBTree().getIO();
		LogCounter startAt = new LogCounter(1,LogToFile.LOG_FILE_HEADER_SIZE);
		Scan ls;
		for(int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			ls = (Scan) gdb.getIOManager().getUlog(i).getLogToFile().openForwardScan(startAt, null);
			HashMap<LogInstance, LogRecord> records = null;
			// backward scan records in reverse order
			while ((records =  ls.getNextRecord(0)) != null) 
			{
				Iterator<Entry<LogInstance, LogRecord>> irecs = records.entrySet().iterator();
				while(irecs.hasNext()) {
					Entry<LogInstance, LogRecord> recEntry = irecs.next();
					LogRecord record = recEntry.getValue();
					System.out.println(record);
				}
			}
		}
	}
}
