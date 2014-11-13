/*

    - Class com.neocoretechs.arieslogger.core.D_FlushedScan

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.neocoretechs.arieslogger.core.impl;


public class D_FlushedScan

{
	/**
	  @exception StandardException Oops.
	  @see Diagnosticable#diag
	  */
    public String diag(FlushedScan fs) throws Exception
    {
		StringBuffer r = new StringBuffer();
		r.append("FlushedScan: \n");
		r.append("    Open: "+fs.isOpen()+"\n");
		r.append("    currentLogFileNumber: "+fs.getCurrentLogFileNumber()+"\n");
		r.append("    currentLogFirstUnflushedPosition: "+
				 fs.getCurrentLogFileFirstUnflushedPosition()+"\n");
		r.append("    currentInstance: "+fs.getCurrentInstance()+"\n");
		r.append("    firstUnflushed: "+fs.getFirstUnflushed()+"\n");
		r.append("    firstUnflushedFileNumber: "+fs.getFirstUnflushedFileNumber()+"\n");
		r.append("    firstUnflushedFilePosition: "+fs.getFirstUnflushedFilePosition()+"\n");
		r.append("    logFactory: \n"+fs.getLogFactory());
		r.append("flushedScanEnd\n");
		return r.toString();
	}
}
