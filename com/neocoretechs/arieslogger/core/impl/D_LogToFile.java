/*

    - Class com.neocoretechs.arieslogger.core.D_LogToFile

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



public class D_LogToFile {

	/**
	  @exception StandardException Oops.
	  @see Diagnosticable#diag
	  */
    public String diag(LogToFile ltf)
 		 throws Exception
    {
		StringBuffer r = new StringBuffer();
		r.append("LogToFile: \n");
		r.append("    Directory: "+ltf.getDataDirectory()+"\n");
		r.append("    endPosition: "+ltf.endPosition()+"\n");
		r.append("    lastFlush(offset): "+ltf.getLastFlush()+"\n");
		r.append("    logFileNumber: "+ltf.getLogFileNumber()+"\n");
		r.append("    firstLogFileNumber: "+ltf.getFirstLogFileNumber()+"\n");
		return r.toString();
	}
}



