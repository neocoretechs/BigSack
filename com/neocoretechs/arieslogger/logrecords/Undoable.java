/*

    - Class com.neocoretechs.arieslogger.logrecords.Undoable

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

package com.neocoretechs.arieslogger.logrecords;

import java.io.IOException;

import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;

/**
	An Undoable operation is an operation that changed the state of the RawStore
	in the context of a transaction and this change can be rolled back.

	@see Transaction#logAndDo
	@see Compensation
*/

public interface Undoable extends Loggable {


	/**
		Generate a loggable which will undo this change, using the optional
		input if necessary.

		<P><B>NOTE</B><BR>Any logical undo logic must be hidden behind generateUndo.
		During recovery redo, it should not depend on any logical undo logic.

		<P>
		There are 3 ways to implement a redo-only log record:
		<NL>
		<LI>Make the log record a Loggable instead of an Undoable, this is the
		cleanest method.
		<LI>If you want to extend a log operation class that is an Undoable,
		you can then either have generateUndo return null - this is preferred -
		(the log operation's applyChange should never be called, so you can put a
		null body there if the super class you are extending does not implement
		a applyChange).
		<LI>Or, have applyChange do nothing - this is least preferred.
		</NL>

		<P>Any resource (e.g., latched page) that is needed for the
		undoable.applyChange() must be acquired in undoable.generateUndo().
		Moreover, that resource must be identified in the compensation
		operation, and reacquired in the compensation.needsRedo() method during
		recovery redo.
		<BR><B>If you do write your own generateUndo or needsRedo, any
		resource you latch or acquire, you must release them in
		Compensation.applyChange() or in Compensation.releaseResource().</B>

		<P> To write a generateUndo operation, find the object that needs to be
		rolled back.  Assuming that it is a page, latch it, put together a
		Compensation operation with the undoOp set to this operation, and save
		the page number in the compensation operation, then
		return the Compensation operation to the logging system.

		<P>
		The sequence of events in a rollback of a undoable operation is
		<NL>
		<LI> The logging system calls undoable.generateUndo.  If this returns
		null, then there is nothing to undo.
		<LI> If generateUndo returns a Compensation operation, then the logging
		system will log the Compensation log record and call
		Compenstation.applyChange().  (Hopefully, this just calls the undoable's applyChange)
		<LI> After the Compensation operation has been applied, the logging
		system will call compensation.releaseResource(). If you do overwrite a
		super class's releaseResource(), it would be prudent to call
		super.releaseResource() first.
		</NL>

		<P> The available() method of in indicates how much data can be read, i.e.
		how much was originally written.

		@param t	the transaction doing the rollback
		@return the compensation operation that will rollback this change, or
		null if nothing to undo. 

		@exception IOException Can be thrown by any of the methods of ObjectInput.

		@see Loggable#releaseResource
		@see Loggable#needsRedo

	*/
	public Compensation generateUndo(GlobalDBIO t) throws IOException;

}
