package com.neocoretechs.bigsack;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
/*
* Copyright (c) 2003, NeoCoreTechs
* All rights reserved.
* Redistribution and use in source and binary forms, with or without modification, 
* are permitted provided that the following conditions are met:
*
* Redistributions of source code must retain the above copyright notice, this list of
* conditions and the following disclaimer. 
* Redistributions in binary form must reproduce the above copyright notice, 
* this list of conditions and the following disclaimer in the documentation and/or
* other materials provided with the distribution. 
* Neither the name of NeoCoreTechs nor the names of its contributors may be 
* used to endorse or promote products derived from this software without specific prior written permission. 
* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED 
* WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A 
* PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR 
* ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
* TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
* HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT 
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED 
* OF THE POSSIBILITY OF SUCH DAMAGE.
*
*/
/**
 * Load and retrieve from properties file for configuration purposes.
 * We assume the the properties file is in BigSack.Properties which we try
 * to locate through system.getProperty("BigSack.properties") and barring that,
 * attempt to load from the system resource stream.
 * @author Groff
 */
public class Props {
	private static final String propsFile = "BigSack.properties";
	private static String propfile = null;
	public static boolean DEBUG = false;
	/**
	 * assume properties file is in 'BigSack.properties'
	 */
	static {
		try {
			String file = System.getProperty(propsFile);
			if (file == null)
				init(top());
			else
				init(new FileInputStream(file));
		} catch (IOException ioe) {
			throw new RuntimeException(ioe.toString());
		}
	}
	
	public static String getPropFile() {
		return propfile;
	}

	/**
	 * Load the properties from the stream
	 * @param propFile The stream for reading properties
	 * @exception IOException If the read fails
	 */
	public static void init(InputStream propFile) throws IOException {
		try {
			System.getProperties().load(propFile);
		} catch (Exception ex) {
			throw new IOException(
				"FATAL ERROR:  unable to load "+propsFile+" file "
					+ ex.toString());
		}
	}

	/**
	 * @param prop The property to retrieve
	 * @return The property as a boolean
	 * @exception IllegalArgumentException if not set. 
	 **/
	public static boolean toBoolean(String prop) {
		String val = Props.toString(prop);
		try {
			return Boolean.valueOf(val).booleanValue();
		} catch (Exception ex) {
			throw new IllegalArgumentException(
				"invalid value "
					+ val
					+ " for property "
					+ prop
					+ " (expected true/false)");
		}
	}

	/** 
	 * @param prop The property to retrieve
	 * @return The property as an int 
	 * @exception IllegalArgumentException if not set. 
	 **/
	public static int toInt(String prop) {
		String val = Props.toString(prop);
		try {
			return Integer.parseInt(val);
		} catch (NumberFormatException ex) {
			throw new IllegalArgumentException(
				"invalid value "
					+ val
					+ " for property "
					+ prop
					+ " (expected integer)");
		}
	}

	/** 
	 * @param prop The property to retrieve
	 * @return The property as a long 
	 * @exception IllegalArgumentException if not set. 
	 **/
	public static long toLong(String prop) {
		String val = Props.toString(prop); // can hurl
		try {
			return Long.parseLong(val);
		} catch (NumberFormatException ex) {
			throw new IllegalArgumentException(
				"invalid value "
					+ val
					+ " for property "
					+ prop
					+ " (expected long)");
		}
	}

	/**
	 * Find the top level resource for props
	 * @return the InputStream of property resource
	 * @exception IOException if we can't get the resource
	 */
	public static InputStream top() throws IOException {
		java.net.URL loader =
			ClassLoader.getSystemResource(propsFile);
		if (loader == null) {
			propfile = System.getProperty(propsFile); // now we look for -DBigSack.properties= on cmdl
			if( propfile == null ) // nowhere left to turn
				throw new IOException("FATAL ERROR:  unable to load "+propsFile+" file: not found on resource path");
		}
		return loader.openStream();
	}

	/** 
	 * @param prop The property to retrieve
	 * @return The property as a String 
	 * @exception IllegalArgumentException if not set. 
	 **/	
	public static String toString(String prop) {
		String result = System.getProperty(prop);
		if (result == null)
			throw new IllegalArgumentException("property " + prop + " not set");
		if (result != null)
			result = result.trim();
		return result;
	}

}
