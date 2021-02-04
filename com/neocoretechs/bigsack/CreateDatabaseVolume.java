package com.neocoretechs.bigsack;

import java.io.File;
import java.io.FilePermission;
import java.io.IOException;


/**
 * 
 * Create the relevant directories o support the creation of databases under the stated volume<p/>
 * The volume is a directory path. The subsequent database will then be referenced as <directory path>/database.
 * @author Jonathan Groff (C) NeoCoreTechs 2021
 *
 */
public class CreateDatabaseVolume {

	public static void main(String[] args) throws IOException {
		if(args.length < 1) {
			System.out.println("Supply an argument for the volume name!");
			System.out.println( "The volume is a directory path. The subsequent database will then be referenced as <directory path>/database.");
			return;
		}
		FilePermission fp;
		String log,tablespace0,tablespace1,tablespace2,tablespace3,tablespace4,tablespace5,tablespace6,tablespace7;
		if(args[0].endsWith("/")) {
			log = args[0]+"Log/";
			tablespace0 = args[0]+"tablespace0/";
			tablespace1 = args[0]+"tablespace1/";
			tablespace2 = args[0]+"tablespace2/";
			tablespace3 = args[0]+"tablespace3/";
			tablespace4 = args[0]+"tablespace4/";
			tablespace5 = args[0]+"tablespace5/";
			tablespace6 = args[0]+"tablespace6/";
			tablespace7 = args[0]+"tablespace7/";
		} else {
			log = args[0]+"/Log/";
			tablespace0 = args[0]+"/tablespace0/";
			tablespace1 = args[0]+"/tablespace1/";
			tablespace2 = args[0]+"/tablespace2/";
			tablespace3 = args[0]+"/tablespace3/";
			tablespace4 = args[0]+"/tablespace4/";
			tablespace5 = args[0]+"/tablespace5/";
			tablespace6 = args[0]+"/tablespace6/";
			tablespace7 = args[0]+"/tablespace7/";
		}
		new File(log).mkdirs();
		fp = new FilePermission(log+"-", "read,write,execute,delete");
		fp.checkGuard(null);
		
		new File(tablespace0).mkdirs();
		fp = new FilePermission(tablespace0+"-", "read,write,execute,delete");
		fp.checkGuard(null);
		
		new File(tablespace1).mkdirs();
		fp = new FilePermission(tablespace1+"-", "read,write,execute,delete");
		fp.checkGuard(null);
		
		new File(tablespace2).mkdirs();
		fp = new FilePermission(tablespace2+"-", "read,write,execute,delete");
		fp.checkGuard(null);
		
		new File(tablespace3).mkdirs();
		fp = new FilePermission(tablespace3+"-", "read,write,execute,delete");
		fp.checkGuard(null);
		
		new File(tablespace4).mkdirs();
		fp = new FilePermission(tablespace4+"-", "read,write,execute,delete");
		fp.checkGuard(null);
		
		new File(tablespace5).mkdirs();
		fp = new FilePermission(tablespace5+"-", "read,write,execute,delete");
		fp.checkGuard(null);
		
		new File(tablespace6).mkdirs();
		fp = new FilePermission(tablespace6+"-", "read,write,execute,delete");
		fp.checkGuard(null);
		
		new File(tablespace7).mkdirs();
		fp = new FilePermission(tablespace7+"-", "read,write,execute,delete");
		fp.checkGuard(null);
		
		System.out.println("Ok");
	}

}
