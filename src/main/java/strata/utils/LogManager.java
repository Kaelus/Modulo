package strata.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class LogManager {

	protected String[] logFileNames;
	
	public LogManager() {
		
	}

	public boolean setLogFiles(String[] logFileNames) {
		// sanity check
		if (logFileNames == null) {
			System.err.println("logFileNames is null");
			return false;
		} else if (logFileNames.length == 0) {
			System.err.println("logFileNames is empty");
			return false;
		}
		// sanity check done
		this.logFileNames = logFileNames;
		return true;
	}
	
	public String grepLogFiles(int logID, String delimiter, int designatedPosition, String marker) {
		// sanity check
		if (logID < 0 || logID >= logFileNames.length) {
			System.err.println("logID is out of range=" + logID + " logFileNames.length=" + logFileNames.length);
			return null;
		} else if (designatedPosition < 0) {
			System.err.println("designatedPosition must be non-negative number=" + designatedPosition);
			return null;
		} else if (delimiter == null || delimiter.isEmpty()) {
			System.err.println("delimiter is either null or an empty string. delimiter=" + delimiter); 
		} else if (marker == null || marker.isEmpty()) {
			System.err.println("marker is either null or an empty string. marker=" + marker);
		}
		// sanity check done
		String retStr = null;
		String logFileName = logFileNames[logID];
		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(logFileName));
			String line;
	        while ((line = reader.readLine()) != null) {
	        	String[] token = line.split(delimiter);
	        	System.out.println("line=" + line);
	        	System.out.println("token length=" + token.length);
	            if (token.length > designatedPosition) {
	            	System.out.println("trimed designatedPosition token=" + token[designatedPosition].trim());
	                if (token[designatedPosition].trim().equals(marker)) {
	                	retStr = line;
	                }
	            }
	        }
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		return retStr;
	}
	
	// testing purpose
	public static void main(String[] args) {
		// test grepLogFiles
		LogManager lmgr = new LogManager();
		String[] logFileNames = new String[3];
		logFileNames[0] = "/home/ben/project/vcon/imple-test/testlog0";
		logFileNames[1] = "/home/ben/project/vcon/imple-test/testlog1";
		logFileNames[2] = "/home/ben/project/vcon/imple-test/testlog2";
		lmgr.setLogFiles(logFileNames);
		String retStr = null;
		retStr = lmgr.grepLogFiles(0, "-", 3, "LEADER");
		if (retStr == null) {
			System.out.println("Test 1 failed in LogManager. retStr is null");
		} else if (retStr.equals("Hello - World - ! - LEADER - MY ROLE IS..")) {
			System.out.println("retStr=" + retStr);
			System.out.println("Test 1 Passed");
		} else {
			System.out.println("retStr=" + retStr);
			System.out.println("Test 1 failed in LogManager");
		}
		retStr = lmgr.grepLogFiles(1, "-", 3, "FOLLOWER");
		if (retStr.equals("Hello - World - ! - FOLLOWER - MY ROLE IS..")) {
			System.out.println("Test 2 Passed");
		} else {
			System.out.println("Test 2 failed in LogManager");
		}
		retStr = lmgr.grepLogFiles(2, "-", 2, "FOLLOWER");
		if (retStr == null) {
			System.out.println("Test 3 Passed");
		} else {
			System.out.println("Test 3 failed in LogManager");
		}
	}
}
