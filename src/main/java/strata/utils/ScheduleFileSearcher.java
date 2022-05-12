package strata.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;

import strata.common.DeviationPath;
import strata.common.Path;
import strata.common.ResyncPath;
import strata.common.StrataSchedule;

/**
 * Given Schedule, find the schedule file containing and index of the given schedule
 * in the directory of schedule files.
 * 
 * @author ben
 *
 */
public class ScheduleFileSearcher {

	static String hayScheduleFilePath = "/home/ben/project/drmbt/scheduleGen/scheduleFile-1-3";
	
	static String needleScheduleFilePath = "/home/ben/experiment/test-1-3-Redis-4.0.0-strata-0.1/progSched";
	static StrataSchedule needleSchedule;
	
	public static void loadNeedleSchedule() {
		try {
			File fi = new File(needleScheduleFilePath);
			needleSchedule = new StrataSchedule();
			try (BufferedReader br = new BufferedReader(new FileReader(fi))) {
			    String line;
			    while ((line = br.readLine()) != null) {
			    	String[] strTokens = line.split("=");
			    	String typeStr = strTokens[1].split(" ")[0];
			    	if (typeStr.equals("DEVIATION")) {
			    		String rawTSStr = strTokens[2];
			    		String filteredTSStr = rawTSStr.replaceAll("\\[|\\]", "").replaceAll(",", "");
			    		String[] tsStrArr = filteredTSStr.split(" ");
			    		int[] ts = new int[tsStrArr.length];
			    		for (int i = 0; i < ts.length; i++) {
			    			ts[i] = Integer.parseInt(tsStrArr[i]);
			    		}
			    		Path devPath = new DeviationPath(ts);
			    		needleSchedule.sched.add(devPath);
			    	} else if (typeStr.equals("RESYNC")) {
			    		//System.out.println("strTokens" + Arrays.toString(strTokens));
			    		String rawDNStr = strTokens[2].split("]")[0];
			    		//System.out.println("rawDNStr=" + rawDNStr);
			    		String filteredDNStr = rawDNStr.replaceAll("\\[|\\]", "").replaceAll(",", "");
			    		//System.out.println("filteredDNStr=" + filteredDNStr);
			    		String[] dnStrArr = filteredDNStr.split(" ");
			    		int[] dn = new int[dnStrArr.length];
			    		for (int i = 0; i < dn.length; i++) {
			    			dn[i] = Integer.parseInt(dnStrArr[i]);
			    		}
			    		String rawSourcesStr = strTokens[3].split("]")[0];
			    		//System.out.println("rawSourcesStr=" + rawSourcesStr);
			    		String filteredSourcesStr = rawSourcesStr.replaceAll("\\[|\\]", "").replaceAll(",", "");
			    		//System.out.println("filteredSourcesStr=" + filteredSourcesStr);
			    		String[] sourcesStrArr = filteredSourcesStr.split(" ");
			    		int[] syncSources = new int[sourcesStrArr.length];
			    		for (int i = 0; i < syncSources.length; i++) {
			    			syncSources[i] = Integer.parseInt(sourcesStrArr[i]);
			    		}
			    		String rawTargetsStr = strTokens[4];
			    		//System.out.println("rawTargetsStr=" + rawTargetsStr);
			    		String filteredTargetsStr = rawTargetsStr.replaceAll("\\[|\\]", "").replaceAll(",", "");
			    		//System.out.println("filteredTargetsStr=" + filteredTargetsStr);
			    		String[] targetsStrArr = filteredTargetsStr.split(" ");
			    		int[] syncTargets = new int[targetsStrArr.length];
			    		for (int i = 0; i < syncTargets.length; i++) {
			    			syncTargets[i] = Integer.parseInt(targetsStrArr[i]);
			    		}
			    		Path resyncPath = new ResyncPath(dn, syncSources, syncTargets);
			    		needleSchedule.sched.add(resyncPath);
			    	} else {
			    		System.err.println("Parse Error!");
			    		System.exit(1);
			    	}
			    }
			}
		} catch (FileNotFoundException fnfe) {
			System.err.println("FileNotFoundException!!");
			System.exit(1);
		} catch (IOException ioe) {
			//log.warn("IOException occurred while reading objects. Treat it as EOF!");
			System.out.println("EOF! We read all Schedules in this file");
		}
	}
	
	public static void main(String[] args){
		File folder = new File(hayScheduleFilePath);
		//int curSchedIndex;
		int schedCounter = 0;
		StrataSchedule inSched = null;
		boolean foundMatch = false;
		LinkedList<StrataSchedule> currentSchedules = null;
		loadNeedleSchedule();
		System.out.println("loaded needle schedule=" + needleSchedule.toString());
		if (needleSchedule.sched.isEmpty()) {
			System.err.println("ASSERTION: needleSchedule.sched is empty!");
			System.exit(1);
		}
		File[] fileList = folder.listFiles();
		Arrays.sort(fileList, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                int n1 = extractNumber(o1.getName());
                int n2 = extractNumber(o2.getName());
                return n1 - n2;
            }

            private int extractNumber(String name) {
                int i = 0;
                try {
                    int s = name.indexOf('N')+5;
                    int e = name.length();
                    String number = name.substring(s, e);
                    i = Integer.parseInt(number);
                } catch(Exception e) {
                    i = 0; // if filename does not match the format
                           // then default to 0
                }
                return i;
            }
        });
		for (File fileEntry : fileList) {
			System.out.println("Scnanning file=" + fileEntry.getName());
			//curSchedIndex = -1;
			int i=0;
			currentSchedules = new LinkedList<StrataSchedule>();
			try {
				FileInputStream fileIn = new FileInputStream(fileEntry);
				ObjectInputStream in = new ObjectInputStream(fileIn);
				//log.info("Schedules:");
				while ((inSched = (StrataSchedule) in.readObject()) != null) {
					//log.info(i++ + "-th Schedule:\n");
					//log.info("" + inSched.toString());
					currentSchedules.add(inSched);
					i++;
				}
				System.out.println("There are " + i + " many schedules in the file");
				in.close();
			} catch (IOException ioe) {
				//log.warn("IOException occurred while reading objects. Treat it as EOF!");
				//System.out.println("EOF! We read all Schedules in this file");
				System.out.println("There are " + i + " many schedules in the file");
				System.out.println("msg=" + ioe.getMessage());
				ioe.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Iterator<StrataSchedule> iter = currentSchedules.iterator();
			int index = 0;
			while (iter.hasNext()) {
				StrataSchedule sch = iter.next();
				schedCounter++;
				index++;
				if (sch.equals(needleSchedule)) {
					System.out.println("Found the needle in the haystack!");
					System.out.println("needle=" + needleSchedule.toString());
					System.out.println("Schedule File Name=" + fileEntry.getName());
					System.out.println("index of the schedule=" + index);
					foundMatch = true;
					break;
				}
			}
			if (foundMatch) {
				break;
			}
		}
		System.out.println("Search completed");
		if (foundMatch) {
			System.out.println("We found the match! after checking as many schedules as " + schedCounter);
		} else {
			System.err.println("We CANNOT found the match!");
		}
	}
	
}
