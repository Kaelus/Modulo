package strata.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
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

	static String hayScheduleFilePath = "/home/ben/project/drmbt/scheduleGen/schedules_zk_3.4.11_q-c-drm/scheduleFile-5-3";
	
	static String needleScheduleFilePath = "/home/ben/experiment/zk_performance_measure/test-5-3-ZooKeeper-3.4.11-strata-0.1/progSched";
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
			    		String rawDNStr = strTokens[2];
			    		String filteredDNStr = rawDNStr.replaceAll("\\[|\\]", "").replaceAll(",", "");
			    		String[] dnStrArr = filteredDNStr.split(" ");
			    		int[] dn = new int[dnStrArr.length];
			    		for (int i = 0; i < dn.length; i++) {
			    			dn[i] = Integer.parseInt(dnStrArr[i]);
			    		}
			    		Path resyncPath = new ResyncPath(dn);
			    		needleSchedule.sched.add(resyncPath);
			    	} else {
			    		System.err.println("Parse Error!");
			    		System.exit(1);
			    	}
			    }
			}
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
