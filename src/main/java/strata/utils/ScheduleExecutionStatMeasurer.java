package strata.utils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/*
 * This measures various time duration taken to perform actions involved in
 * each schedule execution. For the cumulative stats for the entire experiment
 * encompassing multiple schedule executions must be obtained by a separate means.
 * 
 */
public class ScheduleExecutionStatMeasurer {

	public enum StatOperationType {MIN, MAX, AVG, CUM};
	
	protected FileOutputStream statResultFile;
	
	protected class StatRecord {
		public String statRecordName;
		public int statID;
		protected long startTime;
		protected long endTime;
		protected long timeDur;
		
		protected StatRecord(int statID, String name) {
			this.statID = statID;
			this.statRecordName = name;
		}
		
		public String toString() {
			String retStr = "";
			retStr += "[" + statRecordName + "]=";
			retStr += "statID:" + statID + " ";
			retStr += "startTime:" + startTime + " ";
			retStr += "endTime:" + endTime + " ";
			retStr += "timeDur:" + timeDur;
			return retStr;
		}
	}
	
	private HashMap<Integer, StatRecord> statStore;
	
	public ScheduleExecutionStatMeasurer() {
		statStore = new HashMap<Integer, StatRecord>();
	}
	
	public int startTimeMeasure(String name) {
		StatRecord rec = new StatRecord(statStore.size(), name);
		rec.startTime = System.currentTimeMillis();
		statStore.put(rec.statID, rec);
		return rec.statID;
	}
	
	public long endTimeMeasure(int statID) {
		StatRecord rec = statStore.get(statID);
		rec.endTime = System.currentTimeMillis();
		rec.timeDur = rec.endTime - rec.startTime;
		//assert: cumTime > 0
		if (rec.timeDur <= 0) {
			System.err.println("ASSERT Failed: cumTime > 0");
			System.exit(1);
		}
		return rec.timeDur; 
	}
	
	public StatRecord getTimeMeasure(int statID) {
		return statStore.get(statID);
	}
	
	public void printSimpleStatsToFile(String filePath, List<Integer> statIDs) {
		try {
			System.out.println("saveResult entered");
           	statResultFile = new FileOutputStream(filePath);
           	for (Integer statID : statIDs) {
           		StatRecord rec = statStore.get(statID);
           		String strToPrint = "";
           		strToPrint += rec.statRecordName + "=" + rec.timeDur;
           		statResultFile.write(strToPrint.getBytes());
           		statResultFile.write("\n".getBytes());
           	}
            statResultFile.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
	}
	
	public long computeStat(StatOperationType soType, List<Integer> statIDs) {
		long retStat = -1010101010;
		switch (soType) {
		case MIN:
			for (int i = 0; i < statIDs.size(); i++) {
				StatRecord rec = statStore.get(statIDs.get(i));
				if (i == 0 || retStat > rec.timeDur) {
					retStat = rec.timeDur;
				}
			}
			break;
		case MAX:
			for (int i = 0; i < statIDs.size(); i++) {
				StatRecord rec = statStore.get(statIDs.get(i));
				if (i == 0 || retStat < rec.timeDur) {
					retStat = rec.timeDur;
				}
			}
			break;
		case AVG:
			long cumTimeDur = 0;
			for (int i = 0; i < statIDs.size(); i++) {
				StatRecord rec = statStore.get(statIDs.get(i));
				cumTimeDur += rec.timeDur;
			}
			retStat = cumTimeDur / statIDs.size();
			break;
		case CUM:
			cumTimeDur = 0;
			for (int i = 0; i < statIDs.size(); i++) {
				StatRecord rec = statStore.get(statIDs.get(i));
				cumTimeDur += rec.timeDur;
			}
			retStat = cumTimeDur;
			break;
		default:
			break;
		}
		return retStat;
	}
	
	public String toString() {
		String retStr = "";
		Iterator<Integer> statIter = statStore.keySet().iterator();
		while (statIter.hasNext()) {
			StatRecord rec = statStore.get(statIter.next());
			retStr += rec.toString() + "\n";
		}
		return retStr;
	}
	
}
