package strata.aigen;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import strata.common.AbstractState;
import strata.common.DeviationPath;
import strata.common.Path;
import strata.common.PathTuple;
import strata.common.ResyncPath;
import strata.common.StrataSchedule;
import strata.utils.MultisetGenerator;
import strata.utils.Permutation;

public class ScheduleGenerator {

	/***************************************
	 *  Declaration 
	 *   - constants, variables
	 ***************************************/
	String schedDirName; //= "/home/ben/project/vcon/scheduleGen/";
	String schedFileNamePrefix; // = schedDirName + "scheduleFile/" + "schedules";
	final static int resyncIncrease = 10000;
	final static int scheduleCountLimitsPerFile = 1000;
	
	
	AbstractState curState;
	
	Hashtable<Integer, Set<Path>> enabledPathTable;
	LinkedList<PathTuple> currentInitPath;
	LinkedList<PathTuple> currentExploringPath;
	LinkedList<LinkedList<PathTuple>> initialPaths;
	LinkedList<Path> currentEnabledPaths;
	HashSet<LinkedList<PathTuple>> finishedInitialPaths;
	
	int globalState;
	
	LinkedList<AbstractState> prevAbstractState;
	
	boolean manualPickInitPath;
	boolean saveToFile;
	
	String schedFileName;
	
	int origNumAsyncOp, origNumNode;
	int initPathLen;
	
	List<StrataSchedule> scheduleList;
	int scheduleFileCount;
	
	HashSet<Integer> schedHashSet;
	public int uniqSchedCnt = 0;
	public int dupSchedCnt = 0;
	
	/***************************
	 * Constructor
	 ***************************/
	public ScheduleGenerator(int numAsyncOp, int numNode, String schedDir) {
		this.origNumAsyncOp = numAsyncOp;
		this.origNumNode = numNode;
		this.schedDirName = schedDir;
		this.schedFileNamePrefix = schedDirName + "scheduleFile-" + numAsyncOp 
				+ "-" + numNode + "/schedules";
		File schedDirFile = new File(schedDirName);
		if (!schedDirFile.exists()) {
			if (!schedDirFile.mkdir()) {
				System.err.println("Unable to mkdir " + schedDirFile);
	            System.exit(1);
	        }
		}
		File schedFileFile = new File(schedDirName + "scheduleFile-" + numAsyncOp 
				+ "-" + numNode);
		if (!schedFileFile.exists()) {
			if (!schedFileFile.mkdir()) {
				System.err.println("Unable to mkdir " + schedFileFile);
	            System.exit(1);
	        }
		}
		curState = new AbstractState(numAsyncOp, numNode);
		currentExploringPath = new LinkedList<PathTuple>();
		initialPaths = new LinkedList<LinkedList<PathTuple>>();
		currentEnabledPaths = new LinkedList<Path>();
		globalState = 0;
		prevAbstractState = new LinkedList<AbstractState>();
		enabledPathTable = new Hashtable<Integer, Set<Path>>();
		finishedInitialPaths = new HashSet<LinkedList<PathTuple>>();
		initPathLen = 0;
		scheduleList = new LinkedList<StrataSchedule>();
		scheduleFileCount = 0;
		schedFileName = schedFileNamePrefix + "-" + numAsyncOp + "AOP" + "_" + numNode + "Nodes" + scheduleFileCount;
		//loadInitialPathsFromFile();
	}
	
	
	/*******************************************************************
	 * Auxiliary Functions 
	 *  - used by generate schedule and its helper function
	 *******************************************************************/
	
	protected static int getHighestNodeState(int[] nodeState) {
		int highest = 0;
		for (int i = 0; i < nodeState.length; i++) {
			if (highest < nodeState[i]) {
				highest = nodeState[i];
			}
		}
		return highest;
	}
	
	private static List<Integer> getUniqNodeState(int[] nodeState) {
		List<Integer> stateList = new ArrayList<Integer>();
		for (int i = 0; i < nodeState.length; i++) {
			int state = nodeState[i];
			if (!stateList.contains(state)) {
				stateList.add(state);
			}
		}
		return stateList;
	}

	private static String schedListToString(List<StrataSchedule> schedList) {
		String retStr = "++++++++++++++++++++++++++++++\n";
		retStr += "schedule list contains:\n";
		int i = 0;
		for (StrataSchedule sched : schedList) {
			retStr += "--------<" + i++ + "-th schedule>-------\n";
			retStr += sched.toString() + "\n";
		}
		retStr += "++++++++++++++++++++++++++++++\n";
		return retStr;
	}
	
	private static void filterOutSymmetricPairs(List<int[]> devNodePairs, int[] nodeState
			, int[] syncSources, int[] syncTargets) {
		List<int[]> uniqDevNodeStates = new ArrayList<int[]>();
		//System.out.println("given devNodePairs" + " given nodeState=" + Arrays.toString(nodeState));
		//for (int[] tmp : devNodePairs) {
		//	System.out.println(Arrays.toString(tmp));
		//}
		Iterator<int[]> iter = devNodePairs.iterator();
		while(iter.hasNext()) {
			int[] nodePair = iter.next();
			int[] tmpNodeState = new int[nodePair.length];
			int cnt = 0;
			for (int i : nodePair) {
				tmpNodeState[cnt++] = 37*(37*(37*1 + nodeState[i]) + syncSources[i]) + syncTargets[i];
			}
			Arrays.sort(tmpNodeState);
			//System.out.println("For nodePair=" + Arrays.toString(nodePair));
			//System.out.println("sorted array of the state of those nodes are=" + Arrays.toString(tmpNodeState));
			Iterator<int[]> dnsIter = uniqDevNodeStates.iterator();
			boolean isUnique = true;
			while (dnsIter.hasNext()) {
				int[] dns = dnsIter.next();
				if (!Arrays.equals(dns, tmpNodeState)) {
					//System.out.println("found unique deviated node state");
				} else {
					//System.out.println("found duplicated deviated node state");
					iter.remove();
					isUnique = false;
					break;
				}
			}
			if (uniqDevNodeStates.isEmpty()) {
				//System.out.println("found unique deviated node state");
				uniqDevNodeStates.add(tmpNodeState);
			} else {
				if (isUnique) {
					uniqDevNodeStates.add(tmpNodeState);	
				}
			}
		}
		//System.out.println("after filtering devNodePairs");
		//for (int[] tmp : devNodePairs) {
		//	System.out.println(Arrays.toString(tmp));
		//}
	}
	
	// Obsoleted
	private static List<int[]> getAllDevNodePairs(int[] nodeState, boolean[] onlineStatus) {
		List<int[]> retList = new ArrayList<int[]>();
		//System.out.println("getAllDevNodePairs for " + Arrays.toString(nodeState));
		
		for (int i = 0; i < nodeState.length-1; i++) {
			for (int j = i+1; j < nodeState.length; j++) {
				//System.out.println("i=" + i + " j=" + j);
				//System.out.println("nodeState[" + i + "]=" + nodeState[i]);
				//System.out.println("nodeState[" + j + "]=" + nodeState[j]);
				if (nodeState[i] != nodeState[j]) {
					int[] pair = new int[2];
					pair[0] = i;
					pair[1] = j;
					retList.add(pair);
				} else {
					if (!onlineStatus[i] || !onlineStatus[j]) {
						int[] pair = new int[2];
						pair[0] = i;
						pair[1] = j;
						retList.add(pair);
					}
				}
			}
		}
		//filterOutSymmetricPairs(retList, nodeState);
		//System.out.println("retList is:");
		//for (int[] dn : retList) {
		//	System.out.println(Arrays.toString(dn));
		//}
		return retList;
	}
	
	private static List<int[]> getAllDevNodeCombiHelper(int[] nodeState, boolean[] onlineStatus
				, int[] syncSources, int[] syncTargets) {
		//System.out.println("getAllDevNodePairs for " + Arrays.toString(nodeState));
		List<int[]> retList = new ArrayList<int[]>();	
		int numOnline = 0;
		LinkedList<Object> nodeIDList = new LinkedList<Object>();
		for (int i = 0; i < onlineStatus.length; i++) {
			if (onlineStatus[i]) {
				numOnline++;
			} else {
				nodeIDList.add(i);
			}
		}
		if (numOnline == 0) {
			Permutation.getVConCombination(nodeIDList, 2, nodeIDList.size());
		} else {
			Permutation.getVConCombination(nodeIDList, 1, nodeIDList.size());
		}
    	for (LinkedList<Object> vconCombi : Permutation.vconCumCombiList) {
    		int[] combiArr = new int[vconCombi.size()];
    		for (int j = 0; j < combiArr.length; j++) {
    			combiArr[j] = (int) vconCombi.get(j); 
    		}
    		if (combiArr.length > 0) {
    			retList.add(combiArr);
    		}
    	}
		return retList;
	}
	
	private static List<int[]> getAllDevNodeCombi(int[] nodeState, boolean[] onlineStatus
			, int[] syncSources, int[] syncTargets) {
		List<int[]> retList = getAllDevNodeCombiHelper(nodeState, onlineStatus, syncSources, syncTargets);
		filterOutSymmetricPairs(retList, nodeState, syncSources, syncTargets);
		//System.out.println("retList is:");
		//for (int[] dn : retList) {
		//	System.out.println(Arrays.toString(dn));
		//}
		return retList;
	}

	private static boolean hasCycle(int[] syncSources, int id) {
		boolean result = false;
		ArrayList<Integer> visitedNodes = new ArrayList<Integer>();
		int curNode = id;
		while (curNode != -1) {
			if (visitedNodes.contains(curNode)) {
				System.out.println("will form cycle");
				result = true;
				break;
			}
			visitedNodes.add(curNode);
			curNode = syncSources[curNode];
		}
		return result;
	}
	
	/*
	 * For the given dn and states, this returns the list containing all possible enabled 
	 * resync paths.
	 * 
	 * It does not generate the schedule where the restarted node skip the opportunity to
	 * for the new chain although they won't change the chain once it is formed
	 * 
	 * It makes sure no cycle is formed in the replication chain. 
	 * 
	 * It makes sure that it properly fill out targetSyncSourcesChange and 
	 * targetSyncTargetsChange in each ResyncPath.
	 *  
	 */
	private static List<ResyncPath> getAllDevNodeResyncPaths(int[] dn, int[] curNodeState,
			boolean[] curOnlineStatus, int[] curSyncSources, int[] curSyncTargets) {
		List<ResyncPath> retList = new ArrayList<ResyncPath>();
		boolean chainUpdated = false;
		if (dn.length == 2) {
			// To add resync path 1: dn[0]->dn[1] direction
			ResyncPath newResyncPath = new ResyncPath();
			newResyncPath.type = Path.PathType.RESYNC;
			newResyncPath.devNode = dn;
			int[] tssc = curSyncSources.clone();
			int[] tstc = curSyncTargets.clone();
			// we change syncSources and syncTargets only when the corresponding node does not have
			// the sync source yet and no cycle will be formed
			int[] testSyncSources = curSyncSources.clone();
			if (tssc[dn[1]] == -1) {
				testSyncSources[dn[1]] = dn[0];
				if (!hasCycle(testSyncSources, dn[0])) {
					tssc[dn[1]] = dn[0];
					tstc[dn[0]] = tstc[dn[0]] | (1 << dn[1]); // set the bit at the corresponding index
					newResyncPath.targetSyncSourcesChange = tssc;
					newResyncPath.targetSyncTargetsChange = tstc;
					retList.add(newResyncPath);
					chainUpdated = true;
				}
			}
			
			// To add resync path 2: dn[0]<-dn[1] direction
			newResyncPath = new ResyncPath();
			newResyncPath.type = Path.PathType.RESYNC;
			newResyncPath.devNode = dn;
			tssc = curSyncSources.clone();
			tstc = curSyncTargets.clone();
			// we change syncSources and syncTargets only when the corresponding node does not have
			// the sync source yet and no cycle will be formed
			testSyncSources = curSyncSources.clone();
			if (tssc[dn[0]] == -1) {
				testSyncSources[dn[0]] = dn[1];
				if (!hasCycle(testSyncSources, dn[1])) {
					tssc[dn[0]] = dn[1];
					tstc[dn[1]] = tstc[dn[1]] | (1 << dn[0]); // set the bit at the corresponding index
					newResyncPath.targetSyncSourcesChange = tssc;
					newResyncPath.targetSyncTargetsChange = tstc;
					retList.add(newResyncPath);
					chainUpdated = true;
				}
			}
			
			// To add resync path 3: we don't form a new chain because both nodes are already chained
			if (!chainUpdated) {
				newResyncPath.targetSyncSourcesChange = tssc;
				newResyncPath.targetSyncTargetsChange = tstc;
				retList.add(newResyncPath);
			}
		} else if (dn.length == 1) {
			// To add resync paths:
			for (int i = 0; i < curNodeState.length; i++) {
				if (curOnlineStatus[i]) {
					// To add resync path 1: i->dn[0] direction
					ResyncPath newResyncPath = new ResyncPath();
					newResyncPath.type = Path.PathType.RESYNC;
					newResyncPath.devNode = dn;
					int[] tssc = curSyncSources.clone();
					int[] tstc = curSyncTargets.clone();
					// we change syncSources and syncTargets only when the corresponding node does not have
					// the sync source yet and no cycle will be formed
					int[] testSyncSources = curSyncSources.clone();
					if (tssc[dn[0]] == -1) {
						testSyncSources[dn[0]] = i;
						if (!hasCycle(testSyncSources, i)) {
							tssc[dn[0]] = i;
							tstc[i] = tstc[i] | (1 << dn[0]); // set the bit at the corresponding index
							newResyncPath.targetSyncSourcesChange = tssc;
							newResyncPath.targetSyncTargetsChange = tstc;
							retList.add(newResyncPath);
							chainUpdated = true;
						}
					}
					
					// To add resync path 2: i<-dn[0] direction
					newResyncPath = new ResyncPath();
					newResyncPath.type = Path.PathType.RESYNC;
					newResyncPath.devNode = dn;
					tssc = curSyncSources.clone();
					tstc = curSyncTargets.clone();
					// we change syncSources and syncTargets only when the corresponding node does not have
					// the sync source yet
					testSyncSources = curSyncSources.clone();
					if (tssc[i] == -1) {
						testSyncSources[i] = dn[0];
						if (!hasCycle(testSyncSources, dn[0])) {
							tssc[i] = dn[0];
							tstc[dn[0]] = tstc[dn[0]] | (1 << i); // set the bit at the corresponding index
							newResyncPath.targetSyncSourcesChange = tssc;
							newResyncPath.targetSyncTargetsChange = tstc;
							retList.add(newResyncPath);
							chainUpdated = true;
						}
					}
					
					// To add resync path 3: we don't form a new chain because both nodes are already chained
					if (!chainUpdated) {
						newResyncPath.targetSyncSourcesChange = tssc;
						newResyncPath.targetSyncTargetsChange = tstc;
						retList.add(newResyncPath);
					}
				}
			}
		} else {
			System.err.println("ERROR dn has an incorrect length which is=" + dn.length + " dn=" + Arrays.toString(dn));
			System.exit(1);
		}
		return retList;
	}
	
	private static void filterOutInvalidTargetState(List<int[]> multisets, boolean[] onlineStatus){
		//System.out.println("considering the following multisets");
		//System.out.println("Total number of multiset=" + multisets.size());
		//for (int[] ms : multisets){
		//	for (int i = 0; i < ms.length; i++)
		//		System.out.print(ms[i]+" ");
		//	System.out.println();
		//}
		Iterator<int[]> iter = multisets.iterator();
		while (iter.hasNext()) {
			int[] ts = iter.next();
			//System.out.println("considering the ts=");
			//System.out.println(Arrays.toString(ts));
			
			/** Remove any target state where every item is same. e.g. (1,1,1) */
			List<Integer> uniqState = getUniqNodeState(ts);
			if (uniqState.size() <= 1) {
				iter.remove();
			}
			/** just sanity check to make sure we do not apply commit to the node that is down
			 *  normalizeMultiset should have handled this correctly. */
			for (int i = 0; i < ts.length; i++) {
				if (ts[i] > 0 && !onlineStatus[i]) {
					iter.remove();
				}
			}
		}
	}
	

	private void fileterOutInvalidDevNodeCombi(List<int[]> devNodeCombi) {
		// TODO Auto-generated method stub
		
	}


	private static List<ResyncPath> filterRepeatedDevNodeResyncPaths(List<ResyncPath> tmpResyncPathList) {
		List<ResyncPath> filteredDevNodeResyncPaths = new ArrayList<ResyncPath>();
		for (ResyncPath rp : tmpResyncPathList) {
			if (!filteredDevNodeResyncPaths.contains(rp)) {
				filteredDevNodeResyncPaths.add(rp);
			}
		}
		return filteredDevNodeResyncPaths;
	}
	
	static int numNodeOnline(boolean[] onlineStatus) {
		int countOnline = 0;
		for (boolean b : onlineStatus) {
			if (b) {
				countOnline++;
			}
		}
		return countOnline;		
	}
	
	private void getEnabledPaths() {
		int curNumAsyncOp = curState.numAsyncOp;
		int[] curNodeState = curState.nodeState.clone();
		boolean[] curOnlineStatus = curState.onlineStatus.clone();
		int[] curSyncSources = curState.syncSources.clone();
		int[] curSyncTargets = curState.syncTargets.clone();
		int numOnline = numNodeOnline(curOnlineStatus);
		boolean canDeviate = ((curNumAsyncOp > 0) ? true : false) 
					&& (numOnline >= (curOnlineStatus.length/2 + 1));
		boolean canResync = curOnlineStatus.length - numOnline > 0;
		if (canDeviate) {
			/** get the target state */ 
			//List<int[]> multisets = MultisetGenerator.generateMultisets(curNumAsyncOp+1, curNodeState.length);
			List<int[]> multisets = MultisetGenerator.generateMultisets(curNumAsyncOp+1, numOnline);
			multisets = normalizeMultisets(multisets, curOnlineStatus);
			filterOutInvalidTargetState(multisets, curOnlineStatus);
			
			/** add deviation path to currently enabled path list for each target state */
			for (int[] ts : multisets) {
				DeviationPath myDevPath = new DeviationPath();
				myDevPath.type = Path.PathType.DEVIATION;
				myDevPath.targetState = ts;
				currentEnabledPaths.add(myDevPath);
			}
		}
		if (canResync) {
			/** get all pairs of deviated nodes */
			List<int[]> devNodeCombi = getAllDevNodeCombi(curNodeState, curOnlineStatus
					, curSyncSources, curSyncTargets);
			
			/** generate and add resync paths to currently enabled path list 
			 *  for each deviated node pair */
			for (int[] dn : devNodeCombi) {
				List<ResyncPath> tmpResyncPathList = getAllDevNodeResyncPaths(dn, curNodeState, 
						curOnlineStatus, curSyncSources, curSyncTargets);
				// filter out repeated ResyncPaths
				List<ResyncPath> resyncPathList = filterRepeatedDevNodeResyncPaths(tmpResyncPathList);
				for (ResyncPath rp : resyncPathList) {
					currentEnabledPaths.add(rp);
				}
			}
		}
	}


	private List<int[]> normalizeMultisets(List<int[]> multisets, boolean[] curOnlineStatus) {
		//System.out.println("[normalizeMultisets 2] Total number of multiset=" + multisets.size()
		//	+ " cardinality of multiset=" + multisets.get(0).length);
		//for (int[] ms : multisets){
		//	for (int i = 0; i < ms.length; i++)
		//		System.out.print(ms[i]+" ");
		//	System.out.println();
		//}
		
		List<int[]> normalizedMultisets = new ArrayList<int[]>();
		for (int[] ms : multisets) {
			int[] normMS = new int[curOnlineStatus.length];
			int i = 0;
			int m = 0;
			for (boolean b : curOnlineStatus) {
				if (b) {
					try {
						normMS[i] = ms[m];
						m++;
					} catch (ArrayIndexOutOfBoundsException ae) {
						System.out.println("i=" + i + " m=" + m + " normMS.length=" + normMS.length + " ms.length=" + ms.length);
						System.out.println("ms=" + Arrays.toString(ms) + " normMS=" + Arrays.toString(normMS));
						System.out.println("curState.numAsyncOp=" + curState.numAsyncOp);
						System.out.println("curState.nodeState=" + Arrays.toString(curState.nodeState));
						System.out.println("curState.onlineStatus=" + Arrays.toString(curState.onlineStatus));
						System.exit(1);
					}
				} else {
					normMS[i] = 0;
				}
				i++;
			}
			normalizedMultisets.add(normMS);
		}
		//System.out.println("[normalizeMultisets 1] Total number of multiset=" + normalizedMultisets.size()
		//	+ " cardinality of multiset=" + multisets.get(0).length);
		//for (int[] ms : normalizedMultisets){
		//	for (int i = 0; i < ms.length; i++)
		//		System.out.print(ms[i]+" ");
		//	System.out.println();
		//}
		return normalizedMultisets;
	}


	@SuppressWarnings("unchecked")
	protected void addNewInitialPath(LinkedList<PathTuple> initialPath, 
            PathTuple oldPathTuple, PathTuple newPathTuple) {
		LinkedList<PathTuple> pathPrefix = (LinkedList<PathTuple>) initialPath.clone();
        pathPrefix.add(new PathTuple(0, oldPathTuple.path));
        finishedInitialPaths.add(pathPrefix);
        LinkedList<PathTuple> newInitialPath = (LinkedList<PathTuple>) initialPath.clone();
        newInitialPath.add(newPathTuple);
        if (!finishedInitialPaths.contains(newInitialPath)) {
        	//log.info("Path " + newPathTuple.path + " is dependent with " + oldPathTuple.path + " at state " + oldPathTuple.state);
        	//System.out.println("Path " + newPathTuple.path + " is dependent with " + oldPathTuple.path + " at state " + oldPathTuple.state);
            initialPaths.add(newInitialPath);
            finishedInitialPaths.add(newInitialPath);
        }
	}
	
	@SuppressWarnings("unchecked")
	public void findInitialPaths() {
		//System.out.println("%%%%%%%%% Started findInitialpaths %%%%%%%%%%");
    	//AbstractState oldAbstractState = prevAbstractState.removeLast();
    	prevAbstractState.removeLast();
    	LinkedList<PathTuple> tmpPath = (LinkedList<PathTuple>) currentExploringPath.clone();
		Iterator<PathTuple> reverseIter = currentExploringPath.descendingIterator();
		int index = currentExploringPath.size();
		while (reverseIter.hasNext()) {
			index--;
			if (index < initPathLen) {
				break;
			}
            PathTuple tuple = reverseIter.next();
            //System.out.println("EnabledPaths at the state=" + tuple.state);
            Set<Path> enabledPaths = enabledPathTable.get(tuple.state);
            for (Path path : enabledPaths) {
				//log.info(tran.toString());
            	//System.out.println(path.toString());
			}
            tmpPath.pollLast();
            for (Path alterPath : enabledPaths) {
            	if (alterPath.hashCode() != tuple.path.hashCode()) {
            		addNewInitialPath(tmpPath, tuple, new PathTuple(0, alterPath));
            	}
            }
		}
		//saveInitialPathsToFile();

        //log.info("There are " + vconInitialPaths.size() + " initial path of DPOR");
        //System.out.println("There are " + initialPaths.size() + " many initial paths");
        //int i = 1;
        //for (LinkedList<PathTuple> path : initialPaths) {
            //String tmp = "init path no. " + i++ + "\n";
            //for (PathTuple tuple : path) {
            //    tmp += tuple.toString() + "\n";
            //}
            //log.info(tmp);
            //System.out.println(tmp);
        //}
        //System.out.println("%%%%%%%%% End findInitialpaths %%%%%%%%%%");
	}
	
	public Path nextPath(LinkedList<Path> pathList) {
		if (pathList.isEmpty()) {
			return null;
		}
		return pathList.removeFirst();
	}
	
	public void updateGlobalState() {
    	int prime = 23;
    	globalState = prime * globalState + currentEnabledPaths.hashCode();
    }
	
	public AbstractState cloneCurrentState(AbstractState curState) {
		AbstractState retState = new AbstractState(curState.numAsyncOp, 
				curState.nodeState.clone(), curState.onlineStatus.clone());
		return retState;
	}
	
	public void resetTest() {
		globalState = 0;
		enabledPathTable = new Hashtable<Integer, Set<Path>>();
		curState.numAsyncOp = origNumAsyncOp;
		Arrays.fill(curState.nodeState, 0);
		Arrays.fill(curState.onlineStatus, true);
		Arrays.fill(curState.syncSources, -1);
		Arrays.fill(curState.syncTargets, 0);
		currentExploringPath.clear();
		initPathLen = 0;
	}
	
	public void recordEnabledPaths(int globalState, LinkedList<Path> currentEnabledPaths) {
		//System.out.println("%%%%%% Started recordEnabledPaths %%%%");
		//System.out.println("for globalState=" + globalState);
		//for (Path path : currentEnabledPaths) {
		//	System.out.println(path.toString());
		//}
		if (enabledPathTable.containsKey(globalState)) {
            ((Set<Path>) enabledPathTable.get(globalState)).addAll(currentEnabledPaths);
        } else {
            enabledPathTable.put(globalState, new HashSet<Path>(currentEnabledPaths));
        }
		//System.out.println("%%%%%% End recordEnabledPaths %%%%");
		
	}
	
	/******************************************************
	 * File I/O functions for Schedules
	 *  - write a list of schedules or read in
	 ******************************************************/
	
	public PathTuple getRealPathTuple(PathTuple t) {
		PathTuple retPT = null;
		if (t.path instanceof DeviationPath) {
			DeviationPath dp = new DeviationPath(((DeviationPath) t.path).targetState);
			retPT = new PathTuple(t.state, dp);
		} else if (t.path instanceof ResyncPath) {
			ResyncPath rp = new ResyncPath(((ResyncPath) t.path).devNode,
					((ResyncPath) t.path).targetSyncSourcesChange,
					((ResyncPath) t.path).targetSyncTargetsChange);
			retPT = new PathTuple(t.state, rp);
		}
		return retPT;

	}

	protected void saveInitialPathsToFile() {
        try {
        	ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(schedDirName + "initialPaths"));
            LinkedList<LinkedList<PathTuple>> dumbInitialPaths = new LinkedList<LinkedList<PathTuple>>();
            for (LinkedList<PathTuple> initPath : initialPaths) {
                LinkedList<PathTuple> dumbPath = new LinkedList<PathTuple>();
                for (PathTuple realTuple : initPath) {
                    dumbPath.add(realTuple.getSerializable());
                }
                dumbInitialPaths.add(dumbPath);
            }
            oos.writeObject(dumbInitialPaths);
            oos.close();
            HashSet<LinkedList<PathTuple>> dumbFinishedInitialPaths = new HashSet<LinkedList<PathTuple>>();
            for (LinkedList<PathTuple> finishedPath : finishedInitialPaths) {
                LinkedList<PathTuple> dumbPath = new LinkedList<PathTuple>();
                for (PathTuple realTuple : finishedPath) {
                    dumbPath.add(realTuple.getSerializable());
                }
                dumbFinishedInitialPaths.add(dumbPath);
            }
            oos = new ObjectOutputStream(new FileOutputStream(schedDirName + "finishedInitialPaths"));
            oos.writeObject(dumbFinishedInitialPaths);
            oos.close();
        } catch (FileNotFoundException e) {
        	System.err.println("saveInitialpathsToFile got FileNotFoundException=" + e.getMessage());
        } catch (IOException e) {
        	System.err.println("saveInitialpathsToFile got IOException=" + e.getMessage());
        }
    }
	
	@SuppressWarnings("unchecked")
	protected void loadInitialPathsFromFile() {
		try {
            File initialPathFile = new File(schedDirName + "initialPaths");
            if (initialPathFile.exists()) {
                ObjectInputStream ois = new ObjectInputStream(new FileInputStream(initialPathFile));
                LinkedList<LinkedList<PathTuple>> dumbInitialPaths = (LinkedList<LinkedList<PathTuple>>) ois.readObject();
                for (LinkedList<PathTuple> dumbInitPath : dumbInitialPaths) {
                    LinkedList<PathTuple> initPath = new LinkedList<PathTuple>();
                    for (PathTuple dumbTuple : dumbInitPath) {
                        initPath.add(getRealPathTuple(dumbTuple));
                    }
                    initialPaths.add(initPath);
                }
                ois.close();
                currentInitPath = initialPaths.poll();
            }
            File finishedInitialPathFile = new File(schedDirName + "finishedInitialPaths");
            if (finishedInitialPathFile.exists()) {
                ObjectInputStream ois = new ObjectInputStream(new FileInputStream(finishedInitialPathFile));
                HashSet<LinkedList<PathTuple>> dumbFinishedDporInitialPaths = (HashSet<LinkedList<PathTuple>>) ois.readObject();
                for (LinkedList<PathTuple> dumbFinishedPath : dumbFinishedDporInitialPaths) {
                    LinkedList<PathTuple> finishedPath = new LinkedList<PathTuple>();
                    for (PathTuple dumbTuple : dumbFinishedPath) {
                        finishedPath.add(getRealPathTuple(dumbTuple));
                    }
                    finishedInitialPaths.add(finishedPath);
                }
                ois.close();
            } else {
                finishedInitialPaths = new HashSet<LinkedList<PathTuple>>();
            }
        } catch (FileNotFoundException e1) {
            System.err.println("loadInitialpathsFromFile got FileNotFoundException=" + e1.getMessage());
        } catch (IOException e1) {
        	System.err.println("loadInitialpathsFromFile got IOException=" + e1.getMessage());
        } catch (ClassNotFoundException e) {
        	System.err.println("loadInitialpathsFromFile got ClassNotFoundException=" + e.getMessage());
        }
	}

	protected void writeSchedList(List<StrataSchedule> schedList) {
		try {
			File scheduleFile = new File(schedFileName);
            FileOutputStream fileOut = new FileOutputStream(scheduleFile);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            System.out.println("writing a list of schedule object to file=" + schedFileName);
            //System.out.println("writing a list of schedule object...");
            for (StrataSchedule sched : schedList) {
            	out.writeObject(sched);
            }
            out.close();
            fileOut.close();
        } catch (IOException e) {
            System.err.println("Problem writing to the file=" + schedFileName);
        }
	}
	
	protected void readSchedList() {
		List<StrataSchedule> schedules = null;
		StrataSchedule inSched = null;
		try {
			File scheduleFile = new File(schedFileName);
			FileInputStream fileIn = new FileInputStream(scheduleFile);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			System.out.println("Schedules:");
			//int i = 0;
			while ((inSched = (StrataSchedule) in.readObject()) != null) {
				//System.out.println(i++ + "-th Schedule:\n");
				//System.out.println("" + inSched.toString());
				if (schedules == null) {
					schedules = new ArrayList<StrataSchedule>();
				}
				schedules.add(inSched);
			}
			in.close();
		} catch (IOException i) {
			//i.printStackTrace();
			System.out.println("IOException while reading a file=" + schedFileName);
			System.out.println("IOException occurred while reading objects. Treat it as EOF!");
			System.out.println("This is not critical. We are using IOException for EOF");
			return;
		} catch (ClassNotFoundException c) {
			System.out.println("Employee class not found");
			c.printStackTrace();
			return;
		}
	}

	/**********************************************************************
	 * Schedule Generation Core Functions:
	 *  - We do recursively find all possible deviation and resync scenarios
	 *  
	 * Algorithm is:
	 *  - Replay if there is init path given
	 *  - Given numAsyncOp, nodeState, onlineStatus as a state, look for
	 *    enabled paths
	 *  - Pick one, remember the previous state along with the chosen path
	 *    for the corresponding currentExploringPath before applying the path
	 *  - Apply the path and update the state accordingly
	 *  - Stop exploring when there is no numAsyncOp and no enabled path left
	 *  - Based on currentExploringPath, generate paths to explore in next runs 
	 *  
	 **********************************************************************/
	
	@SuppressWarnings("resource")
	public void replayInitPath() {
		//System.out.println("%%%%%%% Started replayInitPath %%%%%%%");
		if (currentInitPath != null) {
			/**  mode for manually picking next init path to start with */
			if (manualPickInitPath) {
        		initialPaths.addFirst(currentInitPath);
        		Scanner reader = new Scanner(System.in);  // Reading from System.in
        		int n;
        		while(true){
        			System.out.println("Enter a number (0 - display ; 1 - " + initialPaths.size() + "): ");
            		n = reader.nextInt(); // Scans the next token of the input as an int.
            		if (n > initialPaths.size() || n < 0) {
            			System.err.println("You should input number 0 to " + initialPaths.size());
            		} else if (n == 0) {
            			int i = 1;
            	        for (LinkedList<PathTuple> path : initialPaths) {
            	            String tmp = "<Selectable> init path no. " + i++ + "\n";
            	            for (PathTuple tuple : path) {
            	                tmp += tuple.toString() + "\n";
            	            }
            	            //log.info(tmp);
            	            System.out.println(tmp);
            	        }
            		} else {
            			break;
            		}
        		}
        		//log.info("Selected " + n + "-th init path to start with");
        		System.out.println("Selected " + n + "-th init path to start with");
        		currentInitPath = initialPaths.remove(n-1);
        	}
			//System.out.println("There is an existing initial path, start with this path first");
			initPathLen = currentInitPath.size();
			//String tmp = "Initial path\n";
			//for (PathTuple tuple : currentInitPath) {
            //	tmp += tuple.toString() + "\n";
            //}
            //log.info(tmp);
			//System.out.println(tmp);
			for (PathTuple tuple : currentInitPath) {
				/** Look for enabled paths */
	        	getEnabledPaths();
	        	
	        	/** record currently enabled paths to the table */
	        	recordEnabledPaths(globalState, currentEnabledPaths);
				
				//System.out.println("applying path=" + tuple.path.toString());
	            
	            /** we rememeber everything we are going to need */
	            currentExploringPath.add(new PathTuple(globalState, tuple.path));
	            AbstractState copyCurState = cloneCurrentState(curState);
	            prevAbstractState.add(copyCurState);
	            
	            //System.out.println("before applying path, state=" + curState.toString());
	            if (tuple.path.apply(curState)) {
	            	//System.out.println("after applying path, state=" + curState.toString());
	                updateGlobalState();
	                currentEnabledPaths.clear();
	            }
			}
		} else {
			System.out.println("nothing to replay. currentInitPath is null");
		}
		//System.out.println("%%%%%%% End replayInitPath %%%%%%%");
	}
	
	public void generateSchedule() {
		//System.out.println("%%%%%%% Started generateSchedule %%%%%%%");
		int numWaitTime = 0;
        while (true) {
        	/** Look for enabled paths */
        	getEnabledPaths();
        	//System.out.println("after getEnabledPaths, currentEnabledPaths contains:");
        	//for (Path p : currentEnabledPaths) {
        	//	System.out.println(p.toString());
        	//}
        	
        	/** record currently enabled paths to the table */
        	recordEnabledPaths(globalState, currentEnabledPaths);
        	
        	/** We finish exploration when there is no more async op and enabled path */
        	if (curState.numAsyncOp == 0 && currentEnabledPaths.isEmpty() || numWaitTime > 10) {
        		StrataSchedule sched = new StrataSchedule();
        		String mainPath = "";
                for (PathTuple tuple : currentExploringPath) {
                    mainPath += tuple.toString() + "\n";
                    sched.getSchedule().add(tuple.path);
                }
                // BEGIN sanity check: see if we are generating any duplicated schedule
                int hashOfSched = 7;
                String schedStr = sched.toString();
                for (int i = 0; i < schedStr.length(); i++) {
                	hashOfSched = hashOfSched*31 + schedStr.charAt(i);
                }
                if (schedHashSet == null) {
                	schedHashSet = new HashSet<Integer>();
                }
                if (schedHashSet.contains(hashOfSched)) {
                	// if we got the duplicated schedule, skip and do not include in the schedule file
                	//System.err.println("we have already generated that schedule:" + schedStr);
                	dupSchedCnt++;
                } else {
                	schedHashSet.add(hashOfSched);
                	scheduleList.add(sched);
                	uniqSchedCnt++;
                }
                // END sanity check
                if (scheduleList.size() > scheduleCountLimitsPerFile) {
                	//if (scheduleFileCount == 60) { 
                	//	System.out.println("scheduleFileCout=60");
                	//	System.out.println("scheduleList.size=" + scheduleList.size());
                	//	System.out.println(scheduleList.get(478));
                	//} else if (scheduleFileCount == 31) { 
                	//	System.out.println("scheduleFileCout=31");
                	//	System.out.println("scheduleList.size=" + scheduleList.size());
                	//	System.out.println(scheduleList.get(478)); 
                	//}
                	writeSchedList(scheduleList);
                	scheduleList.clear();
                	scheduleFileCount++;
                	schedFileName = schedFileNamePrefix + "-" + origNumAsyncOp + "AOP" + 
                			"_" + origNumNode + "Nodes" + scheduleFileCount;
                }
                System.out.println("Main path\n" + mainPath);
                findInitialPaths();
                if (initialPaths.size() == 0) {
                	if (!scheduleList.isEmpty()) {
                		writeSchedList(scheduleList);
                	}
                	System.out.println("Finished exploring all states");
                }
            	break;
        	} else if (currentEnabledPaths.isEmpty()) {
        		try {
                    numWaitTime++;
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                }
                continue;
        	}
        	
        	//System.out.println("numAsyncOp=" + curState.numAsyncOp + " currentEnabledPaths size=" 
        	//		+ currentEnabledPaths.size() + " numWaitTime=" + numWaitTime);
        	
        	/** we pick one path */
        	numWaitTime = 0;
            Path path = nextPath(currentEnabledPaths);
            if (path != null) {
            	//System.out.println("applying path=" + path.toString());
	            
	            /** we rememeber everything we are going to need */
	            currentExploringPath.add(new PathTuple(globalState, path));
	            AbstractState copyCurState = cloneCurrentState(curState);
	            prevAbstractState.add(copyCurState);
	            
	            //System.out.println("before applying path, state=" + curState.toString() + "globalState=" + globalState);
	            if (path.apply(curState)) {
	            	updateGlobalState();
	            	//System.out.println("after applying path, state=" + curState.toString() + "globalState=" + globalState);
	                currentEnabledPaths.clear();
	            }
            }
        }
        //System.out.println("%%%%%%% End generateSchedule %%%%%%%");
	}
	

	public static void main(String[] args) {
		
		// unit test 1: getHighestNodeState
		System.out.println("===================================================");
		System.out.println("unit test 1: getHighestNodeState");
		System.out.println("---------------------------------------------------");
		int[] tmpNS = new int[] {3,6,0};
		int result = getHighestNodeState(tmpNS);
		System.out.println("result is=" + result);
		
		// unit test 2: getUniqNodeState
		System.out.println("===================================================");
		System.out.println("unit test 2: getUniqNodeState");
		System.out.println("---------------------------------------------------");
		List<Integer> resultIntList = getUniqNodeState(tmpNS);
		System.out.println("1. resultIntList has:");
		for (int i : resultIntList) {
			System.out.println("" + i);
		}
		int[] tmpNS2 = new int[] {3,3,6};
		resultIntList = getUniqNodeState(tmpNS2);
		System.out.println("2. resultIntList has:");
		for (int i : resultIntList) {
			System.out.println("" + i);
		}
		
		// unit test 3: getAllDevNodePairs
		System.out.println("===================================================");
		System.out.println("unit test 3: getAllDevNodePairs");
		System.out.println("---------------------------------------------------");
		boolean[] tmpOnlineStatus = new boolean[] {true, true, true};
		List<int[]> resultIntArrList = getAllDevNodePairs(tmpNS, tmpOnlineStatus);
		System.out.println("1. resultIntArrList has:");
		for (int[] iArr : resultIntArrList) {
			System.out.print("" + Arrays.toString(iArr));
		}
		System.out.println("");
		tmpOnlineStatus = new boolean[] {false, true, true};
		List<int[]> resultIntArrList2 =	getAllDevNodePairs(tmpNS2, tmpOnlineStatus);
		System.out.println("2. resultIntArrList2 has:");
		for (int[] iArr : resultIntArrList2) {
			System.out.print("" + Arrays.toString(iArr));
		}
		System.out.println("");
		
		// unit test 4: filterOutInvalidTargetState
		System.out.println("===================================================");
		System.out.println("unit test 4: filterOutInvalidTargetState");
		System.out.println("---------------------------------------------------");
		List<int[]> tmpTargetStateList = new ArrayList<int[]>();
		int[] tmpTS1 = new int[]{3,4,5};
		int[] tmpTS2 = new int[]{3,3,3};
		int[] tmpTS3 = new int[]{1,4,0};
		int[] tmpTS4 = new int[]{1,6,1};
		int[] tmpTS5 = new int[]{0,0,0};
		tmpTargetStateList.add(tmpTS1);
		tmpTargetStateList.add(tmpTS2);
		tmpTargetStateList.add(tmpTS3);
		tmpTargetStateList.add(tmpTS4);
		tmpTargetStateList.add(tmpTS5);
		tmpOnlineStatus = new boolean[] {true, true, true};
		filterOutInvalidTargetState(tmpTargetStateList, tmpOnlineStatus);
		System.out.println("1. after filter out invalid target state, we have:");
		for (int[] iArr : tmpTargetStateList) {
			System.out.println("" + Arrays.toString(iArr));
		}
		tmpOnlineStatus = new boolean[] {true, true, false};
		System.out.println("2. after filter out invalid target state, we have:");
		filterOutInvalidTargetState(tmpTargetStateList, tmpOnlineStatus);
		for (int[] iArr : tmpTargetStateList) {
			System.out.println("" + Arrays.toString(iArr));
		}
		
		// unit test 5: schedListToString
		System.out.println("===================================================");
		System.out.println("unit test 5: schedListToString");
		System.out.println("---------------------------------------------------");
		List<StrataSchedule> tmpSchedList = new ArrayList<StrataSchedule>();
		StrataSchedule sched1 = new StrataSchedule();
		StrataSchedule sched2 = new StrataSchedule();
		StrataSchedule sched3 = new StrataSchedule();
		DeviationPath devPath1 = new DeviationPath();
		devPath1.type = Path.PathType.DEVIATION;
		devPath1.targetState = tmpTS1;
		DeviationPath devPath2 = new DeviationPath();
		devPath2.type = Path.PathType.DEVIATION;
		devPath2.targetState = tmpTS2;
		DeviationPath devPath3 = new DeviationPath();
		devPath3.type = Path.PathType.DEVIATION;
		devPath3.targetState = tmpTS3;
		ResyncPath resyncPath1 = new ResyncPath();
		resyncPath1.type = Path.PathType.RESYNC;
		resyncPath1.devNode = tmpNS;
		ResyncPath resyncPath2 = new ResyncPath();
		resyncPath2.type = Path.PathType.RESYNC;
		resyncPath2.devNode = tmpNS2;
		System.out.println("5.1 testing empty schedule list");
		System.out.println(schedListToString(tmpSchedList));
		sched1.getSchedule().add(devPath1);
		sched1.getSchedule().add(devPath2);
		sched1.getSchedule().add(resyncPath1);
		tmpSchedList.add(sched1);
		System.out.println("5.2 testing schedule list containing schedule 1");
		System.out.println(schedListToString(tmpSchedList));
		sched2.getSchedule().add(devPath3);
		sched2.getSchedule().add(devPath2);
		sched2.getSchedule().add(resyncPath2);
		tmpSchedList.add(sched2);
		tmpSchedList.add(sched3);
		System.out.println("5.3 testing schedule list containing schedule 1, 2 and empty schedule 3");
		System.out.println(schedListToString(tmpSchedList));
		
		// unit test 6: writeSchedule and readSchedules
		System.out.println("===================================================");
		System.out.println("unit test 6: writeSchedule and readSchedules");
		System.out.println("---------------------------------------------------");
		String scheduleDirectoryPath = "/home/ben/project/drmbt/testScheduleDir/"; 
		ScheduleGenerator sgen = new ScheduleGenerator(1, 3, scheduleDirectoryPath);
		System.out.println("6.1. testing reading the default schedule file");
		sgen.readSchedList();
		System.out.println("6.2. testing writing schedule list and read in");
		//System.out.println("writing the following schedule:\n" + sched1.toString());
		sgen.writeSchedList(tmpSchedList);
		sgen.readSchedList();
		
		// unit test 7: test schedule generation for just 1 schedule
		System.out.println("===================================================");
		System.out.println("unit test 7: test schedule generation generating just 1 schedule");
		System.out.println("---------------------------------------------------");
		System.out.println("7.1. testing 1 async op with 3 nodes");
		sgen = new ScheduleGenerator(1, 3, scheduleDirectoryPath);
		sgen.generateSchedule();
		System.out.println("7.2. testing 2 async op with 3 nodes");
		sgen = new ScheduleGenerator(2,3, scheduleDirectoryPath);
		sgen.generateSchedule();
		System.out.println("7.3. testing 6 async op with 3 nodes");
		sgen = new ScheduleGenerator(6,3, scheduleDirectoryPath);
		sgen.generateSchedule();
		System.out.println("7.4. testing 6 async op with 5 nodes");
		sgen = new ScheduleGenerator(6,5, scheduleDirectoryPath);
		sgen.generateSchedule();
		
		// unit test 8: test schedule generation for more than 1 schedules
		System.out.println("===================================================");
		System.out.println("unit test 8: test schedule generation generating just 1 schedule");
		System.out.println("---------------------------------------------------");
		System.out.println("8.1. testing 1 async op with 3 nodes");
		sgen = new ScheduleGenerator(1, 3, scheduleDirectoryPath);
		sgen.generateSchedule();
		System.out.println("1 Schedule Generated\n");
		//sgen.findInitialPaths();
		
		// unit test 9: test schedule generation until nothing more to generate
		System.out.println("===================================================");
		System.out.println("unit test 9: test schedule generation until nothing more to generate");
		System.out.println("---------------------------------------------------");
		
		int i = 1;
		while (!sgen.initialPaths.isEmpty()) {
			sgen.currentInitPath = sgen.initialPaths.removeFirst();
			sgen.resetTest();
			sgen.replayInitPath();
			sgen.generateSchedule();
			System.out.println((i++) + " Schedule Generated\n");
			//sgen.findInitialPaths();
		}
		
		// unit test 10: test if we can get all expected set of nodes to resync for the given states 
		System.out.println("===================================================");
		System.out.println("unit test 10: test devNodeCombi generation, filtering symmetric pairs and getAllDevNodeResyncPaths works");
		System.out.println("---------------------------------------------------");
		System.out.println("10.1.a. testing after divergence and no resync has been done");
		System.out.println("See if I have [0,1]");
		int[] tmpNodeState = new int[] {0, 0, 0, 1};
		tmpOnlineStatus = new boolean[] {false, false, false, false};
		int[] tmpSyncSources = new int[] {-1, -1, -1, -1};
		int[] tmpSyncTargets = new int[] {0, 0, 0, 0};
		List<int[]> tmpDevNodeCombi = getAllDevNodeCombiHelper(tmpNodeState, tmpOnlineStatus, tmpSyncSources, tmpSyncTargets);
		boolean foundExpected = false;
		for (int[] dnc : tmpDevNodeCombi) {
			System.out.println("DevNodeCombi's=" + Arrays.toString(dnc));
			if (Arrays.equals(dnc, new int[] {0,1})) {
				foundExpected = true;
			}
		}
		if (foundExpected) {
			System.out.println("We have [0,1]");
		} else {
			System.out.println("We DON'T have [0,1]");
		}
		System.out.println("10.1.b. after filtering..");
		filterOutSymmetricPairs(tmpDevNodeCombi, tmpNodeState, tmpSyncSources, tmpSyncTargets);
		System.out.println("AFTER Filtering:");
		foundExpected = false;
		for (int[] dnc : tmpDevNodeCombi) {
			System.out.println("DevNodeCombi's=" + Arrays.toString(dnc));
			if (Arrays.equals(dnc, new int[] {0,1})) {
				foundExpected = true;
			}
		}
		if (foundExpected) {
			System.out.println("We have [0,1]");
		} else {
			System.out.println("We DON'T have [0,1]");
		}
		System.out.println("10.1.c. Testing getAllDevNodeResycnPaths");
		System.out.println("For [0,1]:");
		List<ResyncPath> tmpResyncPaths = getAllDevNodeResyncPaths(new int[] {0,1}, tmpNodeState, tmpOnlineStatus, tmpSyncSources, tmpSyncTargets);
		for (ResyncPath rp : tmpResyncPaths) {
			System.out.println("ResyncPath's=" + rp.toString());
		}
			
		System.out.println("10.2.a. testing after divergence and resync was done once");
		System.out.println("See if I have [2]");
		tmpNodeState = new int[] {1000, 1000, 0, 1};
		tmpOnlineStatus = new boolean[] {true, true, false, false};
		tmpSyncSources = new int[] {-1, 0, -1, -1};
		tmpSyncTargets = new int[] {0b0010, 0, 0, 0};
		tmpDevNodeCombi = getAllDevNodeCombiHelper(tmpNodeState, tmpOnlineStatus, tmpSyncSources, tmpSyncTargets);
		foundExpected = false;
		for (int[] dnc : tmpDevNodeCombi) {
			System.out.println("DevNodeCombi's=" + Arrays.toString(dnc));
			if (Arrays.equals(dnc, new int[] {2})) {
				foundExpected = true;
			}
		}
		if (foundExpected) {
			System.out.println("We have [2]");
		} else {
			System.out.println("We DON'T have [2]");
		}
		System.out.println("10.2.b. after filtering..");
		filterOutSymmetricPairs(tmpDevNodeCombi, tmpNodeState, tmpSyncSources, tmpSyncTargets);
		System.out.println("AFTER Filtering:");
		foundExpected = false;
		for (int[] dnc : tmpDevNodeCombi) {
			System.out.println("DevNodeCombi's=" + Arrays.toString(dnc));
			if (Arrays.equals(dnc, new int[] {2})) {
				foundExpected = true;
			}
		}
		if (foundExpected) {
			System.out.println("We have [2]");
		} else {
			System.out.println("We DON'T have [2]");
		}
		System.out.println("10.2.c. Testing getAllDevNodeResycnPaths");
		System.out.println("For [2]:");
		tmpResyncPaths = getAllDevNodeResyncPaths(new int[] {2}, tmpNodeState, tmpOnlineStatus, tmpSyncSources, tmpSyncTargets);
		for (ResyncPath rp : tmpResyncPaths) {
			System.out.println("ResyncPath's=" + rp.toString());
		}
		
		System.out.println("10.3.a. testing after divergence was done twice and resync was done twice");
		System.out.println("See if I have [0,1]");
		tmpNodeState = new int[] {2001, 2001, 2001, 1};
		tmpOnlineStatus = new boolean[] {false, false, false, false};
		tmpSyncSources = new int[] {-1, 0, 1, -1};
		tmpSyncTargets = new int[] {0b0010, 0b0100, 0, 0};
		tmpDevNodeCombi = getAllDevNodeCombiHelper(tmpNodeState, tmpOnlineStatus, tmpSyncSources, tmpSyncTargets);
		foundExpected = false;
		for (int[] dnc : tmpDevNodeCombi) {
			System.out.println("DevNodeCombi's=" + Arrays.toString(dnc));
			if (Arrays.equals(dnc, new int[] {0,1})) {
				foundExpected = true;
			}
		}
		if (foundExpected) {
			System.out.println("We have [0,1]");
		} else {
			System.out.println("We DON'T have [0,1]");
		}
		System.out.println("10.3.b. after filtering..");
		filterOutSymmetricPairs(tmpDevNodeCombi, tmpNodeState, tmpSyncSources, tmpSyncTargets);
		System.out.println("AFTER Filtering:");
		foundExpected = false;
		for (int[] dnc : tmpDevNodeCombi) {
			System.out.println("DevNodeCombi's=" + Arrays.toString(dnc));
			if (Arrays.equals(dnc, new int[] {0,1})) {
				foundExpected = true;
			}
		}
		if (foundExpected) {
			System.out.println("We have [0,1]");
		} else {
			System.out.println("We DON'T have [0,1]");
		}
		System.out.println("10.3.c. Testing getAllDevNodeResycnPaths");
		System.out.println("For [0,1]:");
		tmpResyncPaths = getAllDevNodeResyncPaths(new int[] {0,1}, tmpNodeState, tmpOnlineStatus, tmpSyncSources, tmpSyncTargets);
		for (ResyncPath rp : tmpResyncPaths) {
			System.out.println("ResyncPath's=" + rp.toString());
		}
		
		System.out.println("10.4.a. testing after divergence was done twice and resync was done three times");
		System.out.println("See if I have [3]");
		tmpNodeState = new int[] {3000, 3000, 2001, 1};
		tmpOnlineStatus = new boolean[] {true, true, false, false};
		tmpSyncSources = new int[] {-1, 0, 1, -1};
		tmpSyncTargets = new int[] {0b0010, 0b0100, 0, 0};
		tmpDevNodeCombi = getAllDevNodeCombiHelper(tmpNodeState, tmpOnlineStatus, tmpSyncSources, tmpSyncTargets);
		foundExpected = false;
		for (int[] dnc : tmpDevNodeCombi) {
			System.out.println("DevNodeCombi's=" + Arrays.toString(dnc));
			if (Arrays.equals(dnc, new int[] {3})) {
				foundExpected = true;
			}
		}
		if (foundExpected) {
			System.out.println("We have [3]");
		} else {
			System.out.println("We DON'T have [3]");
		}
		System.out.println("10.4.b. after filtering..");
		filterOutSymmetricPairs(tmpDevNodeCombi, tmpNodeState, tmpSyncSources, tmpSyncTargets);
		System.out.println("AFTER Filtering:");
		foundExpected = false;
		for (int[] dnc : tmpDevNodeCombi) {
			System.out.println("DevNodeCombi's=" + Arrays.toString(dnc));
			if (Arrays.equals(dnc, new int[] {3})) {
				foundExpected = true;
			}
		}
		if (foundExpected) {
			System.out.println("We have [3]");
		} else {
			System.out.println("We DON'T have [3]");
		}
		System.out.println("10.4.c. Testing getAllDevNodeResycnPaths");
		System.out.println("For [3]:");
		tmpResyncPaths = getAllDevNodeResyncPaths(new int[] {3}, tmpNodeState, tmpOnlineStatus, tmpSyncSources, tmpSyncTargets);
		for (ResyncPath rp : tmpResyncPaths) {
			System.out.println("ResyncPath's=" + rp.toString());
		}
		
		System.out.println("10.5.a. testing after divergence was done twice and resync was done four times");
		System.out.println("See if I have [2]");
		tmpNodeState = new int[] {4000, 4000, 2001, 4000};
		tmpOnlineStatus = new boolean[] {true, true, false, true};
		tmpSyncSources = new int[] {3, 0, 1, -1};
		tmpSyncTargets = new int[] {0b0010, 0b0100, 0, 0b0001};
		tmpDevNodeCombi = getAllDevNodeCombiHelper(tmpNodeState, tmpOnlineStatus, tmpSyncSources, tmpSyncTargets);
		foundExpected = false;
		for (int[] dnc : tmpDevNodeCombi) {
			System.out.println("DevNodeCombi's=" + Arrays.toString(dnc));
			if (Arrays.equals(dnc, new int[] {2})) {
				foundExpected = true;
			}
		}
		if (foundExpected) {
			System.out.println("We have [2]");
		} else {
			System.out.println("We DON'T have [2]");
		}
		System.out.println("10.5.b. after filtering..");
		filterOutSymmetricPairs(tmpDevNodeCombi, tmpNodeState, tmpSyncSources, tmpSyncTargets);
		System.out.println("AFTER Filtering:");
		foundExpected = false;
		for (int[] dnc : tmpDevNodeCombi) {
			System.out.println("DevNodeCombi's=" + Arrays.toString(dnc));
			if (Arrays.equals(dnc, new int[] {2})) {
				foundExpected = true;
			}
		}
		if (foundExpected) {
			System.out.println("We have [2]");
		} else {
			System.out.println("We DON'T have [2]");
		}
		System.out.println("10.5.c. Testing getAllDevNodeResycnPaths");
		System.out.println("For [2]:");
		tmpResyncPaths = getAllDevNodeResyncPaths(new int[] {2}, tmpNodeState, tmpOnlineStatus, tmpSyncSources, tmpSyncTargets);
		for (ResyncPath rp : tmpResyncPaths) {
			System.out.println("ResyncPath's=" + rp.toString());
		}
		System.out.println("10.5.d. Testing filterRepeatedDevNodeResyncPaths");
		List<ResyncPath> filteredResyncPaths = filterRepeatedDevNodeResyncPaths(tmpResyncPaths);
		for (ResyncPath rp : filteredResyncPaths) {
			System.out.println("filtered ResyncPath's=" + rp.toString());
		}

		// unit test 11: test if hasCycle works 
		System.out.println("===================================================");
		System.out.println("unit test 11: test hasCycle");
		System.out.println("---------------------------------------------------");
		tmpSyncSources = new int[] {-1,0,0,-1};
		int[] restartedNode = new int[] {3};
		System.out.println("For the given syncSources=" + Arrays.toString(tmpSyncSources) 
			+ " and for the restarted node=" + Arrays.toString(restartedNode) 
			+ ", the result of hasCycle=" + hasCycle(tmpSyncSources, restartedNode[0]));
		

		// unit test 12: test if getAllDevNodeResyncPaths works 
		System.out.println("===================================================");
		System.out.println("unit test 11: test getAllDevNodeResyncPaths");
		System.out.println("---------------------------------------------------");
		tmpNodeState = new int[] {3001, 3001, 3001, 1};
		tmpOnlineStatus = new boolean[] {true, true, true, false};
		tmpSyncSources = new int[] {-1, 0, 0, -1};
		tmpSyncTargets = new int[] {0b0110, 0, 0, 0};
		tmpResyncPaths = getAllDevNodeResyncPaths(new int[] {3}, tmpNodeState,
				tmpOnlineStatus, tmpSyncSources, tmpSyncTargets);
		for (ResyncPath rp : tmpResyncPaths) {
			System.out.println("ResyncPath's=" + rp.toString());
		}
		
		/*System.out.println("8.2. testing 1 async op with 3 nodes for three schedules");
		sgen = new ScheduleGenerator(2,3);
		sgen.generateSchedule();
		sgen.findInitialPaths();
		sgen.currentInitPath = sgen.initialPaths.removeFirst();
		sgen.resetTest();
		sgen.replayInitPath();
		sgen.generateSchedule();
		sgen.findInitialPaths();
		sgen.currentInitPath = sgen.initialPaths.removeFirst();
		sgen.resetTest();
		sgen.replayInitPath();
		sgen.generateSchedule();
		System.out.println("8.3. testing 6 async op with 3 nodes for two schedules");
		sgen = new ScheduleGenerator(6,3);
		sgen.generateSchedule();
		sgen.findInitialPaths();
		sgen.currentInitPath = sgen.initialPaths.removeFirst();
		sgen.resetTest();
		sgen.replayInitPath();
		sgen.generateSchedule();*/
		
	}
	
}
