package strata.conex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Scanner;

import strata.common.DeviationPath;
import strata.common.Path;
import strata.common.ResyncPath;
import strata.common.StrataSchedule;
import strata.conex.mongo.MongoSystemController;
import strata.conex.redis.RedisSystemController;
import strata.conex.zookeeper.ZooKeeperSystemController;
import strata.utils.ScheduleExecutionStatMeasurer;

public class TestingEngine {
	
	/*
	 * Variables for Initial loading of schedule files
	 */
	static String workloadDirName; // = "/home/kroud2/bhkim/strata/scheduleGen/scheduleFile-5-3"; //"/home/ben/project/vcon/scheduleGen/scheduleFile"; // HARD-CODED
	public static String workloadDirNamePrefix;
	LinkedList<File> scheduleFiles;
	protected LinkedList<StrataSchedule> currentSchedules;
	
	/*
	 * Variables to Prepare testing environment
	 */
	//public static String strataSrcDir; // = "/home/kroud2/eclipse-workspace/Strata"; //"/home/ben/workspace-neon2/VConTestGeneration"; // HARD-CODED
	public static String workingDir; // = "/home/kroud2/bhkim/strata/test-5-3-zk-3.4.11-strata-0.1"; //"/home/ben/project/vcon/zktests"; // HARD-CODED
	public static String workingDirPrefix;
	int testNum;
	protected enum SystemUnderTestType {ZooKeeper, Cassandra, Couchbase, MongoDB, HBase, Redis};
	protected static SystemUnderTestType sysType; //= SystemUnderTestType.ZooKeeper; // HARD-CODED
	public static String sutVersion;
	public static String strataVersion;
	protected static SystemController controller;
	public static String sysCntrConfigName;
	
	/*
	 * Variables for Executing a Schedule
	 */
	public static int numAOP = 5; // HARD-CODED
	public static int numNode = 3; // HARD-CODED
	public int numSchedFiles = 10000; // HARD-CODED
	protected String[] keys = new String[numAOP];
	public final static String probingKey = "/testDivergenceResync-Probing-Key";  // HARD-CODED
	int base;
	int vCnt;
	public static boolean debugMode = false;
	public static boolean interruptMode = false;
	public static boolean programMode = false;
	Scanner reader = null;
	
	/*
	 * Variables for Saving results
	 */
	protected int testId;
	protected static String strataTestRecordDirPath; // = workingDir + "/record"; //"/home/ben/project/vcon/zktests/record"; // HARD-CODED
	protected String strataIdRecordDirPath;
	protected final static String STRATA_ABSTRACT_SCHEDULE_FILE = "abstractSchedule"; //HARD-CODED
	protected final static String STRATA_RESULT_FILE = "result";   // HARD-CODED
	protected final static String STRATA_SCHED_EXEC_FILE = "schedExec";  // HARD-CODED
	protected FileOutputStream strataAbstractScheduleFile;
	protected String strataAbstractScheduleFilePath;
	protected FileOutputStream strataSchedExecFile;
	protected String strataSchedExecFilePath;
	protected FileOutputStream strataResultFile;
	protected String strataResultFilePath;

	/*
	 *  Variables for debugging
	 *  
	 */
	public static int pathCounter;
	
	public TestingEngine(String configFile) {
		
		parseConfigInit(configFile);
		
		/*
		 * Initialize unfinished necessary variables
		 * 
		 */
		workloadDirName = workloadDirNamePrefix + "-" + numAOP + "-" + numNode;
		workingDir = workingDirPrefix + "/test-" + numAOP + "-" + numNode + "-" 
				+ sysType.toString() + "-" + sutVersion + "-" 
				+ "strata-" + strataVersion;
		strataTestRecordDirPath = workingDir + "/record";
		System.out.println("workloadDirName=" + workloadDirName);
		System.out.println("workingDir=" + workingDir);
		System.out.println("strataTestRecordDirPath=" + strataTestRecordDirPath);
		
		/*
		 * Initial loading of schedule files
		 */
		scheduleFiles = new LinkedList<File>();
		File folder = new File(workloadDirName);
		int schedFileCnt = 0;
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
			System.out.println("fileEntry=" + fileEntry.getName());
			scheduleFiles.add(fileEntry);
			schedFileCnt++;
			if (schedFileCnt >= numSchedFiles) {
				break;
			}
	    }
		
		//try {
		//	Thread.sleep(100000);
		//} catch (InterruptedException e) {
		//	// TODO Auto-generated catch block
		//	e.printStackTrace();
		//}
		
		currentSchedules = new LinkedList<StrataSchedule>();
		
		/*
		 * Prepare testing environment
		 */
		System.out.println("Preparing Testing Environment");
		if (sysType.equals(SystemUnderTestType.ZooKeeper)) {
        	controller = new ZooKeeperSystemController(numNode, workingDir);
        	controller.sysCtrlParseConfigInit(sysCntrConfigName);
        } else if (sysType.equals(SystemUnderTestType.MongoDB)) {
        	controller = new MongoSystemController(numNode, workingDir);
        	controller.sysCtrlParseConfigInit(sysCntrConfigName);
        } else if (sysType.equals(SystemUnderTestType.Couchbase)) {
        	//TBD
        	
        } else if (sysType.equals(SystemUnderTestType.Redis)) {
        	controller = new RedisSystemController(numNode, workingDir);
        	controller.sysCtrlParseConfigInit(sysCntrConfigName);
        } else {
        	System.err.println("Currently unsupported system type=" + sysType.toString());
        	System.exit(1);
        }
		File workingDirFile = new File(workingDir);
		if (!workingDirFile.exists()) {
			if (!workingDirFile.mkdir()) {
				System.err.println("Unable to mkdir " + workingDirFile);
	            System.exit(1);
	        }
		}
		File gspathDir = new File(workingDir + "/record");
		if (!gspathDir.exists()) {
			if (!gspathDir.mkdir()) {
				System.err.println("Unable to mkdir " + gspathDir);
	            System.exit(1);
	        }
		}
		
		testNum = gspathDir.list().length + 1;
		
        System.out.println("There are as many as " + (testNum - 1) + " test cases executed previously");
        
	}
	
	public StrataSchedule cloneSchedule(StrataSchedule schedOrig) {
		StrataSchedule schedCopy = null;
		if (schedOrig != null) {
			schedCopy = new StrataSchedule();
			for (Path path : schedOrig.sched) {
				if (path instanceof DeviationPath) {
					DeviationPath dPath = (DeviationPath) path;
					DeviationPath dPathCopy = new DeviationPath(dPath.targetState.clone());
					schedCopy.sched.add(dPathCopy);
				} else if (path instanceof ResyncPath) {
					ResyncPath rPath = (ResyncPath) path;
					ResyncPath rPathCopy = new ResyncPath(rPath.devNode.clone(), 
							rPath.targetSyncSourcesChange.clone(), 
							rPath.targetSyncTargetsChange.clone());
					schedCopy.sched.add(rPathCopy);
				}
			}
		}
		
		return schedCopy;
	}
	
	public void loadSchedulesFromSchedFile(File curSchedFile) {
		StrataSchedule inSched = null;
		try {
			FileInputStream fileIn = new FileInputStream(curSchedFile);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			while ((inSched = (StrataSchedule) in.readObject()) != null) {
				StrataSchedule schedWorkingCopy = cloneSchedule(inSched);
				//currentSchedules.add(inSched);
				currentSchedules.add(schedWorkingCopy);
			}
			in.close();
		} catch (IOException ioe) {
			System.out.println("Loaded all Schedules in the file=" + curSchedFile.getAbsolutePath());
			System.out.println(currentSchedules.size() + " many schedules are loaded");
		} catch (ClassNotFoundException c) {
			c.printStackTrace();
			return;
		}
	}

	private void startTesting() {
		/*
		 * start testing
		 */
		long startTime = System.currentTimeMillis();
		boolean result = false;
		String resultStr = null;
		int numViolations = 0;
		ArrayList<StrataSchedule> buggySchedules = new ArrayList<StrataSchedule>();
		int numTestCases = 0;
		
		// stat collection variable declaration
		ScheduleExecutionStatMeasurer statMgr;
		int schedExecDurStatID;
		long schedExecDur;
		ArrayList<Integer> arrayOfStatIDs;
				
		// start testing
		System.out.println("Start Testing!");
		System.out.println("=============================================================================");
		for (File curSchedFile : scheduleFiles) {
			if (curSchedFile == null) {
				System.err.println("curSchedFile is null");
				System.exit(1);
			}
			currentSchedules.clear();
			loadSchedulesFromSchedFile(curSchedFile);
			System.out.println("Processing schedule file=" + curSchedFile.getName());
			System.out.println("Start executing a batch of schedules");
			for (StrataSchedule sched : currentSchedules) {
				vCnt = 0;
				numTestCases++;
				pathCounter = 0;
				if (numTestCases < testNum) {
					continue;
				}
				System.out.println("next test id=" + numTestCases);
				System.out.println("schedule to execute=" + sched.toString());
				
				// set test ID and prepare files to save results of this test
				setTestId(numTestCases);
				
				// setup some system under test specific environment correctly.. 
		        // e.g. create conf directory with the correct configurations, etc.
		        controller.prepareTestingEnvironment();
		        initializeTesting();
				saveAbstractSchedule(sched);
				// init stat collection
				statMgr = new ScheduleExecutionStatMeasurer();
				schedExecDurStatID = statMgr.startTimeMeasure("ScheduleExecutionDuration");
				// start a schedule execution
				boolean schedExecResult = executeSchedule(sched);
				if (schedExecResult) {
					if (result = invariantCheck()) {
						resultStr = "true";
					} else {
						resultStr = "false";
						numViolations++;
						buggySchedules.add(sched);
					}
				} else {
					resultStr = "incomplete";
				}
				// collect a stat and save the stat into a stat file
				schedExecDur = statMgr.endTimeMeasure(schedExecDurStatID);
				arrayOfStatIDs = new ArrayList<Integer>();
				arrayOfStatIDs.add(schedExecDurStatID);
				statMgr.printSimpleStatsToFile(strataIdRecordDirPath + "/performance.stat", arrayOfStatIDs);
				// saveResult
				System.out.println("result=" + resultStr);
				saveResult(resultStr + "\n");
				saveScheduleExecuted(sched);
				if (debugMode) {
					System.out.println("Schedule execution is done. Press 1 and Enter to continue.");
	    			reader.nextInt();
				}
				resetScheduleTesting();
				System.out.println("Testing is done for test number=" + numTestCases);
				System.out.println("-------------------------------------------------");
			}
		}
		
		/*
		 * statistics
		 */
		long elapseTime = System.currentTimeMillis() - startTime;
		System.out.println("\n==================================================");
		System.out.println("Testing Completed!");
		System.out.println("There were " + numTestCases + " test cases executed.");
		System.out.println("Testing took " + elapseTime + " ms");
		System.out.println("There was as many consistency violations as " + numViolations);
		if (!buggySchedules.isEmpty()) {
			System.out.println("Following schedules are buggy:");
			for (StrataSchedule sch : buggySchedules) {
				System.out.println(sch.toString());
			}
		}
		
		if (debugMode) {
			reader.close();
		}
	}
	
	private void setTestId(int testId) {
		this.testId = testId;
		this.strataIdRecordDirPath = strataTestRecordDirPath + "/" + testId;
		File testRecordDir = new File(strataIdRecordDirPath);
        if (!testRecordDir.exists()) {
            testRecordDir.mkdir();
        }
        strataAbstractScheduleFilePath = strataIdRecordDirPath + "/" + STRATA_ABSTRACT_SCHEDULE_FILE;
        strataResultFilePath = strataIdRecordDirPath + "/" + STRATA_RESULT_FILE; 
        strataSchedExecFilePath = strataIdRecordDirPath + "/" + STRATA_SCHED_EXEC_FILE;
	}

	private void resetScheduleTesting() {
		controller.takedownClients();
		controller.stopEnsemble();
		controller.resetTest();
		try {
			strataAbstractScheduleFile.close();
			strataSchedExecFile.close();
			strataResultFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		strataAbstractScheduleFile = null;
		strataSchedExecFile = null;
		strataResultFile = null;
	}

	private void saveAbstractSchedule(StrataSchedule sched) {
		try {
			System.out.println("saveAbstractSchedule entered");
            if (strataAbstractScheduleFile == null) {
            	strataAbstractScheduleFile = new FileOutputStream(strataAbstractScheduleFilePath);
            }
            strataAbstractScheduleFile.write(sched.toString().getBytes());
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
	}
	
	private void saveScheduleExecuted(StrataSchedule sched) {
        try {
            if (strataSchedExecFile == null) {
            	strataSchedExecFile = new FileOutputStream(strataSchedExecFilePath);
            }
            strataSchedExecFile.write(sched.toString().getBytes());
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
	
	private void saveResult(String result) {
		try {
			System.out.println("saveResult entered");
            if (strataResultFile == null) {
            	strataResultFile = new FileOutputStream(strataResultFilePath);
            }
            strataResultFile.write(result.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
	}
	
	private boolean invariantCheck() {
		
		byte[][][] nodeKeyValueByteArray = new byte[numNode][numAOP][];
		String[][] nodeKeyValueStringArray = new String[numNode][numAOP];
		
		System.out.println("============= invariantCheck BEGIN ==============");
		
		for (int i = 0; i < numNode; i++) { // orig
			for (int j = 0; j < numAOP; j++) { // orig
				nodeKeyValueByteArray[i][j] = controller.readData(i, keys[j]);
				try {
					nodeKeyValueStringArray[i][j] = new String(nodeKeyValueByteArray[i][j], "UTF-8");
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
					System.exit(1);
				}
				System.out.println("nodeID=" + i + " key=" + keys[j] + " value=" + nodeKeyValueStringArray[i][j]);
			}
		}
		for (int i = 0; i < numAOP; i++) { // orig
			for (int j = 0; j < numNode - 1; j++) {
				if (!(nodeKeyValueStringArray[j % numNode][i]).equals(nodeKeyValueStringArray[j % numNode + 1][i])) {
					System.out.println("Expecting the value of the key=" + (keys[i]) + " on the " 
							+ j + "-st and " + (j + 1) + "-st servers should be same");
					return false;
				}
			}
		}
		return true;
	}

	private void initializeTesting() {
		String path = "/testDivergenceResync";
		base = 0;
		// starting ensemble and bootstrapping client threads	
		controller.startEnsemble();
		controller.bootstrapClients();
		// create initial data records 
		for (int i = 0; i < numAOP; i++) {
			keys[i] = path + i;
		}
		for (int j = 0; j < numAOP; j++) {
   			controller.createData(0,  keys[j], j+base);
   		}
		// create the data record for probing to check system's availability
		controller.createData(0,  probingKey, 777);
		
		base = 1000;
	}

	private boolean executeSchedule(StrataSchedule sched) {
		//System.out.println("sched=" + sched.toString());
		boolean pathExplorationResult = false;
		pathCounter = 0;
		for (Path path : sched.getSchedule()) {
			if (debugMode && interruptMode) {
				System.out.println("Press 1 and Enter to explore the next path: " + path.toString());
	    		reader.nextInt();
			}
			pathCounter++;
			//System.out.println("path=" + path.toString());
			if (path.type == Path.PathType.DEVIATION) {
				controller.beforeDivergencePath();
				pathExplorationResult = executeDivergencePath(path);
				controller.afterDivergencePath();
			} else { // path.type == Path.PathType.RESYNC
				controller.beforeResyncPath();
				pathExplorationResult = executeResyncPath(path);
				controller.afterResyncPath();
				if (!pathExplorationResult) {
					return false;
				}
			}
			if (debugMode) {
				System.out.println("Debug Mode is enabled. Backup the log directory"
						+ " for the testId=" + this.testId
						+ " after executing " + pathCounter + "-th path"
						+ " to the directory=" + this.strataIdRecordDirPath);
				String[] backupLogCmd = {"cp", "-R", workingDir + "/log", 
					this.strataIdRecordDirPath + "/log-" + pathCounter };
				try {
					ProcessBuilder builder = new ProcessBuilder();
		            Process p = builder.command(backupLogCmd).start();
		            p.waitFor();
		            //Thread.sleep(100000);
		        } catch (IOException e) {
		            e.printStackTrace();
		            System.exit(1);
		        } catch (InterruptedException e) {
					e.printStackTrace();
					System.exit(1);
				}
			}
			//System.out.println("Execution is done for the path=" + path.toString());
		}
		return true;
	}
	
	private boolean executeDivergencePath(Path path) {
		System.out.println(">> divergence path=" + path.toString());
		DeviationPath dp = (DeviationPath) path;
		int[] targetState = dp.targetState;
		int numAOPInjection = 0;
		// get the number of asynchronous operations needed
		for (int i = 0; i < targetState.length; i++) {
			if (targetState[i] > numAOPInjection) {
				numAOPInjection = targetState[i];
			}
		}
		targetState = controller.sortTargetState(targetState);
		ArrayList<Integer> commitNodes = new ArrayList<Integer>();
		for (int i = 0; i < numAOPInjection; i++) {
			commitNodes.clear();
			for (int j = 0; j < numNode; j++) {
				if (targetState[j] < i + 1) {
					//System.out.println("shutting down node=" + j);
					if (targetState[j] == i) {
						controller.stopNode(j);
					}
				} else {
					//System.out.println("Let commit happen on node=" + j);
					commitNodes.add(j);
				}
			}
			controller.writeData(commitNodes.get(0), keys[vCnt], vCnt+base);
			System.out.println("Writing to node=" + commitNodes.get(0) + " key=" + keys[vCnt] + " the value=" + (vCnt + base));
			controller.waitForCommit(commitNodes, keys[vCnt], (vCnt + base));
			vCnt++;
		}
		for (int i : commitNodes) {
			//System.out.println("after commit, shutting down node=" + i);
			controller.stopNode(i);
		}
		return true;
	}
	
	private boolean executeResyncPath(Path path) {
		System.out.println(">> resync path=" + path.toString());
		//resyncCounter++;
		ResyncPath rp = (ResyncPath) path;
		int[] restartNodes = rp.devNode;
		for (int nodeID : restartNodes) {
			//System.out.println("starting nodeID=" + nodeID);
			controller.startNode(nodeID);
		}
		controller.triggerResyncPath(rp.targetSyncSourcesChange);
		//System.out.println("now waiting for resync...");
		boolean waitResult = controller.waitForResync(restartNodes);
		//System.out.println("done with resync...");
		
		if (!waitResult) {
			return false;
		}
		return true;
	}


	private void runProgrammedSchedule(String inputSchedFilePath) {
		
		/*
		 * start testing
		 */
		long startTime = System.currentTimeMillis();
		boolean result = false;
		String resultStr = null;
		int numViolations = 0;
		ArrayList<StrataSchedule> buggySchedules = new ArrayList<StrataSchedule>();
		int numTestCases = 0;
		
		System.out.println("Running Programmed Schedule=" + inputSchedFilePath);
		System.out.println("=============================================================================");
		
		vCnt = 0;
		setTestId(0); // using test id 0, for special purpose
		controller.prepareTestingEnvironment();
		initializeTesting();
		
		StrataSchedule sched = new StrataSchedule();
		boolean schedExecResult = true;
		File curSchedFile = new File(inputSchedFilePath);
		try (BufferedReader br = new BufferedReader(new FileReader(curSchedFile))) {
			pathCounter = 0;
			String line;
		    while ((line = br.readLine()) != null) {
		    	boolean pathExplorationResult = false;
				String[] bigTokens = line.split("=");
				pathCounter++;
		    	String pathType = bigTokens[1].split(" ")[0];
		    	System.out.println("Parsed pathType=" + pathType);
		    	if (pathType.equals("DEVIATION")) {
		    		String targetStateStr = bigTokens[2];
		    		String[] items = targetStateStr.replaceAll("\\[", "").replaceAll("\\]", "").replaceAll("\\s", "").split(",");
		    		int[] targetState = new int[items.length];
		    		for (int i = 0; i < items.length; i++) {
		    		    try {
		    		    	targetState[i] = Integer.parseInt(items[i]);
		    		    } catch (NumberFormatException nfe) {
		    		        //NOTE: write something here if you need to recover from formatting errors
		    		    };
		    		}
		    		Path path = new DeviationPath(targetState);
		    		sched.sched.add(path);
		    		controller.beforeDivergencePath();
		    		pathExplorationResult = executeDivergencePath(path);
		    		controller.afterDivergencePath();
		    		if (!pathExplorationResult) {
    					System.err.println("DivergencePath execution returned false");
    					schedExecResult = false;
    					break;
    				}
		    	} else {
		    		String devNodeStr = bigTokens[2].split("]")[0];
		    		String[] items = devNodeStr.replaceAll("\\[", "").replaceAll("\\]", "").replaceAll("\\s", "").split(",");
		    		//System.out.println("devNodeStr=" + devNodeStr);
		    		//System.out.println("items=" + Arrays.deepToString(items));
		    		int[] devNode = new int[items.length];
		    		for (int i = 0; i < items.length; i++) {
		    		    try {
		    		    	devNode[i] = Integer.parseInt(items[i]);
		    		    } catch (NumberFormatException nfe) {
		    		        //NOTE: write something here if you need to recover from formatting errors
		    		    };
		    		}
		    		String rawSourcesStr = bigTokens[3].split("]")[0];
		    		//System.out.println("rawSourcesStr=" + rawSourcesStr);
		    		String filteredSourcesStr = rawSourcesStr.replaceAll("\\[|\\]", "").replaceAll(",", "");
		    		//System.out.println("filteredSourcesStr=" + filteredSourcesStr);
		    		String[] sourcesStrArr = filteredSourcesStr.split(" ");
		    		int[] syncSources = new int[sourcesStrArr.length];
		    		for (int i = 0; i < syncSources.length; i++) {
		    			syncSources[i] = Integer.parseInt(sourcesStrArr[i]);
		    		}
		    		String rawTargetsStr = bigTokens[4];
		    		//System.out.println("rawTargetsStr=" + rawTargetsStr);
		    		String filteredTargetsStr = rawTargetsStr.replaceAll("\\[|\\]", "").replaceAll(",", "");
		    		//System.out.println("filteredTargetsStr=" + filteredTargetsStr);
		    		String[] targetsStrArr = filteredTargetsStr.split(" ");
		    		int[] syncTargets = new int[targetsStrArr.length];
		    		for (int i = 0; i < syncTargets.length; i++) {
		    			syncTargets[i] = Integer.parseInt(targetsStrArr[i]);
		    		}
		    		//System.out.println("syncSources=" + Arrays.toString(syncSources));
		    		//System.out.println("syncTargets=" + Arrays.toString(syncTargets));
		    		Path path = new ResyncPath(devNode, syncSources, syncTargets);
		    		//Path path = new ResyncPath(devNode);
		    		sched.sched.add(path);
		    		controller.beforeResyncPath();
		    		pathExplorationResult = executeResyncPath(path);
		    		controller.afterResyncPath();
    				if (!pathExplorationResult) {
    					System.err.println("ResyncPath execution returned false");
    					schedExecResult = false;
    					break;
    				}
		    		
    				// use followings after modifying if you want to debug each path execution
		    		//if (debugMode) {
		    		//	System.out.println("Press 1 and Enter to explore the next path: " + path.toString());
		    	    //	reader.nextInt();
		    		//}
		    	}
		    	
		    	// making backup of log directory
		    	System.out.println("Program Mode is enabled. Backup the log directory"
						+ " for the testId=" + this.testId
						+ " after executing " + pathCounter + "-th path"
						+ " to the directory=" + this.strataIdRecordDirPath);
				//String[] backupLogCmd = {"cp", "-R", workingDir + "/log", 
				//	this.strataIdRecordDirPath + "/log-" + pathCounter };
		    	String[] backupLogCmd = {"cp", "-R", workingDir + "/redis_dir", 
						this.strataIdRecordDirPath + "/log-" + pathCounter };
				try {
					ProcessBuilder builder = new ProcessBuilder();
		            Process p = builder.command(backupLogCmd).start();
		            p.waitFor();
		            //Thread.sleep(100000);
		        } catch (IOException e) {
		            e.printStackTrace();
		            System.exit(1);
		        } catch (InterruptedException e) {
					e.printStackTrace();
					System.exit(1);
				}
				
		    }
		    if (schedExecResult) {
				if (result = invariantCheck()) {
					resultStr = "true";
				} else {
					resultStr = "false";
				}
			} else {
				resultStr = "incomplete";
			}
		    controller.takedownClients();
			controller.stopEnsemble();
			controller.resetTest();
			System.out.println("Testing is done for test number=" + numTestCases);
			System.out.println("-------------------------------------------------");
		    System.out.println("Result=" + resultStr);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void parseConfigInit(String configFile) {
		File confFile = new File(configFile);
		if (!confFile.exists()) {
			if (!confFile.mkdir()) {
				System.err.println("Unable to find " + confFile);
	            System.exit(1);
	        }
		}
		try (BufferedReader br = new BufferedReader(new FileReader(configFile))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		       if (line.startsWith("numAOP")) {
		    	   String[] tokens = line.split("=");
		    	   numAOP = Integer.parseInt(tokens[1]);
		       } else if (line.startsWith("numNode")) {
		    	   String[] tokens = line.split("=");
		    	   numNode = Integer.parseInt(tokens[1]);
		       } else if (line.startsWith("workloadDirNamePrefix")) {
		    	   String[] tokens = line.split("=");
		    	   workloadDirNamePrefix = tokens[1];
		       //} else if (line.startsWith("strataSrcDir")) {
		       //   String[] tokens = line.split("=");
		       //   strataSrcDir = tokens[1];
		       } else if (line.startsWith("workingDirPrefix")) {
		    	   String[] tokens = line.split("=");
		    	   workingDirPrefix = tokens[1];
		       } else if (line.startsWith("sysType")) {
		    	   String[] tokens = line.split("=");
		    	   String sysTypeStr = tokens[1];
		    	   if (sysTypeStr.equals("zookeeper")) {
		    		   sysType = SystemUnderTestType.ZooKeeper;
		    	   } else if (sysTypeStr.equals("cassandra")) {
		    		   sysType = SystemUnderTestType.Cassandra;
		    	   } else if (sysTypeStr.equals("Couchbase")) {
		    		   sysType = SystemUnderTestType.Couchbase; 
		    	   } else if (sysTypeStr.equals("MongoDB")) {
		    		   sysType = SystemUnderTestType.MongoDB;
		    	   } else if (sysTypeStr.equals("HBase")) {
		    		   sysType = SystemUnderTestType.HBase;
		    	   } else if (sysTypeStr.equals("Redis")) {
		    		   sysType = SystemUnderTestType.Redis;
		    	   }
		       } else if (line.startsWith("sutVersion")) {
		    	   String[] tokens = line.split("=");
		    	   sutVersion = tokens[1];
		       } else if (line.startsWith("strataVersion")) {
		    	   String[] tokens = line.split("=");
		    	   strataVersion = tokens[1];
		       } else if (line.startsWith("sutConfig")) {
		    	   String[] tokens = line.split("=");
		    	   sysCntrConfigName = tokens[1];
		       }
		    }
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main (String[] args) {
		
		String configFile = null;
		int tmpTestNum = -1;
		String inputSchedFilePath = null;
		// check if the debug mode is enabled
		for (int i = 0; i < args.length; i++) {
			String arg = args[i];
			if (arg.equals("-c") && i < args.length - 1) {
				System.out.println("configuration file is given=" + args[i+1]);
				configFile = args[i+1];
			} else if (arg.equals("-d") && i < args.length - 1) {
				System.out.println("debugMode enabled");
				tmpTestNum = Integer.parseInt(args[i+1]);
				debugMode = true;
				interruptMode = true;
			} else if (arg.equals("-n") && i < args.length - 1) {
				System.out.println("non-interruptible debug Mode enabled");
				tmpTestNum = Integer.parseInt(args[i+1]);
				debugMode = true;
			} else if (arg.equals("-i")) {
				System.out.println("programMode enabled");
				programMode = true;
			}
		}
		if (configFile == null) {
			System.err.println("ERROR configFile is not given..");
			System.exit(1);
		}
		TestingEngine engine = new TestingEngine(configFile);
		if (debugMode) {
			engine.testNum = tmpTestNum;
			engine.reader = new Scanner(System.in);
		}
		if (programMode) {
			inputSchedFilePath = workingDir + "/progSched";
			engine.runProgrammedSchedule(inputSchedFilePath);
		} else {
			engine.startTesting();
		}
	}

}
