package strata.conex.zookeeper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import strata.conex.SystemController;
import strata.conex.TestingEngine;

public class ZooKeeperSystemController extends SystemController {

	protected String sutSrcDir; //= "/home/kroud2/bhkim/zookeeper"; // "/home/ben/project/zookeeper-hacking/zookeeper"; // HARD-CODED
	protected String libClasspath; 
		//= ":" + TestingEngine.workingDir
		//+ ":" + sutSrcDir + "/.eclipse/classes-main";
		//+ ":/home/kroud2/.ant/cache/org.slf4j/slf4j-api/jars/slf4j-api-1.6.1.jar"
		//+ ":/home/kroud2/.ant/cache/org.slf4j/slf4j-log4j12/jars/slf4j-log4j12-1.6.1.jar"
		//+ ":/home/kroud2/.ant/cache/log4j/log4j/bundles/log4j-1.2.16.jar"
		//+ ":/home/kroud2/.ant/cache/jline/jline/jars/jline-0.9.94.jar"
		//+ ":/home/kroud2/.ant/cache/io.netty/netty/jars/netty-3.10.5.Final.jar"
		//+ ":/home/kroud2/.ant/cache/junit/junit/jars/junit-4.8.1.jar"
		//+ ":/home/kroud2/.ant/cache/org.mockito/mockito-all/jars/mockito-all-1.8.2.jar"
		//+ ":/home/kroud2/.ant/cache/checkstyle/checkstyle/jars/checkstyle-5.0.jar"
		//+ ":/home/kroud2/.ant/cache/commons-collections/commons-collections.jars/commons-collections-3.2.2.jar"; // HARD-CODED

	protected String updatedClasspath = null;
	protected String sutLogConfig;

        public static int CONNECTION_TIMEOUT = 30000;

        final int clientPorts[] = new int[numNode];
	ZooKeeper[] zkClients;
	
	protected enum CommitWaitMode {CWM_SLEEP, CWM_PROBE};
	protected enum ResyncWaitMode {RWM_SLEEP, RWM_PROBE};
	protected int waitForCommitDuration; // = 3000; // ms
	protected int waitForResyncDuration; // = 3000; // ms
	public CommitWaitMode cwmMode; //= CommitWaitMode.CWM_SLEEP; // HARD-CODED
	public ResyncWaitMode rwmMode; //= ResyncWaitMode.RWM_PROBE; // HARD-CODED
	
	protected String[] logFileNames;
	final String logFileName = "zookeeper.log"; // HARD-CODED
	
	public boolean[] isNodeOnline;
	public int actualLeader = -1;
	
	static final String[] CMD = { "java", "-cp", "%s", "-Xmx1G",
	        "-Dzookeeper.log.dir=%s/log/%d", "-Dapple.awt.UIElement=true", 
	        "-Dlog4j.configuration=%s", "org.apache.zookeeper.server.quorum.QuorumPeerMain", 
	        "conf/%d" }; // HARD-CODED
	
	public ZooKeeperSystemController(int numNode, String workingdir) {
		super(numNode, workingdir);
		zkClients = new ZooKeeper[numNode];
		logFileNames = new String[numNode];
		isNodeOnline = new boolean[numNode];
		System.out.println("ZooKeeperSystemController is constructed");
	}
	

	@Override
	public void sysCtrlParseConfigInit(String configFileName) {
		File confFile = new File(configFileName);
		if (!confFile.exists()) {
			if (!confFile.mkdir()) {
				System.err.println("Unable to find " + confFile);
	            System.exit(1);
	        }
		}
		try (BufferedReader br = new BufferedReader(new FileReader(confFile))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		       if (line.startsWith("sutSrcDir")) {
		    	   String[] tokens = line.split("=");
		    	   sutSrcDir = tokens[1];
		    	   System.out.println("sutSrcDir=" + sutSrcDir);
		       } else if (line.startsWith("libClasspath")) {
		    	   String[] tokens = line.split("=");
		    	   libClasspath = ":" + TestingEngine.workingDir
		    				+ ":" + sutSrcDir + "/.eclipse/classes-main";
		    	   libClasspath += tokens[1];
		    	   System.out.println("tokens[1]=" + tokens[1]);
		    	   System.out.println("libClasspath=" + libClasspath);
		       } else if (line.startsWith("sutLogConfig")) {
		    	   String[] tokens = line.split("=");
		    	   sutLogConfig = tokens[1];
		       } else if (line.startsWith("waitForCommitDuration")) {
		    	   String[] tokens = line.split("=");
		    	   waitForCommitDuration = Integer.parseInt(tokens[1]);
		       } else if (line.startsWith("waitForResyncDuration")) {
		    	   String[] tokens = line.split("=");
		    	   waitForResyncDuration = Integer.parseInt(tokens[1]);
		       } else if (line.startsWith("cwmMode")) {
		    	   String[] tokens = line.split("=");
		    	   if (tokens[1].equals("sleep")) {
		    		   cwmMode = CommitWaitMode.CWM_SLEEP;
		    	   } else if (tokens[1].equals("probe")) {
		    		   cwmMode = CommitWaitMode.CWM_PROBE;
		    	   } else {
		    		   System.err.println("Unknown cwm mode =" + tokens[1]);
		    		   System.exit(1);
		    	   }
		       } else if (line.startsWith("rwmMode")) {
		    	   String[] tokens = line.split("=");
		    	   if (tokens[1].equals("sleep")) {
		    		   rwmMode = ResyncWaitMode.RWM_SLEEP;
		    	   } else if (tokens[1].equals("probe")) {
		    		   rwmMode = ResyncWaitMode.RWM_PROBE;
		    	   } else {
		    		   System.err.println("Unknown rwm mode =" + tokens[1]);
		    		   System.exit(1);
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
	}

	@Override
	public void prepareTestingEnvironment() {
		System.out.println("setupEnvironment");
		File confDirFile = new File(workingDir + "/conf");
		System.out.println("checking directory for nodes' configuration files");
		if (!confDirFile.exists()) {
			if (!confDirFile.mkdir()) {
				System.err.println("Unable to mkdir " + confDirFile);
	            System.exit(1);
	        }
		}
		File dataDirFile = new File(workingDir + "/data");
		System.out.println("checking directory for nodes' data files");
		if (!dataDirFile.exists()) {
			if (!dataDirFile.mkdir()) {
				System.err.println("Unable to mkdir " + dataDirFile);
	            System.exit(1);
	        }
		}
		File logPropFile = new File(sutLogConfig);
		System.out.println("checking log4j property file");
		if (!logPropFile.exists()) {
			System.err.println("Unable to find log4j property file=" + logPropFile);
			System.exit(1);
		} else {
			String[] cpCmd = { "cp", "%s", "%s" };
			cpCmd[1] = String.format(cpCmd[1], sutLogConfig);
			cpCmd[2] = String.format(cpCmd[2], workingDir);
            try {
            	System.out.println("Invoking cpCmd=\n" + String.join(" ", cpCmd));
            	ProcessBuilder builder = new ProcessBuilder();
				builder.command(cpCmd).start();
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		File logDirFile = new File(workingDir + "/log");
		System.out.println("checking directory for nodes' log files");
		if (!logDirFile.exists()) {
			if (!logDirFile.mkdir()) {
				System.err.println("Unable to mkdir " + logDirFile);
	            System.exit(1);
	        }
		}
		for (int i = 0; i < numNode; i++) {
			logFileNames[i] = workingDir + "/log" + "/" + i + "/" + logFileName;
		}
		// prepare a client port for each node just in case
		StringBuilder sb = new StringBuilder();
		int nextPort = 11221;
		for(int i = 0; i < numNode; i++) {
  	        clientPorts[i] = nextPort++;
  	        sb.append("server."+i+"=127.0.0.1:" + (nextPort++) + ":" + (nextPort++) +"\n");
  	    }
		String quorumCfgSection = sb.toString();
  	    for (int i = 0; i < numNode; i++) {
			// check and create a data directory for each node
			File nodeDataDirFile = new File(workingDir + "/data" + "/" + i);
			System.out.println("checking directory for node " + i + "'s data file");
			if (!nodeDataDirFile.exists()) {
				if (!nodeDataDirFile.mkdir()) {
					System.err.println("Unable to mkdir " + nodeDataDirFile);
		            System.exit(1);
		        }
			}
			FileWriter fwriter;
			File myidFile = new File(nodeDataDirFile, "myid");
			if (!myidFile.exists()) {
	            try {
					fwriter = new FileWriter(myidFile);
					fwriter.write(Integer.toString(i));
		            fwriter.flush();
		            fwriter.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
            // check and create configuration file for each node
			File nodeConfFile = new File(workingDir + "/conf" + "/" + i);
			System.out.println("checking configuration file for node " + i);
			if (!nodeConfFile.exists()) {
				System.out.println("No conf file found for the node. So, we are creating configuration file for node " + i);
				try {
					fwriter = new FileWriter(nodeConfFile);
					fwriter.write("tickTime=4000\n");
		            fwriter.write("initLimit=10\n");
		            fwriter.write("syncLimit=5\n");
		            fwriter.write("dataDir=" + dataDirFile.toString() + "/" + i + "\n");
		            fwriter.write("clientPort=" + clientPorts[i] + "\n");
		            fwriter.write(quorumCfgSection + "\n");
		            fwriter.flush();
		            fwriter.close();
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}
			}
			
			File nodeLogDirFile = new File(workingDir + "/log" + "/" + i);
			System.out.println("checking directory for node " + i + "'s log file");
			if (!nodeLogDirFile.exists()) {
				if (!nodeLogDirFile.mkdir()) {
					System.err.println("Unable to mkdir " + nodeLogDirFile);
		            System.exit(1);
		        }
			}
		}
  	    
  	    /*
  	     * update the classpath
  	     */
		//String addClasspath = sutSrcDir + "/.eclipse/classes-main";
		//addClasspath = addClasspath + ":" + homeDir;
		//updatedClasspath = System.getenv("CLASSPATH") + ":" + addClasspath + libClasspath;
  	    updatedClasspath = System.getenv("CLASSPATH") + libClasspath;
		System.out.println("updated classpath=" + updatedClasspath);
		
	}
	
	@Override
	public int findLeader() {
		System.out.println("findLeader");
		String[] resStrArray = new String[numNode];
    	for (int i = 0 ; i < logFileNames.length ; i++) {
    		if (!isNodeOnline[i]) {
    			resStrArray[i] = "OFFLINE";
    			continue;
    		}
    		try  {
    			String state = "LOOKING";
    			String logFileName = logFileNames[i];
    			//System.out.println("Node=" + i + " logFileName=" + logFileName);
    			BufferedReader reader = new BufferedReader(new FileReader(logFileName));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] token = line.split("-");
                    if (token.length > 3) {
                        if (token[4].trim().equals("LEADING")) {
                            state = "LEADING";
                        } else if (token[4].trim().equals("FOLLOWING")) {
                            state = "FOLLOWING";
                        } else if (token[4].trim().equals("LOOKING")) {
                            state = "LOOKING";
                        }
                    }
                }
                resStrArray[i] = state;
                reader.close();
    		} catch (FileNotFoundException e) {
    			resStrArray[i] = "FileNotFoundException";
                continue;
            } catch (IOException e) {
            	resStrArray[i] = "IOException";
                continue;
            }
    	}
		for (int i = 0; i < numNode; i++) {
			String str = resStrArray[i];
			//System.out.println("node=" + i + " role=" + str);
			if (str.equals("LEADING")) {
				System.out.println("  leader id=" + i);
				return i;
			}
		}
		return -1;
	}

	@Override
	public void bootstrapClients() {
		System.out.println("bootstrapClients");
		for (int i = 0; i < numNode; i++) {
			try {
				zkClients[i] = new ZooKeeper("127.0.0.1:" + clientPorts[i], CONNECTION_TIMEOUT, null);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		try {
			Thread.sleep(300);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void takedownClients() {
		System.out.println("takedownClients");
		for (int i = 0; i < numNode; i++) {
			try {
				zkClients[i].close();
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	@Override
	public void startEnsemble() {
		System.out.println("startEnsemble");
		ProcessBuilder builder = new ProcessBuilder();
		builder.directory(new File(workingDir));
		for (int i = 0; i < numNode; ++i) {
			String[] cmd = Arrays.copyOf(CMD, CMD.length);
			cmd[2] = String.format(cmd[2], updatedClasspath);
			cmd[4] = String.format(cmd[4], workingDir, i);
            cmd[6] = String.format(cmd[6], "zk_log.properties"); // HARD-CODED
            cmd[8] = String.format(cmd[8], i);
            System.out.println("Starting node " + i);
            try {
            	System.out.println("Invoking CMD=\n" + String.join(" ", cmd));
				nodeProc[i] = builder.command(cmd).start();
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		try {
			Thread.sleep(300);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Arrays.fill(isNodeOnline, true);
	}

	@Override
	public void stopEnsemble() {
		System.out.println("stopEnsemble");
		for (Process node : nodeProc) {
            node.destroy();
        }
        for (Process node : nodeProc) {
            try {
                node.waitFor();
            } catch (InterruptedException e) {
            	e.printStackTrace();
            	System.exit(1);
            }
        }
        Arrays.fill(isNodeOnline, false);
	}
	
	@Override
	public void startNode(int id) {
		System.out.println("startNode " + id);
		ProcessBuilder builder = new ProcessBuilder();
		builder.directory(new File(workingDir));
		
		// try to clean up the log file from the previous lifetime of this node
		/*if (TestingEngine.debugMode) {
			System.out.println("Debug Mode is enabled. Backup the log directory");
			String[] backupLogCmd = {"cp", workingDir + "/log/" + id + "/zookeeper.log", 
				workingDir + "/log/" + id + "/zookeeper.log-" + TestingEngine.resyncCounter};
			try {
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
		}*/
		String[] rmLogCmd = { "rm", "-f", workingDir + "/log/" + id + "/zookeeper.log" };
		try {
            Process p = builder.command(rmLogCmd).start();
            p.waitFor();
            //Thread.sleep(100000);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		// start the node now
		String[] cmd = Arrays.copyOf(CMD, CMD.length);
		cmd[2] = String.format(cmd[2], updatedClasspath);
		cmd[4] = String.format(cmd[4], workingDir, id);
		cmd[6] = String.format(cmd[6], "zk_log.properties"); // HARD-CODED
        cmd[8] = String.format(cmd[8], id);
        System.out.println("Starting node " + id);
        try {
        	System.out.println("Invoking CMD=\n" + String.join(" ", cmd));
			nodeProc[id] = builder.command(cmd).start();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		isNodeOnline[id] = true;
	}

	@Override
	public void stopNode(int id) {
		System.out.println("stopNode " + id);
		nodeProc[id].destroy();
		try {
			nodeProc[id].waitFor();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		isNodeOnline[id] = false;
	}

	@Override
	public void resetTest() {
		System.out.println("resetTest");
		if (TestingEngine.programMode) {
			System.out.println("Program Mode is enabled. We don't need to reset anything here.");
			return;
		}
		// delete data directory
        ProcessBuilder builder = new ProcessBuilder();
        builder.directory(new File(workingDir));
        String[] rmDataCmd = { "rm", "-rf", workingDir + "/data", workingDir + "/log" };
        try {
            Process p = builder.command(rmDataCmd).start();
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

	private boolean waitForConnect(ArrayList<Integer> nodes) {
		int waitCntCap = 100;
		int waitCntWarn = 98;
		int waitSleepDur = 600;
		int waitCnt = 0;
		String key = TestingEngine.probingKey;
		for (Integer nodeID : nodes) {
			while(true) {
				try {
					System.out.println("Try to getData");
					zkClients[nodeID].getData(key, false, null);
					System.out.println("connected");
					break;
				} catch (KeeperException e) {
					if (e.code().equals(KeeperException.Code.NONODE)) {
						// We are connected but the probing key does not exist. It's very wierd. Early exit.
						System.err.println("No such key exists");
						return false;
						//System.exit(1);
					} else if (e.code().equals((KeeperException.Code.SESSIONEXPIRED))) {
						try {
							System.err.println("session expired");
							zkClients[nodeID] = new ZooKeeper("127.0.0.1:" + clientPorts[nodeID], CONNECTION_TIMEOUT, null);
						} catch (IOException ex) {
							ex.printStackTrace();
							return false;
							//System.exit(1);
						}
					} else if (e.code().equals((KeeperException.Code.CONNECTIONLOSS))){
						System.err.println("connection is lost");
						//connection is not established yet
						if (waitCnt > waitCntWarn) {
							e.printStackTrace();
						}
					} else {
						System.err.println("Unhandled exception.. Deal with it");
						System.exit(1);
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if (waitCnt > waitCntWarn) {
					System.out.println("[WARNING] Wait for " + waitSleepDur + " ms");
				}
				waitCnt++;
				try {
					Thread.sleep(waitSleepDur);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				if (waitCnt % waitCntCap == 0) {
					System.err.println("timeout for getData.. Perhaps there is no ZooKeeper node running.." 
							+ " It's possible leader election had some unexpected exceptions rainsed");
					return false;
					//System.exit(1);
				}
			}
		}
		return true;
	}
	
	@Override
	public byte[] readData(int nodeID, String key) {
		System.out.println("readData node=" + nodeID + " key=" + key);
		
		// now, actually try to read for the given key from the given node
		byte[] retBytes = null;
		while (true) {
			try {
				retBytes = zkClients[nodeID].getData(key, false, null);
				break;
			} catch (KeeperException e) {
				if (e.code().equals((KeeperException.Code.SESSIONEXPIRED))) {
					try {
						zkClients[nodeID] = new ZooKeeper("127.0.0.1:" + clientPorts[nodeID], CONNECTION_TIMEOUT, null);
					} catch (IOException ex) {
						ex.printStackTrace();
						System.exit(1);
					}
				} else {
					// it seems connection is not established, retry
					continue;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		return retBytes;
	}

	@Override
	public void createData(int nodeID, String key, int value) {
		System.out.println("createData node=" + nodeID + " key=" + key + " value=" + value);

		byte[] valToWrite = (value + "").getBytes();
		while (true) {
			try {
				zkClients[nodeID].create(key, valToWrite, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				break;
			} catch (KeeperException e) {
				if (e.code().equals((KeeperException.Code.SESSIONEXPIRED))) {
					try {
						zkClients[nodeID] = new ZooKeeper("127.0.0.1:" + clientPorts[nodeID], CONNECTION_TIMEOUT, null);
					} catch (IOException ex) {
						ex.printStackTrace();
						System.exit(1);
					}
				} else {
					// it seems connection is not established, retry
					continue;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	@Override
	public void writeData(int nodeID, String key, int value) {
		System.out.println("writeData node=" + nodeID + " key=" + key + " value=" + value);
		
		byte[] valToWrite = (value + "").getBytes();
		zkClients[nodeID].setData(key, valToWrite, -1, null, null);
	}

	@Override
	public int[] sortTargetState(int[] targetState) {
		System.out.println("sortTargetState=" + Arrays.toString(targetState));
		System.out.println("  Note: node online status=" + Arrays.toString(isNodeOnline));
		
		// sanity checks:
		// 1. check if the length of the targetState equals to the number of nodes
		// 2. see if the number of non-zero values in targetState equals to the number of online nodes 
		if (targetState.length != numNode) {
			System.err.println("Sanity check failed in sortTargetState. targetState length=" + targetState.length
					+ " numNode=" + numNode);
			System.exit(1);
		}
		int numNonZero = 0;
		int numOnlineNode = 0;
		for (int i = 0; i < numNode; i++) {
			if (targetState[i] > 0) {
				numNonZero++;
			}
			if (isNodeOnline[i]) {
				numOnlineNode++;
			}
		}
		if (numNonZero > numOnlineNode) {
			System.err.println("Sanity check failed in sortTargetState. There are " + numNonZero 
					+ " many non zero values in the targetState, but there are " + numOnlineNode
					+ " many online nodes currently.");
			System.exit(1);
		}
		
		// first sort so that only online nodes will be assigned with the positive number
		ArrayList<Integer> nonZeroValues = new ArrayList<Integer>();
		ArrayList<Integer> onlineNodeIDsWithZeroAssigned = new ArrayList<Integer>();
		for (int i = 0; i < numNode; i++) {
			if (!isNodeOnline[i]) {
				if (targetState[i] > 0) {
					nonZeroValues.add(targetState[i]);
					targetState[i] = 0;
				}
			} else {
				if (targetState[i] == 0) {
					onlineNodeIDsWithZeroAssigned.add(i);
				}
			}
		}
		for (int i = 0; i < nonZeroValues.size(); i++) {
			targetState[onlineNodeIDsWithZeroAssigned.get(i)] = nonZeroValues.get(i);
			if (nonZeroValues.get(i) == 0) {
				System.err.println("failed during sorting the target state. we assigned zero value again.");
				System.exit(1);
			}
		}
		// sanity check. now all nodes assigned with the non-zero value must be online
		for (int i = 0; i < numNode; i++) {
			if (targetState[i] > 0) {
				if (!isNodeOnline[i]) {
					System.err.println("failed to sort correctly. non-zero values may be assigned to some offline nodes");
					System.exit(1);
				}
			} else if (targetState[i] < 0) {
				System.err.println("failed to sort correctly. somehow negative values are assigned to the targetState");
				System.exit(1);
			}
		}
		
		// At this point, any node with non-zero value in targetState is guaranteed to be online.
		// sort so that the leader will be assigned the highest value
		int guessedLeader = -1;
		int highestStateNode = 0;
		for (int i = 0; i < targetState.length; i++) {
			if (targetState[highestStateNode] < targetState[i]) {
				highestStateNode = i;
			}
		}
		guessedLeader = highestStateNode;
		if (actualLeader < 0) {
			actualLeader = findLeader();
		}
		if (guessedLeader != actualLeader) {
			int tmpStateStore = targetState[guessedLeader];
			try {
				targetState[guessedLeader] = targetState[actualLeader];
			} catch (ArrayIndexOutOfBoundsException e) {
				java.util.Date date = new java.util.Date();
			    System.out.println(date);
			    e.printStackTrace();
			}
			targetState[actualLeader] = tmpStateStore;
		}
		System.out.println("  returning targetState=" + Arrays.toString(targetState));
		return targetState;
	}

	@Override
	protected boolean waitForCommit(ArrayList<Integer> commitNodes, String key, int val) {
		System.out.println("waitForCommit nodes=" + Arrays.deepToString(commitNodes.toArray()) + " key=" + key + " val=" + val);
		if (cwmMode.equals(CommitWaitMode.CWM_SLEEP)) {
			try {
				Thread.sleep(waitForCommitDuration);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("wait for commit using probing key");
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			for (int nodeID : commitNodes) {
				
			}
		}
		return true;
	}

	@Override
	protected boolean waitForResync(int[] restartNodeIDs) {
		System.out.println("waitForResync nodes=" + Arrays.toString(restartNodeIDs));
		if (rwmMode.equals(ResyncWaitMode.RWM_SLEEP)) {
			System.out.println("resync wait using sleep");
			try {
				Thread.sleep(waitForResyncDuration);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("wait for resync using probing key");
			ArrayList<Integer> nodes = new ArrayList<Integer>();
			for (int id : restartNodeIDs) {
				nodes.add(id);
			}
			boolean retBool = waitForConnect(nodes);
			if (!retBool) {
				System.err.println("waitForConnect failed in waitForResync");
				return false;
			}
			actualLeader = -1;
			int waitCnt = 0;
			while (true) {
				actualLeader = findLeader();
				if (actualLeader >= 0) {
					break;
				}
				if (waitCnt > 100) {
					System.err.println("Couldn't find leader.. ");
					return false;
				}
				try {
					Thread.sleep(600);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				waitCnt++;
			}
		}
		return true;
	}


	@Override
	public void beforeDivergencePath() {
		// NOP
		
	}


	@Override
	public void afterDivergencePath() {
		// NOP
		
	}


	@Override
	public void beforeResyncPath() {
		// NOP
		
	}


	@Override
	public void afterResyncPath() {
		// NOP
		
	}

	
}
