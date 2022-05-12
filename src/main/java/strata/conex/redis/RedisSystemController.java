package strata.conex.redis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisException;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import strata.conex.SystemController;
import strata.conex.TestingEngine;

public class RedisSystemController extends SystemController {

	// HARD-CODED redis base port
	int basePort = 6379;
	
	// HARD-CODED redis cluster name
	final String clusterName = "mymaster";
	
	// HARD-CODED redis node role String
	final String MASTER_ROLE = "master";
	final String SLAVE_ROLE = "slave";
	
	//HARD-CODED sentinel configuration parameters
	//final int downAfterMilliseconds = 500 * 2;
	//final int failoverTimeout = 3000 * 2;
	
	final int injectedDelayDuration = 8000; //milliseconds

	protected String sutSrcDir;
	protected String deployScriptDir;
	protected String sutLogConfig;
	
	protected enum CommitWaitMode {CWM_SLEEP, CWM_PROBE};
	protected enum ResyncWaitMode {RWM_SLEEP, RWM_PROBE};
	protected int waitForCommitDuration; // = 3000; // ms
	protected int waitForResyncDuration; // = 3000; // ms
	public CommitWaitMode cwmMode; //= CommitWaitMode.CWM_SLEEP; // HARD-CODED
	public ResyncWaitMode rwmMode; //= ResyncWaitMode.RWM_PROBE; // HARD-CODED
	
	//protected String[] serverLogFileNames;
	final String serverLogFileName = "redis-server.log"; // HARD-CODED
	//protected String[] sentinelLogFileNames;
	//final String sentinelLogFileName = "redis-sentinel.log"; // HARD-CODED

	public boolean[] isNodeOnline;
	public int actualLeader = -1;
	
	public int[] pidMap; // indexed by node IDs
	//private int[] sentinelPidMap;	
	
	RedisClient[] redisClients;
	StatefulRedisConnection<String, String>[] connections;
	RedisCommands<String, String>[] commandsArray;
	
	public int[] epochCnts;
	//public int[] txnCnts;
	public String[] nodeRoles;
	public int[] syncSources;
	
	@SuppressWarnings("unchecked")
	public RedisSystemController(int numNode, String workingdir) {
		super(numNode, workingdir);
		isNodeOnline = new boolean[numNode];
		redisClients = new RedisClient[numNode];
		connections = new StatefulRedisConnection[numNode];
		commandsArray = new RedisCommands[numNode];
		//serverLogFileNames = new String[numNode];
		//sentinelLogFileNames = new String[numNode];
		pidMap = new int[numNode];
		//sentinelPidMap = new int[numNode];
		epochCnts = new int[numNode];
		//txnCnts = new int[numNode];
		nodeRoles = new String[numNode];
		syncSources = new int[numNode];
		Arrays.fill(epochCnts, 0);
		//Arrays.fill(txnCnts, 0);
		Arrays.fill(nodeRoles, "none");
		Arrays.fill(syncSources, -1);
		System.out.println("RedisSystemController is constructed");
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
		       } else if (line.startsWith("deployScriptDir")) { 
		    	   String[] tokens = line.split("=");
		    	   deployScriptDir = tokens[1];
		    	   System.out.println("deployScriptDir=" + deployScriptDir);
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
		// run the setup.sh script to prepare all the environment settings
		String[] setupCmd = { "./setup.sh", "%s", "%s" };
		setupCmd[1] = String.format(setupCmd[1], numNode);
		setupCmd[2] = String.format(setupCmd[2], workingDir);
		ProcessBuilder builder = new ProcessBuilder();
		builder.directory(new File(deployScriptDir));
		try {
			System.out.println("Invoking CMD=\n" + String.join(" ", setupCmd));
			Process p = builder.command(setupCmd).start();
			p.waitFor();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(1);
		}
		//for (int i = 0; i < numNode; i++) {
			//serverLogFileNames[i] = workingDir + "/" + i + "/" + serverLogFileName;
			//sentinelLogFileNames[i] = workingDir + "/" + i + "/" + sentinelLogFileName;
		//}
		//startSentinels();
	}

	@Override
	public int findLeader() {
		System.out.println("findLeader");
		int leaderID = -1;
		for (int i = 0; i < numNode; i++) {
			if (isNodeOnline[i] && (syncSources[i] == -1)) {
				//if (leaderID > -1) {
				//	System.err.println("Assert: it seems we have more than one leader");
				//	System.exit(1);
				//} else {
					leaderID = i;
				//}
			}
		}
		return leaderID;
		
		/*
		int leaderID = -1;
		for (int i = 0; i < numNode; i++) {
			System.out.println("node i=" + i 
					+ " role=" + nodeRoles[i]);
			if (nodeRoles[i].equals(MASTER_ROLE)) {
				leaderID = i;
			}
		}
		return leaderID;
		*/ 
		
		/*
		// using sentinel
		RedisClient redisCli = RedisClient.create("redis://localhost:"  + (20000 + basePort));
		StatefulRedisSentinelConnection<String, String> redisSentinelConn = redisCli.connectSentinel();
		RedisSentinelCommands<String, String> redisSentinalCmds = redisSentinelConn.sync();
		SocketAddress addr = redisSentinalCmds.getMasterAddrByName("mymaster");
		redisSentinelConn.close();
		InetSocketAddress inetSockAddr = (InetSocketAddress) addr;
		int masterPort = inetSockAddr.getPort();
		int masterNodeID = masterPort - basePort;
		return masterNodeID;
		*/

		/*
		// using logfile
		int leaderID = -1;
		String logFileName = sentinelLogFileNames[0];
		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(logFileName));
			String line;
	        while ((line = reader.readLine()) != null) {
	        	if (line.contains("+switch-master")) {
	               	String[] token = line.split("\\s+");
	               	leaderID = Integer.valueOf(token[5].trim());
	               	System.out.println("  leader id=" + leaderID);
	            }
	        }
	        reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
        return leaderID;*/
	}

	@Override
	public void bootstrapClients() {
		System.out.println("bootstrapClients");
		for (int i = 0; i < numNode; i++) {
			redisClients[i] = RedisClient.create("redis://localhost:"  + (basePort + i));
			connections[i] = redisClients[i].connect();
			commandsArray[i] = connections[i].sync();
		}
	}

	@Override
	public void takedownClients() {
		for (int i = 0; i < numNode; i++) {
			connections[i].close();
			redisClients[i].shutdown();
			commandsArray[i] = null;
			connections[i] = null;
			redisClients[i] = null;
		}
	}

	private void bootstrapClient(int nodeID) {
		System.out.println("bootstrapClient for nodeID=" + nodeID);
		redisClients[nodeID] = RedisClient.create("redis://localhost:"  + (basePort + nodeID));
		connections[nodeID] = redisClients[nodeID].connect();
		commandsArray[nodeID] = connections[nodeID].sync();
	}
	
	private void bootstrapClientWithPort(int nodeID, int port) {
		System.out.println("bootstrapClient for nodeID=" + nodeID);
		redisClients[nodeID] = RedisClient.create("redis://localhost:"  + port);
		connections[nodeID] = redisClients[nodeID].connect();
		commandsArray[nodeID] = connections[nodeID].sync();
	}

	
	private void takedownClient(int nodeID) {
		connections[nodeID].close();
		redisClients[nodeID].shutdown();
		commandsArray[nodeID] = null;
		connections[nodeID] = null;
		redisClients[nodeID] = null;
	}
	
	private int getPid(int nodeID) {
		int returnPid = -1;
		int waitCnt = 0;
		// wait until the pid file for the process associated with the given nodeID to be created
        while (true) {
        	// run ps command to see if the redis process is still running
			ProcessBuilder psBuilder = new ProcessBuilder();
			String[] psCmd = { "/bin/sh", "-c", "ps aux | grep redis-server | grep -v grep | grep -v ps | grep \"redis-server 127.0.0.1:%d\" | awk '{print $2}'" };
			psCmd[2] = String.format(psCmd[2], basePort + nodeID);
			Process psProc = null;
			try {
				System.out.println("Invoking cmd=\n" + String.join(" ", psCmd));
				psProc = psBuilder.command(psCmd).start();
				psProc.waitFor();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// gathered the output from the ps command ran above
			try {
				BufferedReader reader = new BufferedReader(new InputStreamReader(psProc.getInputStream()));
				StringBuilder builder = new StringBuilder();
				String line = null;
				while ((line = reader.readLine()) != null) {
					builder.append(line);
					//builder.append(System.getProperty("line.separator"));
				}
				if (builder.toString().isEmpty()) {
					System.out.println("Redis Server Process for node=" + nodeID + " is not yet started");
					Thread.sleep(100);
				} else {
					String pidResult = builder.toString();
					returnPid = Integer.parseInt(pidResult);
					System.out.println("pid of the Redis Server started for node=" + nodeID + " is=" + returnPid);
					break;
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	/*
	        File logDirFile = new File(workingDir + "/redis_dir/" + nodeID + "/redis.pid");
			if (logDirFile.exists()) {
				System.out.println("ok, pid file for nodeID=" + nodeID 
						+ " is created. Now we read pid from the redis.pid file!");
				InputStream is;
				try {
					is = new FileInputStream(logDirFile);
					BufferedReader buf = new BufferedReader(new InputStreamReader(is));
					String line = buf.readLine();
					StringBuilder sb = new StringBuilder();
					while(line != null){
					   sb.append(line).append("\n");
					   line = buf.readLine();
					}
					String fileAsString = sb.toString();
					System.out.println("Contents : " + fileAsString);
					returnPid = Integer.parseInt(fileAsString.trim());
					buf.close();
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}   
				break;
			} else {
				try {
					Thread.sleep(300);
					waitCnt++;
					if (waitCnt > 10) {
						break;
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			*/
        }
		return returnPid;
	}
	
	/*
	private void startSentinels() {
		System.out.println("startSentinels");
		ProcessBuilder builder = new ProcessBuilder();
		builder.directory(new File(workingDir));
		for (int i = 0; i < numNode; ++i) {
			// start a sentinel now
			String[] redisCmd = {"%s/start-sentinel.sh", "%s", "%s"};
			redisCmd[0] = String.format(redisCmd[0], deployScriptDir);
			redisCmd[1] = String.format(redisCmd[1], sutSrcDir);
			redisCmd[2] = String.format(redisCmd[2], workingDir + "/redis_dir/" + i + "/sentinel.conf");
			System.out.println("Starting sentinel for node " + i);
	        try {
	        	System.out.println("Invoking command=\n" + String.join(" ", redisCmd));
				nodeProc[i] = builder.command(redisCmd).start();
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
	        //int newPid = getPid(i);
	        //sentinelPidMap[i] = newPid;
		}
		int counter = countRedisSentinelProcesses();
		while (counter != numNode) {
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			counter = countRedisSentinelProcesses();
		}
	}
	*/	

	/*
	private int countRedisSentinelProcesses() {
		int processCounter = 0;
		ProcessBuilder psBuilder = new ProcessBuilder();
		//String[] psCmd = { "ps", "aux", "|", "grep", "mongo" };
		//String[] psCmd = { "ps", "aux" };
		String[] psCmd = {"/bin/sh", "-c", "ps aux | grep redis-sentinel | grep -v grep | grep -v ps"};
		Process process = null;
		try {
			process = psBuilder.command(psCmd).start();
			process.waitFor();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		BufferedReader reader = 
                new BufferedReader(new InputStreamReader(process.getInputStream()));
		StringBuilder builder = new StringBuilder();
		String line = null;
		try {
			while ((line = reader.readLine()) != null) {
				builder.append(line);
				builder.append(System.getProperty("line.separator"));
				if (line.contains("redis-sentinel")) {
					processCounter++;
				}
			}
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//String result = builder.toString();
		//System.out.println("initializeReplicaSet() result String=\n" + result);
		return processCounter;
	}
	*/
	
	protected void setupInitConcreteState() {
		for (int i = 0; i < numNode; i++) {
			int tssc = -1;
			if (i > 0) {
				RedisClient rcli = RedisClient.create("redis://localhost:"  + (basePort + i));
				StatefulRedisConnection<String, String> rconn = rcli.connect();
				RedisCommands<String, String> rcmdary = rconn.sync();
				tssc = i - 1;
				System.out.println("node" + i + " sync from node" + tssc);
				rcmdary.slaveof("localhost", basePort + tssc);
				syncSources[i] = tssc;
			}
		}
		System.out.println("initial sync sources=" + Arrays.toString(syncSources));
	}
	
	@Override
	public void startEnsemble() {
		System.out.println("startEnsemble");
		ProcessBuilder builder = new ProcessBuilder();
		builder.directory(new File(workingDir));
		for (int i = 0; i < numNode; ++i) {
			//String[] rmLogCmd = { "rm", "-f", workingDir + "/redis_dir/" + i + "/" + serverLogFileName};
			String[] rmLogCmd = { "rm", "-rf", workingDir + "/redis_dir/" + i + "/dump.rdb"
					  , workingDir + "/redis_dir/" + i + "/appendonly.aof"
					  , workingDir + "/redis_dir/" + i + "/" + serverLogFileName };
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
			String[] redisCmd = {"%s/start-redis.sh", "%s", "%s"};
			redisCmd[0] = String.format(redisCmd[0], deployScriptDir);
			redisCmd[1] = String.format(redisCmd[1], sutSrcDir);
			redisCmd[2] = String.format(redisCmd[2], workingDir + "/redis_dir/" + i + "/redis.conf");
			System.out.println("Starting node " + i);
	        try {
	        	System.out.println("Invoking command=\n" + String.join(" ", redisCmd));
				nodeProc[i] = builder.command(redisCmd).start();
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
	        int newPid = getPid(i);
	        if (newPid <= 0) {
	        	System.err.println("ASSERT: new redis-server process must have a pid greater than 0");
	        	System.exit(1);
	        }
	        pidMap[i] = newPid;
		}
		int counter = countRedisServerProcesses();
		while (counter != numNode) {
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			counter = countRedisServerProcesses();
		}
		
		// setup init state
		setupInitConcreteState();
		
		Arrays.fill(isNodeOnline, true);
		actualLeader = findLeader();
		
		/*
		int[] participatingNodes = new int[numNode];
		for (int i = 0; i < numNode; i++) {
			participatingNodes[i] = i;
		}
		actualLeader = electRedisMaster(participatingNodes);

		// update the role list and setup replication settings 
		// by appropriately using the slaveof command
		for (int i = 0; i < numNode; i++) {
			if (i == actualLeader) {
				nodeRoles[i] = MASTER_ROLE;
				// use redis slaveof no one here
				// everyone is slaveof no one initially isn't it?
				// but use redis slaveof no one anyways just to be sure
				RedisClient rcli = RedisClient.create("redis://localhost:"  + (basePort + i));
				StatefulRedisConnection<String, String> rconn = rcli.connect();
				RedisCommands<String, String> rcmdary = rconn.sync();
				rcmdary.slaveofNoOne();
			} else {
				nodeRoles[i] = SLAVE_ROLE;
				// use redis slaveof <hostname> <port> here
				// <hostname> is 127.0.0.1, localhost
				// <port> should be retrieved from redis.pid of actualLeader
				RedisClient rcli = RedisClient.create("redis://localhost:"  + (basePort + i));
				StatefulRedisConnection<String, String> rconn = rcli.connect();
				RedisCommands<String, String> rcmdary = rconn.sync();
				rcmdary.slaveof("localhost", basePort + actualLeader);
			}
		}		

		try {
			// wait for 1 sec in order to give a time for initial full resync
			// to be completed after making nodes to be slaves
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
		
		/*
		int leaderID = -1;
		leaderID = findLeader();
		while (leaderID < 0) {
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			leaderID = findLeader();
		}
		actualLeader = leaderID;
		*/
	}
	
	/*
	private int electRedisMaster(int[] participatingNodes) {
		int newMasterID = -1;
		
		// find the highestEpochCnt first (only among online nodes though)
		int highestEpochCnt = -1;
		for (int i = 0; i < participatingNodes.length; i++) {
			if (highestEpochCnt < epochCnts[participatingNodes[i]]) {
				// update the highestEpochCnt
				highestEpochCnt = epochCnts[participatingNodes[i]]; 
			}
		}
		System.out.println("highestEpochCnt=" + highestEpochCnt);
		
		// check if there are more than one nodes with the highestEpochCnt
		int numHighestEpochNode = 0;
		for (int i = 0; i < participatingNodes.length; i++) {
			if (epochCnts[participatingNodes[i]] == highestEpochCnt) {
				System.out.println("node=" + participatingNodes[i] 
						 + " has highest epoch cnt");
				numHighestEpochNode++;
			}
		}
		System.out.println("numHighestEpochNode=" + numHighestEpochNode);
		
		if (numHighestEpochNode > 1) {
			// need to compare txn counts
			System.out.println("need to compare txn counts");
			
			// find nodeIDs to compare txn counts
			int[] highestEpochNodes = new int[numHighestEpochNode];
			int indexHEN = 0;
			for (int i = 0; i< participatingNodes.length; i++) {
				if (epochCnts[participatingNodes[i]] == highestEpochCnt) {
					highestEpochNodes[indexHEN++] = participatingNodes[i];
					System.out.println("node=" + participatingNodes[i] + " has the highest epoch cnt");
				}
			}
			
			// find the highestTxnCnt first
			int highestTxnCnt = -1;
			for (int i = 0; i < highestEpochNodes.length; i++) {
				if (highestTxnCnt < txnCnts[highestEpochNodes[i]]) {
					// update the highestEpochCnt
					highestTxnCnt = txnCnts[highestEpochNodes[i]]; 
				}
			}
			System.out.println("highestTxnCnt=" + highestTxnCnt);
			
			// check if there are more than one nodes with the highestTxnCnt
			int numHighestTxnNode = 0;
			for (int i = 0; i < highestEpochNodes.length; i++) {
				if (txnCnts[highestEpochNodes[i]] == highestTxnCnt) {
					numHighestTxnNode++;
					System.out.println("node=" + highestEpochNodes[i] + " has the highest txn cnt");
				}
			}
			
			// now we elect the first node (which is either the only one 
			// with the highest epoch and txn or the one with the lowest
			// nodeID)
			for (int i = 0; i < highestEpochNodes.length; i++) {
				if (txnCnts[highestEpochNodes[i]] == highestTxnCnt) {
					newMasterID = highestEpochNodes[i];
					break;
				}
			}			
		} else if (numHighestEpochNode == 1) {
			System.out.println("we elect the node with the highest epoch");
			
			// we elect the node with the highest epoch
			for (int i = 0; i < participatingNodes.length; i++) {
				if (epochCnts[participatingNodes[i]] == highestEpochCnt) {
					newMasterID = participatingNodes[i];
					break;
				}
			}
		} else {
			// doesn't make sense
			System.err.println("Wierd Error: highest epoch node count is 0");
			System.exit(1);
		}
		
		// increase the epoch counts of both participatingNodes
		// to be the highestEpochCnt plus 1 and transaction counts to 0
		for (int i = 0; i < participatingNodes.length; i++) {
			epochCnts[participatingNodes[i]] = highestEpochCnt + 1;
			txnCnts[participatingNodes[i]] = 0;
		}
		
		System.out.println("newMasterID=" + newMasterID);
		
		return newMasterID;
	}
	*/

	private int countRedisServerProcesses() {
		int processCounter = 0;
		ProcessBuilder psBuilder = new ProcessBuilder();
		//String[] psCmd = { "ps", "aux", "|", "grep", "mongo" };
		//String[] psCmd = { "ps", "aux" };
		String[] psCmd = {"/bin/sh", "-c", "ps aux | grep redis-server | grep -v grep | grep -v ps"};
		Process process = null;
		try {
			process = psBuilder.command(psCmd).start();
			process.waitFor();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		BufferedReader reader = 
                new BufferedReader(new InputStreamReader(process.getInputStream()));
		StringBuilder builder = new StringBuilder();
		String line = null;
		try {
			while ((line = reader.readLine()) != null) {
				builder.append(line);
				builder.append(System.getProperty("line.separator"));
				if (line.contains("redis-server")) {
					processCounter++;
				}
			}
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//String result = builder.toString();
		//System.out.println("initializeReplicaSet() result String=\n" + result);
		return processCounter;
	}

	@Override
	public void stopEnsemble() {
		System.out.println("stopEnsemble");
		for (Process node : nodeProc) {
            node.destroy();
        }
		for (int i = 0; i < numNode; i++) {
        	nodeProc[i] = null;
        }
        System.gc();
        // explicitly kill each process
     	for (int i = 0; i < numNode; i++) {
     		killNode(i);
     	}
     	Arrays.fill(isNodeOnline, false);
	}

	// explicitly kill the process associated with the given node id
	private void killNode(int nodeID) {
		int pid = pidMap[nodeID];
		while(true) {
			ProcessBuilder killBuilder = new ProcessBuilder();
			String[] killCmd = {"/bin/sh", "-c", "kill -9 %d" };
			killCmd[2] = String.format(killCmd[2], pid);
			Process killProc = null;
			try {
				System.out.println("Invoking cmd=\n" + String.join(" ", killCmd));
				killProc = killBuilder.command(killCmd).start();
				killProc.waitFor();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			// run ps command to see if the redis process is still running
			ProcessBuilder psBuilder = new ProcessBuilder();
			String[] psCmd = { "/bin/sh", "-c", "ps aux | grep redis-server | grep -v grep | grep -v ps | awk '{print $2}' | grep \" %d \"" };
			psCmd[2] = String.format(psCmd[2], pid);
			Process psProc = null;
			try {
				System.out.println("Invoking cmd=\n" + String.join(" ", psCmd));
				psProc = psBuilder.command(psCmd).start();
				psProc.waitFor();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// gathered the output from the ps command ran above
			try {
				BufferedReader reader = new BufferedReader(new InputStreamReader(psProc.getInputStream()));
				StringBuilder builder = new StringBuilder();
				String line = null;
				while ((line = reader.readLine()) != null) {
					builder.append(line);
					builder.append(System.getProperty("line.separator"));
				}
				if (builder.toString().isEmpty()) {
					System.out.println("Verified that we succeeded to completely kill the given pid process");
					reader.close();
					break;
				} else {
					String result = builder.toString();
					System.out.println("killNode() result String=\n" + result);
					Thread.sleep(1000);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public void startNode(int id) {
		System.out.println("startNode " + id);
		ProcessBuilder builder = new ProcessBuilder();
		builder.directory(new File(workingDir));
		
		/*
		String[] rmLogCmd = { "rm", "-f", workingDir + "/redis_dir/" + id + "/" + serverLogFileName
										, workingDir + "/redis_dir/" + id + "/redis.pid"};
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
		*/
		
		// start the node now
		String[] startCmd = {"/bin/sh", "-c", "kill -CONT %d"};
		int pid = pidMap[id];	
		startCmd[2] = String.format(startCmd[2], pid);
		System.out.println("Starting node " + id);
        try {
        	System.out.println("Invoking command=\n" + String.join(" ", startCmd));
			Process proc = builder.command(startCmd).start();
			proc.waitFor();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(1);
		}
        System.out.println("invoking command succeeded");
        isNodeOnline[id] = true;
        // additionally set online for the node that is linked with the restarted node
        for (int i = 0; i < numNode; i++) {
			ArrayList<Integer> chain = new ArrayList<Integer>();
			int currID = i;
			boolean containResNode = false;
			while (true) {
				chain.add(currID);
				if (!containResNode) {
					if (currID == id) {
						containResNode = true;
					}
				}
				if (syncSources[currID] == -1) {
					break;
				} else {
					currID = syncSources[currID];
				}
			}
			if (containResNode) {
				for (int nodeToBeOnline : chain) {
					isNodeOnline[nodeToBeOnline] = true;
				}
			}
		}
        
        
    	/*
		int waitCnt = 0;
		while (true) {
			// start the node now
			String[] redisCmd = {"%s/start-redis.sh", "%s", "%s"};
			redisCmd[0] = String.format(redisCmd[0], deployScriptDir);
			redisCmd[1] = String.format(redisCmd[1], sutSrcDir);
			redisCmd[2] = String.format(redisCmd[2], workingDir + "/redis_dir/" + id + "/redis.conf");
			System.out.println("Starting node " + id);
	        try {
	        	System.out.println("Invoking command=\n" + String.join(" ", redisCmd));
				nodeProc[id] = builder.command(redisCmd).start();
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
	        System.out.println("invoking command succeeded");
	        isNodeOnline[id] = true;
			int newPid = getPid(id);
			if (newPid < 0) {
				waitCnt++;
				if (waitCnt > 10) {
					System.err.println("Cannot successfully start node for some reason");
					System.exit(1);
				}
				continue;
			} else {
				System.out.println("getpid succeeded");
				pidMap[id] = newPid;
				break;
			}
		}
		*/
		
	}

	@Override
	public void stopNode(int id) {
		System.out.println("stopNode " + id);
		//nodeProc[id].destroy();

		while (true) {
			try {
				if (syncSources[id] != -1) {
					//System.out.println("cachedTargetState=" + Arrays.toString(cachedTargetState));
					//System.out.println("syncSources[id]=" + syncSources[id]);
					//System.out.println("cachedTargetState[syncSources[id]]=" + cachedTargetState[syncSources[id]]);
					//System.out.println("cachedTargetState[id]=" + cachedTargetState[id]);
					
					if (cachedTargetState[syncSources[id]] > cachedTargetState[id]) {
						commandsArray[id].slaveofNoOne();
						syncSources[id] = -1;
						System.out.println("id=" + id + " slaveof no one");
					} else {
						System.out.println("id=" + id + " is still slaveof " + syncSources[id]);
					}
				} else {
					System.out.println("id=" + id + " is a primary and already slaveof no one.");
				}
				break;
			} catch (RedisCommandExecutionException rcee) {
				System.err.println("RedisCommandExecutionException caught in stopNode for id=" + id);
				takedownClient(id);
				bootstrapClient(id);
			}	
		}
		
		
		// stop the node now
		/*
		ProcessBuilder builder = new ProcessBuilder();
		builder.directory(new File(workingDir));
		String[] stopCmd = {"/bin/sh", "-c", "kill -STOP %d"};
		int pid = pidMap[id];	
		stopCmd[2] = String.format(stopCmd[2], pid);
		System.out.println("Stopping node " + id);
        try {
        	System.out.println("Invoking command=\n" + String.join(" ", stopCmd));
			Process proc = builder.command(stopCmd).start();
			proc.waitFor();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(1);
		}
		*/
		
        System.out.println("invoking command succeeded");
        System.out.println("setting node offline for id=" + id);
		isNodeOnline[id] = false;
		//killNode(id);
		
		try {
			System.out.println("injecting sleep for duration given=" + injectedDelayDuration);
			Thread.sleep(injectedDelayDuration);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	/*
	private void shutdownSentinels() {
		ProcessBuilder killBuilder = new ProcessBuilder();
		String[] killCmd = {"/bin/sh", "-c", "kill -9 $(ps aux | grep 'redis-sentinel ' | grep -v grep | awk '{print $2}')" };
		Process killProc = null;
		try {
			System.out.println("Invoking cmd=\n" + String.join(" ", killCmd));
			killProc = killBuilder.command(killCmd).start();
			killProc.waitFor();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		while (true) {
			// run ps command to see if the sentinel process is still running
			ProcessBuilder psBuilder = new ProcessBuilder();
			String[] psCmd = { "/bin/sh", "-c", "ps aux | grep redis-sentinel | grep -v grep | grep -v ps" };
			Process psProc = null;
			try {
				System.out.println("Invoking cmd=\n" + String.join(" ", psCmd));
				psProc = psBuilder.command(psCmd).start();
				psProc.waitFor();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// gathered the output from the ps command ran above
			try {
				BufferedReader reader = new BufferedReader(new InputStreamReader(psProc.getInputStream()));
				StringBuilder builder = new StringBuilder();
				String line = null;
				while ((line = reader.readLine()) != null) {
					builder.append(line);
					builder.append(System.getProperty("line.separator"));
				}
				if (builder.toString().isEmpty()) {
					System.out.println("Verified that we succeeded to completely kill the given pid process");
					reader.close();
					break;
				} else {
					String result = builder.toString();
					System.out.println("attempting to kill sentinels result in the output String=\n" + result);
					Thread.sleep(1000);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	*/
	
	@Override
	public void resetTest() {
		System.out.println("resetTest");
		
		// shutdown sentinels
		//shutdownSentinels();
		
		if (TestingEngine.programMode) {
			System.out.println("Program Mode is enabled. We don't need to reset anything here.");
			return;
		}

		// reset counters and node roles for the next schedule
		Arrays.fill(epochCnts, 0);
		//Arrays.fill(txnCnts, 0);
		Arrays.fill(nodeRoles, "none");
		Arrays.fill(syncSources, -1);
		
		// delete data directory
        ProcessBuilder builder = new ProcessBuilder();
        builder.directory(new File(workingDir));
        for (int i = 0; i < numNode; i++) {
        	//String[] rmDataCmd = { "rm", "-rf", workingDir + "/redis_dir/" + i + "/dump.rdb"
	        //								  , workingDir + "/redis_dir/" + i + "/appendonly.aof"
	        //								  , workingDir + "/redis_dir/" + i + serverLogFileName
	        //								  , workingDir + "/redis_dir/" + i + sentinelLogFileName};
        	String[] rmDataCmd = { "rm", "-rf", workingDir + "/redis_dir/" + i + "/dump.rdb"
					  , workingDir + "/redis_dir/" + i + "/appendonly.aof"
					  , workingDir + "/redis_dir/" + i + "/" + serverLogFileName };
        	try {
	            Process p = builder.command(rmDataCmd).start();
	            p.waitFor();
	            //Thread.sleep(31000);
	        } catch (IOException e) {
	            e.printStackTrace();
	            System.exit(1);
	        } catch (InterruptedException e) {
				e.printStackTrace();
				System.exit(1);
			}
        }
	}

	@Override
	public byte[] readData(int nodeID, String key) {
		byte[] bytesToReturn = null;
		int retryCnt = 0;
		while (true) {
			try {
				String value = commandsArray[nodeID].get(key);
				if (value == null) {
					retryCnt++;
					if (retryCnt > 100) {
						System.err.println("Retry failed... Don't know what is going on. return null str");
						String retNullStr = "null";
						return retNullStr.getBytes();
					} else {
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					continue;
				}
				bytesToReturn = value.getBytes();
				break;
			} catch (RedisCommandExecutionException rcee) {
				System.err.println("RedisCommandExecutionException caught in readData");
				takedownClient(nodeID);
				bootstrapClient(nodeID);
			}
			retryCnt++;
			if (retryCnt > 100) {
				System.err.println("Retry failed... Don't know what is going on. return null str");
				String retNullStr = "null";
				return retNullStr.getBytes();
			} else {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return bytesToReturn;
	}

	@Override
	public void createData(int nodeID, String key, int value) {
		while (true) {
			int primaryID = -1;
			try {
				//for (int i = 0; i < numNode; i++) {
					//if (syncSources[i] != -1) {
					//	System.err.println("ASSERT: we should use this createData only during the beginning");
					//	System.exit(1);
					//}
					//commandsArray[i].set(key, "" + value);
				//}
				primaryID = findLeader();
				if (primaryID == -1) {
					System.err.println("Assert: in createData. should be able to find the leader by now");
					System.exit(1);
				}
				System.out.println("primaryID=" + primaryID + " key=" + key + " value=" + value);
				commandsArray[primaryID].set(key, "" + value);
				break;
			} catch (RedisCommandExecutionException rcee) {
				System.err.println("RedisCommandExecutionException caught in createData");
				takedownClient(primaryID);
				bootstrapClient(primaryID);
			}
		}
		//for (int i = 0; i < numNode; i++) {
			//txnCnts[i] = txnCnts[i] + 1;
			//txnCnts[nodeID] = txnCnts[nodeID] + 1;
		//}
		try {
			Thread.sleep(waitForCommitDuration);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void writeData(int nodeID, String key, int value) {
		String paddingCnt = "";
		for (int i = 0; i < 1000; i++) {
			paddingCnt += "0000000000";
		}
		int primaryID = -1;
		primaryID = findLeader();
		while (true) {
			try {
				for (int i = 0; i < 10; i++) {
					String valueStr = value + "." + paddingCnt + "." + i;
					commandsArray[primaryID].set(key, valueStr);
				}
				//System.out.println("in writeData. primaryID=" + primaryID);
				//System.out.println("setting key=" + key + " value=" + value);
				//commandsArray[primaryID].set(key, "" + value);
				break;
			} catch (RedisCommandExecutionException rcee) {
				System.err.println("RedisCommandExecutionException caught in writeData");
				takedownClient(primaryID);
				bootstrapClient(primaryID);
			}	
		}
		
		//txnCnts[nodeID] = txnCnts[nodeID] + 1;
	}

	int[] cachedTargetState;
	
	@Override
	public int[] sortTargetState(int[] targetState) {
		
		System.out.println("sortTargetState=" + Arrays.toString(targetState));
		System.out.println("  Note: node online status=" + Arrays.toString(isNodeOnline));
		
		//Just do sanity checking:
		// 1. only online nodes should have a non-negative integer assigned
		// 2. while traversing each replication chain starting with each non-negative integer 
		//    in syncSources should see target state change value in increasing order
		// 3. only leader or offline nodes should have -1 in targetState 
		for (int i = 0; i < numNode; i++) {
			if (targetState[i] > 0) {
				if (!isNodeOnline[i]) {
					System.err.println("Assert: only online nodes should have a non-negative integer assigned");
					System.exit(1);
				}
			}
		}
		for (int i = 0; i < numNode; i++) {
			int curTscVal = targetState[i];
			int curParent = syncSources[i];
			while (curParent != -1) {
				int parentTscVal = targetState[curParent];
				if (curTscVal > parentTscVal) {
					System.err.println("Assert: while traversing each replication chain starting with each "
							+ "non-negative integer in syncSources should see target state change value in "
							+ "increasing order");
					System.err.println("current node=" + i + " curTscVal=" + curTscVal + " parent node=" + curParent
							+ " parentTscVal=" + parentTscVal);
					System.exit(1);
				}
				curTscVal = parentTscVal;
				curParent = syncSources[curParent];
			}
		}
		int primaryID = findLeader(); 
		for (int i = 0; i < numNode; i++) {
			if (targetState[i] < 0) {
				if (primaryID != i && isNodeOnline[i]) {
					System.err.println("Assert: only leader or offline nodes should have -1 in targetState");
				}
			}
		}
		
		cachedTargetState = targetState;
		
		return targetState;
		
		/*
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
		actualLeader = findLeader();
		System.out.println("actualLeader=" + actualLeader + " guessedLeader=" + guessedLeader);
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
		*/
	}

	@Override
	protected boolean waitForCommit(ArrayList<Integer> commitNodes, String key, int val) {
		System.out.println("waitForCommit nodes=" + Arrays.deepToString(commitNodes.toArray()) + " key=" + key + " val=" + val);
		
		//try {	Thread.sleep(6000000); } catch (InterruptedException e) { e.printStackTrace(); }
		
		if (cwmMode.equals(CommitWaitMode.CWM_SLEEP)) {
			try {
				Thread.sleep(waitForCommitDuration);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} else if (cwmMode.equals(CommitWaitMode.CWM_PROBE)) {
			boolean timeoutFlag = false;
			for (int i = 0; i < commitNodes.size(); i++) {
				boolean foundMessage = false;
				foundMessage = findCommitMessage(commitNodes.get(i), key, "" + val);
				int timeoutCnt = 0;
				while (!foundMessage) {
					try {
						Thread.sleep(100);
						timeoutCnt++;
						if (timeoutCnt == 600) {
							timeoutFlag = true;
							break;
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					foundMessage = findCommitMessage(commitNodes.get(i), key, "" + val);
				}
				if (timeoutFlag) {
					break;
				}
			}
		} else {
			System.err.println("ERROR: unknown commit wait mode");
		}
		return true;
	}
	
	private boolean findCommitMessage(int nodeID, String key, String value) {
		System.out.println("findCommitMessage");
		System.out.println("nodeID=" + nodeID + " key=" + key + " value=" + value);
		try  {
			String logFileName = workingDir + "/redis_dir/" + nodeID + "/appendonly.aof";
			BufferedReader reader = new BufferedReader(new FileReader(logFileName));
            String line;
            while ((line = reader.readLine()) != null) {
            	System.out.println("line=" + line);
            	if (line.contains("" + value)) {
            		reader.close();
            		return true;
            	}
            }
            reader.close();
		} catch (FileNotFoundException e) {
			
        } catch (IOException e) {
        	
        }
		return false;
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
		} else if (rwmMode.equals(ResyncWaitMode.RWM_PROBE)) {
			
			boolean[] isSynced = new boolean[numNode];
			Arrays.fill(isSynced, false);
			//for (int reID : restartNodeIDs) {
			for (int reID = 0; reID < numNode; reID++) {
				System.out.println("wait for resync for node" + reID);
				System.out.println("isSynced for the node" + reID + "=" + isSynced[reID]);
				if (!isNodeOnline[reID]) {
					System.out.println("node" + reID + " is NOT online");
				} else if (isSynced[reID]) {
					System.out.println("node" + reID + " is online");
					System.out.println("node" + reID + " is already synced");
				} else if (syncSources[reID] == -1) {
					System.out.println("node" + reID + " is online");
					System.out.println("node" + reID + " is NOT yet synced");
					System.out.println("node" + reID + " is the root of the chain. Set it as synced");
					isSynced[reID] = true;
				} else if (!isNodeOnline[syncSources[reID]]) {
					System.out.println("node" + reID + " is online");
					System.out.println("node" + reID + " is NOT yet synced");
					System.out.println("node" + reID + " is NOT the root of the chain");
					System.out.println("node" + reID + " has sync source that is not online. Treat it as synced");
					isSynced[reID] = true;
				} else {
					System.out.println("node" + reID + " is online");
					System.out.println("node" + reID + " is NOT yet synced");
					System.out.println("node" + reID + " is NOT the root of the chain");
					System.out.println("node" + reID + " has sync source that is online");
					System.out.println("Find the closest online and out-of-sync ancester of the node " 
							+ reID + " in the chain");
					
					ArrayList<Integer> currentChain = new ArrayList<Integer>();
					int currentNode = reID;
					while(true) {
						currentChain.add(0, currentNode);
						currentNode = syncSources[currentNode];
						if (isSynced[currentNode] || syncSources[currentNode] == -1 
								|| !isNodeOnline[syncSources[currentNode]]) {
							if (!isSynced[currentNode]) {
								isSynced[currentNode] = true;
							}
							break;
						}
					}
					System.out.println("currentChain=" + Arrays.toString(currentChain.toArray()));
					for (int node : currentChain) {
						System.out.println("node" + node + " has sync source=" + syncSources[node]);
						int i = 0;
						while (true) {
							System.out.println("---- syncing wait " + (i++) + "-th times ----");
							String infoResult = "";
							while (true) {
								try {
									infoResult = commandsArray[node].info();
									break;
								} catch (RedisCommandExecutionException rcee) {
									System.err.println("RedisCommandExecutionException caught in info");
									takedownClient(node);
									bootstrapClient(node);
								} catch (RedisCommandTimeoutException rcte) {
									System.err.println("RedisCommandTimeoutException caught in info");
									takedownClient(node);
									bootstrapClient(node);
								} catch (RedisException re) {
									System.err.println("RedisException re");
									takedownClient(node);
									bootstrapClient(node);
								}
							}
							String[] infoTokens = infoResult.split("\\s+");
							boolean syncIsConfirmed = true;
							for (int k = 0; k < infoTokens.length; k++) {
								//if (infoTokens[k].contains("master_sync_in_progress")) {
									//System.out.println("master_sync_in_progress=" + infoTokens[k]);
									//int inProgValue = Integer.parseInt(infoTokens[k].split(":")[1]);
									//if (inProgValue == 0) {
										//isSynced[j] = true;
									//} else {
										//isSynced[j] = false;
									//}
								//}

								if (infoTokens[k].contains("master_link_status")) {	
									System.out.println("master_link_status=" + infoTokens[k]);
									String mlsStr = infoTokens[k].split(":")[1];
									if (mlsStr.equals("up")) {
										syncIsConfirmed &= true;
									} else {
										syncIsConfirmed &= false;
									}
								}
								if (infoTokens[k].contains("master_sync_in_progress")) {
									System.out.println("master_sync_in_progress=" + infoTokens[k]);
									String msipStr = infoTokens[k].split(":")[1];
									if (msipStr.equals("0")) {
										syncIsConfirmed &= true;
									} else {
										syncIsConfirmed &= false;
									}
								} 
								if (infoTokens[k].contains("aof_rewrite_in_progress")) {
									System.out.println("aof_rewrite_in_progress=" + infoTokens[k]);
									String aripStr = infoTokens[k].split(":")[1];
									if (aripStr.equals("0")) {
										syncIsConfirmed &= true;
									} else {
										syncIsConfirmed &= false;
									}
								}
								if (infoTokens[k].contains("rgb_bgsave_in_progress")) {
									System.out.println("rgb_bgsave_in_progress=" + infoTokens[k]);
									String msipStr = infoTokens[k].split(":")[1];
									if (msipStr.equals("0")) {
										syncIsConfirmed &= true;
									} else {
										syncIsConfirmed &= false;
									}
								}
							}
							System.out.println("syncIsConfirmed=" + syncIsConfirmed);
							if (syncIsConfirmed) {
								isSynced[node] = true;
								break;
							} else {
								isSynced[node] = false;
								if (i > 20) {
									System.out.println("Retry Count is more than 20000. Give up waiting for resync to be finished..");
									break;
								} else {
									System.out.println("Resync is still not finished.. wait longer");
								}
								try {	Thread.sleep(1000); } catch (InterruptedException e) { e.printStackTrace(); }
							}
						}
						if (i > 200) {
							System.out.println("Retry Count is more than 20000. Give up waiting for resync to be finished..");
							break;
						}
					}
					
					try { 
						System.out.println("Wait additional 3 sec");
						Thread.sleep(3000); 
					} catch (InterruptedException e) { e.printStackTrace(); }
					
					System.out.println("currentChain " + Arrays.toString(currentChain.toArray())
						+ " is done with resync");
				}
			}
			
			/*
			if (leaderElectionNeeded) {
				System.out.println("leaderElectionNeeded true");
				actualLeader = electRedisMaster(restartNodeIDs);
				// update the role list and setup replication settings 
				// by appropriately using the slaveof command
				for (int i = 0; i < numNode; i++) {
					if (isNodeOnline[i]) {
						if (i == actualLeader) {
							System.out.println("actualLeader role is set to master");
							nodeRoles[i] = MASTER_ROLE;
							// use redis slaveof no one here
							// everyone is slaveof no one initially isn't it?
							// but use redis slaveof no one anyways just to be sure
							RedisClient rcli = RedisClient.create("redis://localhost:"  + (basePort + i));
							StatefulRedisConnection<String, String> rconn = rcli.connect();
							RedisCommands<String, String> rcmdary = rconn.sync();
							rcmdary.slaveofNoOne();
						} else {
							System.out.println("the node role is set to slave");
							nodeRoles[i] = SLAVE_ROLE;
							// use redis slaveof <hostname> <port> here
							// <hostname> is 127.0.0.1, localhost
							// <port> should be retrieved from redis.pid of actualLeader
							RedisClient rcli = RedisClient.create("redis://localhost:"  + (basePort + i));
							StatefulRedisConnection<String, String> rconn = rcli.connect();
							RedisCommands<String, String> rcmdary = rconn.sync();
							rcmdary.slaveof("localhost", basePort + actualLeader);
						}
					}
				}
			} else {
				System.out.println("leaderElectionNeeded false");
				
				// before calling findLeader, set all restarted nodes' role
				// to slave because we already have a master to sync with
				for (int i = 0; i < restartNodeIDs.length; i++) {
					nodeRoles[restartNodeIDs[i]] = SLAVE_ROLE;
				}
				
				// just make sure restarted node(s) will sync from the new leader
				int syncSource = findLeader();
				for (int i = 0; i < restartNodeIDs.length; i++) {
					System.out.println("node i=" + restartNodeIDs[i] + " is set to slave");
					System.out.println("node i=" + restartNodeIDs[i] + " is syncing from master=" + syncSource);
					
					int restartedSlave = restartNodeIDs[i];
					nodeRoles[restartedSlave] = SLAVE_ROLE;
					RedisClient rcli = RedisClient.create("redis://localhost:"  + (basePort + restartedSlave));
					StatefulRedisConnection<String, String> rconn = rcli.connect();
					RedisCommands<String, String> rcmdary = rconn.sync();
					System.out.println("syncSource=" + syncSource);
					rcmdary.slaveof("localhost", basePort + syncSource);
					
					// although it may be better to set epoch and transaction
					// counters after resync is done, let's do it here
					// for now
					epochCnts[restartNodeIDs[i]] = epochCnts[syncSource];
					txnCnts[restartNodeIDs[i]] = txnCnts[syncSource];
				}
			}
			*/
			
			/*
			boolean quorumExist = false;
			boolean restartingOldMaster = false;
			boolean manualFailoverRequired = false;
			
			// check if the node of rememberedOldMasterID is online
			// -- i.e. the quorum already exists
			if (isNodeOnline[rememberedOldMasterID]) {
				System.out.println("node rememberedOldMasterID=" 
						+ rememberedOldMasterID + " is currently online"
						+ " - quorum seems existing already");
				quorumExist = true;
			}
			// additionally, check if we are restarting offline old master
			for (int id : restartNodeIDs) {
				if (rememberedOldMasterID == id) {
					System.out.println("node rememberedOldMasterID=" 
							+ rememberedOldMasterID + " is being restarted "
							+ "while there is no quorum"
							+ " - same master will be elected");
					restartingOldMaster = true;
					break;
				}
			}
			
			// check if we should do manual failover as well as an assertion check
			if (!quorumExist && !restartingOldMaster) {
				System.out.println("We need to do manual failover");
				manualFailoverRequired = true;
			} else {
				System.out.println("We do not need to do manual failover"
						+ " because quorumExist=" + quorumExist + "AND "
						+ " restartingOldMaster=" + restartingOldMaster);
				manualFailoverRequired = false;
			}
			
			// If we still have to do manual failover, that means
			// 1) we do NOT have quorum yet
			// AND
			// 2) we are NOT restarting the previous old master
			//
			// This will result in the situation where
			// only old slaves are restarted and no election occurs
			// because sentinels will have no good slave to failover itself
			if (manualFailoverRequired) {
				System.out.println("we are invoking manualFailover via sentinels");
				//manualFailover(restartNodeIDs);
			}
			*/

			/*
			// look for the id of the master 
			int leaderID = -1;
			leaderID = findLeader();
			while (leaderID < 0) {
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				leaderID = findLeader();
			}
			actualLeader = leaderID;
			int i = 0;
			while (true) {
				System.out.println("---- syncing wait " + (i++) + "-th times ----");
				String infoResult = "";
				boolean[] isSynced = new boolean[numNode];
				Arrays.fill(isSynced, false);
				
				// check if each node is resynced or not. skip master node. check slaves.
				for (int j = 0; j < numNode; j++) {
					System.out.println("check for sync completion for node=" + j);
					if (!isNodeOnline[j]) {
						System.out.println("node=" + j + " is offline");
						isSynced[j] = true;
						continue;
					}
					System.out.println("node=" + j + " is online");
					while (true) {
						try {
							infoResult = commandsArray[j].info();
							break;
						} catch (RedisCommandExecutionException rcee) {
							System.err.println("RedisCommandExecutionException caught in info");
							takedownClient(j);
							bootstrapClient(j);
						} catch (RedisCommandTimeoutException rcte) {
							System.err.println("RedisCommandTimeoutException caught in info");
							takedownClient(j);
							bootstrapClient(j);
						} catch (RedisException re) {
							System.err.println("RedisException re");
							takedownClient(j);
							bootstrapClient(j);
						}
					}
					//System.out.println("infoResult=" + infoResult);
					if (j == actualLeader) {
						System.out.println("node=" + j + " is actualLeader");
						isSynced[j] = true;
						continue;
					} else {
						System.out.println("node=" + j + " is slave");
						isSynced[j] = false;
					}		
					String[] infoTokens = infoResult.split("\\s+");
					boolean syncIsConfirmed = true;
					for (int k = 0; k < infoTokens.length; k++) {
						//if (infoTokens[k].contains("master_sync_in_progress")) {
							//System.out.println("master_sync_in_progress=" + infoTokens[k]);
							//int inProgValue = Integer.parseInt(infoTokens[k].split(":")[1]);
							//if (inProgValue == 0) {
								//isSynced[j] = true;
							//} else {
								//isSynced[j] = false;
							//}
						//}

						if (infoTokens[k].contains("master_link_status")) {	
							System.out.println("master_link_status=" + infoTokens[k]);
							String mlsStr = infoTokens[k].split(":")[1];
							if (mlsStr.equals("up")) {
								syncIsConfirmed &= true;
							} else {
								syncIsConfirmed &= false;
							}
						}
						if (infoTokens[k].contains("master_sync_in_progress")) {
							System.out.println("master_sync_in_progress=" + infoTokens[k]);
							String msipStr = infoTokens[k].split(":")[1];
							if (msipStr.equals("0")) {
								syncIsConfirmed &= true;
							} else {
								syncIsConfirmed &= false;
							}
						} 
						if (infoTokens[k].contains("aof_rewrite_in_progress")) {
							System.out.println("aof_rewrite_in_progress=" + infoTokens[k]);
							String aripStr = infoTokens[k].split(":")[1];
							if (aripStr.equals("0")) {
								syncIsConfirmed &= true;
							} else {
								syncIsConfirmed &= false;
							}
						}
						if (infoTokens[k].contains("rgb_bgsave_in_progress")) {
							System.out.println("rgb_bgsave_in_progress=" + infoTokens[k]);
							String msipStr = infoTokens[k].split(":")[1];
							if (msipStr.equals("0")) {
								syncIsConfirmed &= true;
							} else {
								syncIsConfirmed &= false;
							}
						}
					}
					System.out.println("syncIsConfirmed=" + syncIsConfirmed);
					if (syncIsConfirmed) {
						isSynced[j] = true;
					} else {
						isSynced[j] = false;
					}
				} 
				System.out.println("\n isSynced array=" + Arrays.toString(isSynced));
				// check if every slave is finished with resync with the master
				boolean resyncCheckResult = true;
				for (int j = 0; j < numNode; j++) {
					resyncCheckResult &= isSynced[j];
				}
				if (resyncCheckResult) {
					System.out.println("Verified participating slaves are resync'ed with the master");
					break;
				} else if (i > 600) {
					System.out.println("Retry Count is more than 6600. Give up waiting for resync to be finished..");
					break;
				} else {
					System.out.println("Resync is still not finished.. wait longer");
				}
				try {	Thread.sleep(100); } catch (InterruptedException e) { e.printStackTrace(); }
			}
			*/
		} else {
			System.err.println("ERROR: unknown resync wait mode");
		}
		
		
		return true;
	}

	/* manual failover:
	 * 1. select slave to promote
	 * 2. perform "slaveof no one" to the selected slave
	 * 3. continuously, perform "sentinel failover <name>"  
	 * 4. wait until the selected slave gets promoted to the new master
	 */
/*	private void manualFailover(int[] restartNodeIDs) {

		int newMasterID = selectSlaveToPromote(restartNodeIDs);
		
		// "slaveof no one"
		try {
			commandsArray[newMasterID].slaveofNoOne();
		} catch (RedisCommandExecutionException rcee) {
			System.err.println("RedisCommandExecutionException caught in manual failover");
			takedownClient(newMasterID);
			bootstrapClient(newMasterID);
		}
		
		// "sentinel failover <name>"
		sentinelManualFailover(clusterName);
		
		// wait the new master is elected
		int waitCnt = -1;
		while (true) {
			int newMaster = findLeader();
			if (newMaster == newMasterID) {
				break;
			} else {
				try {
					waitCnt++;
					if (waitCnt > 100) {
						System.err.println("ERROR we timeout on waiting for the "
								+ "new master to be elected after 10 sec");
						break;
					}
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
*/
	
	/*
	private void sentinelManualFailover(String clusterName2) {
		System.out.println("sentinelManualFailover");
		RedisClient redisCli = RedisClient.create("redis://localhost:"  + (20000 + basePort));
		StatefulRedisSentinelConnection<String, String> redisSentinelConn = redisCli.connectSentinel();
		RedisSentinelCommands<String, String> redisSentinalCmds = redisSentinelConn.sync();
		String resultStr = redisSentinalCmds.failover(clusterName);
		System.out.println("sentinel failover " + clusterName + " done: " + resultStr);
	}
	*/

	/*
	private int selectSlaveToPromote(int[] restartNodeIDs) {
		System.out.println("selectSlaveToPromote");
		RedisClient redisCli = RedisClient.create("redis://localhost:"  + (20000 + basePort));
		StatefulRedisSentinelConnection<String, String> redisSentinelConn = redisCli.connectSentinel();
		RedisSentinelCommands<String, String> redisSentinalCmds = redisSentinelConn.sync();
		List<Map<String, String>> slavesInfo = redisSentinalCmds.slaves(clusterName);
		int highestSlaveReplOffsetIndex = -1;
		int highestSlaveReplOffsetIntSoFar = -1;
		for (int i = 0; i < slavesInfo.size(); i++) {
			System.out.println(i + "-th slavesInfo=" + slavesInfo.get(i));
			String slaveReplOffset = slavesInfo.get(i).get("slave-repl-offset");
			int slaveReplOffsetInt = Integer.parseInt(slaveReplOffset);
			if (highestSlaveReplOffsetIntSoFar < slaveReplOffsetInt) {
				highestSlaveReplOffsetIndex = i;
				highestSlaveReplOffsetIntSoFar = slaveReplOffsetInt;
			}
		}
		int portNo = Integer.parseInt(slavesInfo.get(highestSlaveReplOffsetIndex).get("port"));
		int selectedSlaveID = portNo - basePort;
		System.out.println("node=" + selectedSlaveID 
				+ " is selected to be promoted with manual failover");
		redisSentinelConn.close();
		System.out.println("slave with the highest slave-repl-offset is found");
		return selectedSlaveID;
	}
	*/

	@Override
	public void beforeDivergencePath() {
		
	}

	@Override
	public void afterDivergencePath() {
		System.out.println("afterDivergencePath");
		
		/*
		RedisClient redisCli = RedisClient.create("redis://localhost:"  + (20000 + basePort));
		StatefulRedisSentinelConnection<String, String> redisSentinelConn = redisCli.connectSentinel();
		RedisSentinelCommands<String, String> redisSentinalCmds = redisSentinelConn.sync();
		int waitCnt = 0;
		while (true) {
			Map<String, String> masterInfo = redisSentinalCmds.master(clusterName);
			String flagsStr = masterInfo.get("flags");
			System.out.println("flagsStr=" + flagsStr);
			if (flagsStr.contains("o_down")) {
				System.out.println("master in the o_down status verified");
				break;
			} else {
				try {
					waitCnt++;
					if (waitCnt > 310) {
						System.err.println("timeout while waiting to verify the old master"
								+ "is in o_down status. Check if sentinels are running correctly");
						System.exit(1);
					}
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		redisSentinelConn.close();
		*/
	}

	//int rememberedOldMasterID = -1;
	
	boolean leaderElectionNeeded = false;
	
	@Override
	public void beforeResyncPath() {
		System.out.println("beforeResyncPath");		
		//rememberedOldMasterID = findLeader();
		//System.out.println("old master ID was=" + rememberedOldMasterID);
		
		/*
		int onlineNodeCnt = 0;
		for (int i = 0; i < numNode; i++) {
			if (isNodeOnline[i]) {
				onlineNodeCnt++;
			}
		}
		if (onlineNodeCnt > numNode / 2) {
			// already have a quorum, don't need to elect a new leader
			leaderElectionNeeded = false;
		} else {
			// we don't have a quorum yet, need to do leader election
			leaderElectionNeeded = true;
		}
		*/
	}

	@Override
	public void afterResyncPath() {
		System.out.println("afterResyncPath");
		// reset the rememberedOldMasterID to -1 again
		//rememberedOldMasterID = -1;
	}

	@Override
	public void triggerResyncPath(int[] targetSyncSourcesChange) {
		for (int i = 0; i < targetSyncSourcesChange.length; i++ ) {
			// we only consider if the node trying to sync from other node is online
			if (isNodeOnline[i]) {
				int tssc = targetSyncSourcesChange[i];
				// we only consider if the sync source is online
				if (tssc != -1) {
					// we create a new chain
					if (syncSources[i] == -1) {
						// ASSERT: sync source must be online by now
						if (!isNodeOnline[tssc]) {
							System.err.println("ASSERT: sync source must be online by now");
							System.exit(1);
						}
						RedisClient rcli = RedisClient.create("redis://localhost:"  + (basePort + i));
						StatefulRedisConnection<String, String> rconn = rcli.connect();
						RedisCommands<String, String> rcmdary = rconn.sync();
						System.out.println("node" + i + " sync from node" + tssc);
						rcmdary.slaveof("localhost", basePort + tssc);
						syncSources[i] = tssc;
					} else {
						// assertion: chain should not be changed once it's formed
						if (tssc != syncSources[i]) {
							System.err.println("ASSERT[1]: chain should not be changed once it's formed.");
							System.err.println("targetSyncSourcesChange=" + Arrays.toString(targetSyncSourcesChange));
							System.err.println("syncSources=" + Arrays.toString(syncSources));
							System.exit(1);
						}
					}
				} else {
					// assertion: chain should not be broken once it's formed
					if (syncSources[i] != -1) {
						System.err.println("ASSERT[2]: chain should not be broken once it's formed.");
						System.err.println("targetSyncSourcesChange=" + Arrays.toString(targetSyncSourcesChange));
						System.err.println("syncSources=" + Arrays.toString(syncSources));
						System.exit(1);
					}
				}
				
			}
		}
	}

	private static void stopEnsembleForTesting(RedisSystemController cont, int testNumNode) {
		for (int i = 0; i < testNumNode; i++) {
			cont.killNode(i);
		}
	}
	
	private static void startEnsembleForTesting(RedisSystemController cont, int testNumNode) {
		Process[] proc = new Process[testNumNode];
		int basePort = cont.basePort;
		//int node0port = 8888;
		//int node1port = 8889;
		//int node2port = 8810;
		//int node3port = 8811;
		String nodeLabelPrefix = "node";
		//String node0Label = "a";
		//String node1Label = "b";
		//String node2Label = "c";
		//String node3Label = "d";
		String redisServerScript = "/home/ben/project/redis/src/redis-server";
		String[] startRedisServerCommandTemplate = {"/bin/sh", "-c", 
				"%s --logfile /tmp/%s/redis.log --port %d &"};
		//String redisClientScript = "/home/ben/project/redis/src/redis-cli";
		//String[] invokeRedisClientAPICommand = {"/bin/sh", "-c", "%s -p %d %s"};
		ProcessBuilder builder = new ProcessBuilder();
		try {
			
			String[] prepareEnvCmd = {"/bin/sh", "-c", "mkdir -p /tmp/node%d; rm -rf /tmp/node%d/*"};
			for (int i = 0; i < testNumNode; i++) {
				Process envProc = builder.command(prepareEnvCmd).start();
				envProc.waitFor();
			}
			
			String[] startRedisServerCommand = null;
			for (int i = 0; i < testNumNode; i++) {
				startRedisServerCommand = startRedisServerCommandTemplate.clone();
				startRedisServerCommand[2] = String.format(startRedisServerCommand[2], 
						redisServerScript, nodeLabelPrefix + i, basePort + i);
				System.out.println("Invoking command=\n" + String.join(" ", startRedisServerCommand));
				proc[i] = builder.command(startRedisServerCommand).start();
				proc[i].waitFor();
				cont.isNodeOnline[i] = true;
			}

			Thread.sleep(1);
			System.out.println("Ensemble is started");
			printPS("grep redis-server | grep -v grep | grep -v ps");
			
			for (int i = 0 ; i < testNumNode; i++) {
				String pidStr = printPS("grep redis-server | grep -v grep | grep -v ps | grep " + (basePort + i) + " | awk '{print $2}'");
				int pid  = Integer.parseInt(pidStr.trim());
				cont.pidMap[i] = pid;
			}
			System.out.println(Arrays.toString(cont.pidMap));	
			
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private static String printPS(String grepStr) {
		// run ps command to see if the sentinel process is still running
		ProcessBuilder psBuilder = new ProcessBuilder();
		//String[] psCmd = { "/bin/sh", "-c", "ps aux | grep redis-server | grep -v grep | grep -v ps" };
		String[] psCmd;
		if (grepStr != null) {
			psCmd = new String[] { "/bin/sh", "-c", "ps aux | %s" };
		} else {
			psCmd = new String[] { "/bin/sh", "-c", "ps aux" };
		}
		psCmd[2] = String.format(psCmd[2], grepStr);
		Process psProc = null;
		try {
			System.out.println("Invoking cmd=\n" + String.join(" ", psCmd));
			psProc = psBuilder.command(psCmd).start();
			psProc.waitFor();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// gathered the output from the ps command ran above
		String retStr = "";
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(psProc.getInputStream()));
			StringBuilder builder = new StringBuilder();
			String line = null;
			while ((line = reader.readLine()) != null) {
				builder.append(line);
				builder.append(System.getProperty("line.separator"));
			}
			System.out.println("ps outputs:\n" + builder.toString());
			reader.close();
			retStr = builder.toString();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return retStr;
	}
	
	// for the testing purpose
	public static void main(String[] args) {
		// unit test 1: getHighestNodeState
		System.out.println("===================================================");
		System.out.println("unit test 1: findLeader() test");
		System.out.println("---------------------------------------------------");
		System.out.println("1.1. testing findLeader for the very first divergence path scenario");
		String workingDirPathName = "/home/ben/experiment/test-2-4-Redis-2.8.0-strata-0.1";
		RedisSystemController cont = new RedisSystemController(4, workingDirPathName);
		cont.isNodeOnline = new boolean[] {true, true, true, true};
		cont.syncSources = new int[] {-1, -1, -1, -1};
		int leaderID = cont.findLeader();
		System.out.println("leaderID=" + leaderID);
		if (leaderID == 3) {
			System.out.println("Test PASS");
		} else {
			System.out.println("Test FAIL");
		}
		System.out.println("1.2. testing findLeader for some intermediate divergence path scenario");
		cont.isNodeOnline = new boolean[] {true, true, true, false};
		cont.syncSources = new int[] {-1, 0, 1, -1};
		leaderID = cont.findLeader();
		System.out.println("leaderID=" + leaderID);
		if (leaderID == 0) {
			System.out.println("Test PASS");
		} else {
			System.out.println("Test FAIL");
		}
		
		// unit test 2: starting and stopping nodes
		System.out.println("===================================================");
		System.out.println("unit test 2: startNode and stopNode");
		System.out.println("---------------------------------------------------");
		System.out.println("2.1. testing starting and stopping nodes");
		int testNumNode = 4;
		int testBasePort = 8888;
		cont = new RedisSystemController(testNumNode, workingDirPathName);
		cont.basePort = testBasePort;
		startEnsembleForTesting(cont, testNumNode);
		cont.stopNode(0);
		cont.stopNode(1);
		cont.stopNode(2);
		System.out.println("Node 0, 1, 2 are stopped");
		printPS("grep redis-server | grep -v grep | grep -v ps");
		cont.startNode(0);
		cont.startNode(1);
		System.out.println("Node 0, 1 are started");
		printPS("grep redis-server | grep -v grep | grep -v ps");
		cont.startNode(2);
		System.out.println("Node 2 are started");
		printPS("grep redis-server | grep -v grep | grep -v ps");
		stopEnsembleForTesting(cont, testNumNode);
		System.out.println("Node 0, 1, 2, 3 are all killed");
		printPS("grep redis-server | grep -v grep | grep -v ps");
		
		// unit test 3: creating, writing and modifying
		System.out.println("===================================================");
		System.out.println("unit test 3: creating, writing and modifying");
		System.out.println("---------------------------------------------------");
		System.out.println("3.1. creating a record initialized to 0");
		startEnsembleForTesting(cont, testNumNode);
		for (int i = 0; i < testNumNode; i++) {
			cont.bootstrapClientWithPort(i, testBasePort + i);
		}
		cont.createData(0, "a", 0);
		byte[] valueReadByteArr = null;
		String valueReadString = "";
		try {
			for (int i = 0 ; i < testNumNode; i++) {
				valueReadByteArr = cont.readData(i, "a");
				valueReadString = new String(valueReadByteArr, "UTF-8");
				System.out.println("node" + i + " has value=" + valueReadString);
				if (!valueReadString.equals("0")) {
					System.err.println("Test Fail: For node" + i + " value of a is " + valueReadString);
					System.exit(1);
				}
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("unit test 3.1 PASS");
		System.out.println("3.2. writing 1 to a on node 3");
		cont.stopNode(0);
		cont.stopNode(1);
		cont.stopNode(2);
		System.out.println("isNodeOnline=" + Arrays.toString(cont.isNodeOnline));
		cont.writeData(3, "a", 1);
		try {
			for (int i = 0 ; i < testNumNode; i++) {
				if (cont.isNodeOnline[i]) {
					valueReadByteArr = cont.readData(i, "a");
					valueReadString = new String(valueReadByteArr, "UTF-8");
					System.out.println("node" + i + " has value=" + valueReadString);
					if (i == 3) {
						if (!valueReadString.equals("1")) {
							System.err.println("Test Fail: For node" + i + " value of a is " + valueReadString);
							System.exit(1);
						}
					} else {
						if (!valueReadString.equals("0")) {
							System.err.println("Test Fail: For node" + i + " value of a is " + valueReadString);
							System.exit(1);
						}
					}
				}
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		cont.stopNode(3);
		System.out.println("Stopped node 3. isNodeOnline=" + Arrays.toString(cont.isNodeOnline));
		System.out.println("Test 3.2 PASS");

		// unit test 4: triggerResyncPath test
		System.out.println("===================================================");
		System.out.println("unit test 4: triggerResyncPath and waitForResync test");
		System.out.println("---------------------------------------------------");
		System.out.println("4.1. create resync chain 0->1->2");
		cont.rwmMode = ResyncWaitMode.RWM_PROBE;
		cont.startNode(0);
		cont.startNode(1);
		cont.startNode(2);
		System.out.println("4.1. Before resync, syncSources=" + Arrays.toString(cont.syncSources));
		cont.triggerResyncPath(new int[] {-1, 0, -1, -1});
		int[] restartNodes = new int[] {0, 1};
		cont.waitForResync(restartNodes);
		//try {
		//	Thread.sleep(2000);
		//} catch (InterruptedException e1) {
		//	// TODO Auto-generated catch block
		//	e1.printStackTrace();
		//}
		System.out.println("4.1. After resync 1, syncSources=" + Arrays.toString(cont.syncSources));
		cont.triggerResyncPath(new int[] {-1, 0, 1, -1});
		restartNodes = new int[] {2};
		cont.waitForResync(restartNodes);
		//try {
		//	Thread.sleep(2000);
		//} catch (InterruptedException e1) {
		//	// TODO Auto-generated catch block
		//	e1.printStackTrace();
		//}
		System.out.println("4.1. After resync 2, syncSources=" + Arrays.toString(cont.syncSources));
		cont.writeData(0, "a", 2);
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			for (int i = 0 ; i < testNumNode; i++) {
				if (cont.isNodeOnline[i]) {
					valueReadByteArr = cont.readData(i, "a");
					valueReadString = new String(valueReadByteArr, "UTF-8");
					System.out.println("node" + i + " has value=" + valueReadString);
					if (!valueReadString.equals("2")) {
						System.err.println("Test 4.1. Fail: For node" + i + " value of a is " + valueReadString);
						System.exit(1);
					}
				} else {
					System.out.println("Skipping reading from the offline node="+i);
				}
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		cont.stopNode(0);
		cont.stopNode(1);
		cont.stopNode(2);
		System.out.println("Test 4.1. PASS");
		
		System.out.println("4.2. restart node 0, 1; 3; 2 and create a resync chain 3->0->1->2");
		cont.startNode(0);
		cont.startNode(1);
		System.out.println("4.2. Before resync, syncSources=" + Arrays.toString(cont.syncSources));
		cont.triggerResyncPath(new int[] {-1, 0, 1, -1});
		restartNodes = new int[] {0, 1};
		cont.waitForResync(restartNodes);
		//try {
		//	Thread.sleep(2000);
		//} catch (InterruptedException e1) {
		//	// TODO Auto-generated catch block
		//	e1.printStackTrace();
		//}
		System.out.println("4.2. After resync 1, syncSources=" + Arrays.toString(cont.syncSources));
		cont.startNode(3);
		cont.triggerResyncPath(new int[] {3, 0, 1, -1});
		restartNodes = new int[] {3};
		cont.waitForResync(restartNodes);
		//try {
		//	Thread.sleep(2000);
		//} catch (InterruptedException e1) {
		//	// TODO Auto-generated catch block
		//	e1.printStackTrace();
		//}

		System.out.println("4.2. After resync 2, syncSources=" + Arrays.toString(cont.syncSources));
		cont.startNode(2);
		restartNodes = new int[] {2};
		cont.waitForResync(restartNodes);
		//try {
		//	Thread.sleep(2000);
		//} catch (InterruptedException e1) {
		//	// TODO Auto-generated catch block
		//	e1.printStackTrace();
		//}

		System.out.println("4.2. After resync 3, syncSources=" + Arrays.toString(cont.syncSources));
		
		System.out.println("Wait for the last minute verification...");
		try {
			Thread.sleep(6000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			boolean verifResult = true;
			for (int i = 0 ; i < testNumNode; i++) {
				if (cont.isNodeOnline[i]) {
					valueReadByteArr = cont.readData(i, "a");
					valueReadString = new String(valueReadByteArr, "UTF-8");
					System.out.println("node" + i + " has value=" + valueReadString);
					if (i != 2 && !valueReadString.equals("1")) {
						System.err.println("Test 4.2. Fail: node"+ i + " is supposed to have 1");
						System.exit(1);
					} else if (i == 2 && !valueReadString.equals("2")) {
						System.err.println("Test 4.2. Fail: node 2 is supposed to have 2");
						System.exit(1);
					}
				} else {
					System.out.println("Skipping reading from the offline node="+i);
				}
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Test 4.2. PASS: We found inconsistency!");
		for (int i = 0; i < testNumNode; i++) {
			cont.takedownClient(i);
		}
		stopEnsembleForTesting(cont, testNumNode);
		System.out.println("RedisSystemController main function testing is done");
	}
	
}
