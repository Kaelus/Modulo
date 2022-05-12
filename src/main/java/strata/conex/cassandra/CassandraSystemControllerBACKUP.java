package strata.conex.cassandra;

import java.util.ArrayList;

import strata.common.Path;
import strata.conex.SystemController;

public class CassandraSystemControllerBACKUP extends SystemController {
/*
	protected String sutSrcDir; //= "/home/ben/project/cassandra"; // HARD-CODED
	protected String libClasspath; 
		//= ":/home/ben/project/cass-hacking/cassandra/build/lib/jars/*"
		//+ ":/home/ben/project/cass-hacking/cassandra/lib/*"
		//+ ":/usr/lib/jvm/java-8-oracle/lib/tools.jar"
		//+ ":/home/ben/project/cass-hacking/cassandra/build/classes/thrift"
		//+ ":/home/ben/project/cass-hacking/cassandra/test/conf";
	
	protected String updatedClasspath = null;
	protected String sutLogConfig;
	
	final int clientPorts[] = new int[numNode];
	Cluster[] clusters;
	Session[] sessions;
	String[] endpoints;
	int nativeTranPort = 9042;
	
	String queryStr;
	SimpleStatement query;
	
	protected enum CommitWaitMode {CWM_SLEEP, CWM_PROBE};
	protected enum ResyncWaitMode {RWM_SLEEP, RWM_PROBE};
	protected int waitForCommitDuration = 3000; // ms
	protected int waitForResyncDuration = 3000; // ms
	public CommitWaitMode cwmMode = CommitWaitMode.CWM_SLEEP; // HARD-CODED
	public ResyncWaitMode rwmMode = ResyncWaitMode.RWM_PROBE; // HARD-CODED
	
	protected String[] logFileNames;
	final String logFileName = "system.log"; // HARD-CODED
	
	public boolean[] isNodeOnline;
	public int actualLeader = -1;
	
	static final String[] CMD = { "java", "-Dcassandra.jmx.local.port=%d",
            "-Dlogback.configurationFile=logback.xml",
            "-Dcassandra.logdir=%s/log/%d", "-Dcassandra.storagedir=%s/data/%d",
            "-Dcassandra-foreground=no", "-cp", "%s",
        	"-Dlog4j.defaultInitOverride=true",
            "-Did=%d", "org.apache.cassandra.service.CassandraDaemon" }; // HARD-CODED
	
	public CassandraSystemController(int numNode, String workingdir) {
		super(numNode, workingdir);
		clusters = new Cluster[numNode];
		sessions = new Session[numNode];
		endpoints = new String[numNode];
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
		
	}

	@Override
	public int findLeader() {
		return 0;
	}

	@Override
	public void bootstrapClients() {
		System.out.println("bootstrapClients");
		for (int i = 0; i < numNode; i++) {
			endpoints[i] = "127.0.0." + (i+1);
		}
		for (int i = 0; i < numNode; i++) {
			clusters[i] = Cluster.builder().addContactPoints(endpoints[i])
					.withPort(nativeTranPort)
					.withRetryPolicy(FallthroughRetryPolicy.INSTANCE).build();
			clusters[i].getConfiguration().getSocketOptions().setReadTimeoutMillis(31000);
			sessions[i] = clusters[i].connect();
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
		for (int i = 0; i < numNode; i++) {
        	sessions[i].close();
        	clusters[i].close();
        }
	}
	
	private void initializeTable() {
		System.out.println("Creating the table...");
        sessions[0].execute("drop keyspace if exists aigenkeyspace;");
        sessions[0].execute("create keyspace aigenkeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };");
        sessions[0].execute("CREATE TABLE aigenkeyspace.aigentable (\n" +
                "  key text PRIMARY KEY,\n" +
                "  val text\n" +
                ") WITH read_repair_chance = 0;");
        System.out.println("Table is created.");
	}
	
	@Override
	public void startEnsemble() {
		System.out.println("startEnsemble");
		ProcessBuilder builder = new ProcessBuilder();
		builder.directory(new File(workingDir));
		for (int i = 0; i < numNode; ++i) {
			String[] cmd = Arrays.copyOf(CMD, CMD.length);
			cmd[1] = String.format(cmd[1], 7199 + i);
            cmd[3] = String.format(cmd[3], workingDir, i);
            cmd[4] = String.format(cmd[4], workingDir, i);
            cmd[7] = String.format(cmd[7], updatedClasspath);
            cmd[9] = String.format(cmd[9], i);
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
		
		initializeTable();
		
		//// cluster = Cluster.builder().addContactPoint("127.0.0.4").withLoadBalancingPolicy(new RoundRobinPolicy()).build();
        //cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        //cluster.getConfiguration().getPoolingOptions().setPoolTimeoutMillis(100000000);
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
		/*String[] rmLogCmd = { "rm", "-f", workingDir + "/log/" + id + "/zookeeper.log" };
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
	
	/*
		// start the node now
		String[] cmd = Arrays.copyOf(CMD, CMD.length);
		cmd[1] = String.format(cmd[1], 7199 + id);
        cmd[3] = String.format(cmd[3], workingDir, id);
        cmd[4] = String.format(cmd[4], workingDir, id);
        cmd[7] = String.format(cmd[7], updatedClasspath);
        cmd[9] = String.format(cmd[9], id);
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
	
	@Override
	public byte[] readData(int nodeID, String key) {
		ResultSet results = sessions[nodeID].execute(
                "SELECT * FROM aigenkeyspace.aigentable " +
                "WHERE key = '" + key + "';");
        Row row = results.one();
        byte[] valRead = row.getString("val").getBytes();
		return valRead;
	}

	@Override
	public void createData(int nodeID, String key, int value) {
		System.out.println("createData node=" + nodeID + " key=" + key + " value=" + value);
		while (true) {
			queryStr = "INSERT INTO aigenkeyspace.aigentable (key, val) " +
	                "VALUES ('" + key + "', '" + value + "');";
	    	query = new SimpleStatement(queryStr);
	    	query.setConsistencyLevel(ConsistencyLevel.ALL);
	    	try {
        		sessions[0].execute(query);
        		break;
        	} catch (Exception e) {
        		e.printStackTrace();
				System.exit(1);
        	}
		}
	}

	@Override
	public void writeData(int nodeID, String key, int value) {
		System.out.println("writeData node=" + nodeID + " key=" + key + " value=" + value);
		queryStr = "UPDATE aigenkeyspace.aigentable " +
                "SET val = '" + value + "' " +
                "WHERE key = '" + key + "';";
		query = new SimpleStatement(queryStr);
		query.setConsistencyLevel(ConsistencyLevel.ANY);
		sessions[nodeID].executeAsync(query);
	}

	@Override
	public int[] sortTargetState(int[] targetState) {
		return targetState;
	}

	@Override
	protected boolean waitForCommit(ArrayList<Integer> commitNodes, String key, int val) {
		try {
			Thread.sleep(waitForCommitDuration);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return true;
	}

	@Override
	protected boolean waitForResync(int[] restartNodeIDs) {
		System.out.println("resync wait using sleep");
		try {
			Thread.sleep(waitForResyncDuration);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return true;
	}
	*/

	public CassandraSystemControllerBACKUP(int numNode, String workingdir) {
		super(numNode, workingdir);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void beforeDivergencePath() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void afterDivergencePath() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void beforeResyncPath() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void afterResyncPath() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void sysCtrlParseConfigInit(String configFile) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void prepareTestingEnvironment() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int findLeader() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void bootstrapClients() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void takedownClients() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void startEnsemble() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stopEnsemble() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void startNode(int id) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stopNode(int id) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void resetTest() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] readData(int nodeID, String key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void createData(int nodeID, String key, int value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeData(int nodeID, String key, int value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int[] sortTargetState(int[] targetState) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected boolean waitForCommit(ArrayList<Integer> commitNodes, String key, int val) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected boolean waitForResync(int[] restartNodeIDs) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void triggerResyncPath(int[] targetSyncSourcesChange) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void loadPath(Path path) {
		// TODO Auto-generated method stub
		
	}
	
}
