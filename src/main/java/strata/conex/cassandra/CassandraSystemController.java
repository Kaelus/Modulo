package strata.conex.cassandra;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import strata.conex.SystemController;
import strata.conex.TestingEngine;

public class CassandraSystemController extends SystemController {

	//protected final static String caSrcDir = "/home/ben/project/cass-hacking/cassandra"; // HARD-CODED
	//protected final static String libClasspath 
	//	= ":/home/ben/project/cass-hacking/cassandra/build/lib/jars/*"
	//	+ ":/home/ben/project/cass-hacking/cassandra/lib/*"
	//	+ ":/usr/lib/jvm/java-8-oracle/lib/tools.jar"
	//	+ ":/home/ben/project/cass-hacking/cassandra/build/classes/thrift"
	//	+ ":/home/ben/project/cass-hacking/cassandra/test/conf";
	
	//protected String updatedClasspath = null;
	
	protected String sutSrcDir;
	protected String deployScriptDir;
	protected String cassHomeDir;
	protected String sutLogConfig;
	
	final int clientPorts[] = new int[numNode];
	//Cluster[] clusters;
	CqlSession[] sessions;
	String[] endpoints;
	
	protected enum CommitWaitMode {CWM_SLEEP, CWM_PROBE};
	protected enum ResyncWaitMode {RWM_SLEEP, RWM_PROBE};
	protected int waitForCommitDuration = 3000; // ms
	protected int waitForResyncDuration = 3000; // ms
	protected int waitBeforeVerificationDuration = 6000; // ms
	public CommitWaitMode cwmMode = CommitWaitMode.CWM_SLEEP; // HARD-CODED
	public ResyncWaitMode rwmMode = ResyncWaitMode.RWM_PROBE; // HARD-CODED
	
	protected String[] logFileNames;
	final String logFileName = "system.log"; // HARD-CODED
	
	public boolean[] isNodeOnline;
	public int actualLeader = -1;
	
	public int defaultJMXPort = 7199; //HARD-CODED
	
	public int[] pidMap; // indexed by node IDs
	
	//static final String[] CMD = { "java", "-Dcassandra.jmx.local.port=%d",
    //        "-Dlogback.configurationFile=logback.xml",
    //        "-Dcassandra.logdir=%s/log/%d", "-Dcassandra.storagedir=%s/data/%d",
    //        "-Dcassandra-foreground=no", "-cp", "%s/conf/%d:" + System.getenv("CLASSPATH"),
    //    	"-Dlog4j.defaultInitOverride=true",
    //        "-Did=%d", "org.apache.cassandra.service.CassandraDaemon" }; // HARD-CODED
	
	public CassandraSystemController(int numNode, String workingdir) {
		super(numNode, workingdir);
		//clusters = new Cluster[numNode];
		sessions = new CqlSession[numNode];
		//endpoints = new String[numNode];
		logFileNames = new String[numNode];
		pidMap = new int[numNode];
		isNodeOnline = new boolean[numNode];
		
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
		       } else if (line.startsWith("cassHomeDir")) {
		    	   String[] tokens = line.split("=");
		    	   cassHomeDir = tokens[1];
		    	   System.out.println("cassHomeDir=" + cassHomeDir);
		       } else if (line.startsWith("waitForCommitDuration")) {
		    	   String[] tokens = line.split("=");
		    	   waitForCommitDuration = Integer.parseInt(tokens[1]);
		       } else if (line.startsWith("waitForResyncDuration")) {
		    	   String[] tokens = line.split("=");
		    	   waitForResyncDuration = Integer.parseInt(tokens[1]);
		       } else if (line.startsWith("waitBeforeVerificationDuration")) {
		    	   String[] tokens = line.split("=");
		    	   waitBeforeVerificationDuration = Integer.parseInt(tokens[1]);
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
		String[] setupCmd = { "./setup.sh", "%s", "%s", "%s" };
		setupCmd[1] = String.format(setupCmd[1], numNode);
		setupCmd[2] = String.format(setupCmd[2], workingDir);
		setupCmd[3] = String.format(setupCmd[3], cassHomeDir);
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
		for (int i = 0; i < numNode; i++) {
			logFileNames[i] = workingDir + "/cass_dir" + "/" + i + "/log/" + logFileName;
		}
	}

	@Override
	public int findLeader() {
		// return the first online node
		int leaderID = -1;
		for (int i = 0; i < numNode; i++) {
			if (isNodeOnline[i]) {
				leaderID = i;
				break;
			}
		}
		return leaderID;
	}

	@Override
	public void bootstrapClients() {
		
	}

	@Override
	public void takedownClients() {
		
	}
	
	private String getStatusStateFromNodetoolStatusOutForNode(int answerer, int nodeID) {
		if (!isNodeOnline[answerer]) {
			System.err.println("Error: answerer is not an online node.");
			System.exit(1);
		} else if (answerer < 0) {
			System.err.println("Error: answerer ID is negative.");
			System.exit(1);
		}
		
		String returnStr = "";
		ProcessBuilder psBuilder = new ProcessBuilder();
		String[] psCmd = { "/bin/sh", "-c", "%s/nodetool -h %s -p %d status | grep %s | awk '{print $1}'" };
		int baseIPEnding = 1;
		//String nodeIPString = "127.0.0." + (baseIPEnding + nodeID);
		String nodeIPString = "127.0.0.1"; // Cassandra by default accept nodetool request from localhost 127.0.0.1 only
		String nodeIPStringPrefix = "127.0.0.";
		
		psCmd[2] = String.format(psCmd[2],
				cassHomeDir + "/bin",
				nodeIPString,
				defaultJMXPort + (100 * answerer),
				nodeIPStringPrefix + (1 + nodeID));
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
		// gathered the output from the nodetool status command ran above
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(psProc.getInputStream()));
			StringBuilder strBuilder = new StringBuilder();
			String line = null;
			while ((line = reader.readLine()) != null) {
				strBuilder.append(line);
			}
			if (!strBuilder.toString().isEmpty()) {
				System.out.println("We succeeded to get status state of node=" + nodeID);
				String result = strBuilder.toString();
				System.out.println("Status/State=" + result);
				returnStr = result;
			}
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		return returnStr;
	}
	
	private int getPIDFromPSOutForNode(int nodeID) {
		int returnPID = -1;
		ProcessBuilder psBuilder = new ProcessBuilder();
		String[] psCmd = { "/bin/sh", "-c", "ps aux | grep CassandraDaemon | grep -v grep | grep %d | awk '{print $2}'" };
		psCmd[2] = String.format(psCmd[2], defaultJMXPort + (nodeID*100));
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
			StringBuilder strBuilder = new StringBuilder();
			String line = null;
			while ((line = reader.readLine()) != null) {
				strBuilder.append(line);
			}
			if (!strBuilder.toString().isEmpty()) {
				System.out.println("We succeeded to get the pid of the node we started");
				String result = strBuilder.toString();
				System.out.println("PID=" + result);
				returnPID = Integer.parseInt(result);
			}
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return returnPID;
	}
	
	private void initializeTable() {
		System.out.println("Creating the table...");
		while (true) {
			try {
				CqlSessionBuilder builder = CqlSession.builder();
				//builder.addContactPoint(new InetSocketAddress("127.0.0.1", 9042));
				//builder.withLocalDatacenter("datacenter1");
				CqlSession session = builder.build();
	
				DriverConfig config = session.getContext().getConfig();
				DriverExecutionProfile defaultProfile = config.getDefaultProfile();
				DriverExecutionProfile slowProfile = config.getProfile("slow");
				config.getProfiles().forEach((name, profile) -> System.out.println("" + name 
						+ "=" + profile.toString()));
				
				System.out.println("defaultProfile=" + defaultProfile.getDuration(DefaultDriverOption.REQUEST_TIMEOUT).toString());
				System.out.println("slowProfile=" + slowProfile.getDuration(DefaultDriverOption.REQUEST_TIMEOUT).toString());
				
				SimpleStatement s =
						  SimpleStatement.builder("drop keyspace if exists modulokeyspace;")
						      .setExecutionProfileName("slow")
						      .build();
				session.execute(s);
				//session.execute("drop keyspace if exists modulokeyspace;");
				
				s = SimpleStatement.builder("create keyspace modulokeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
						.setExecutionProfile(slowProfile)
						.build();
				session.execute(s);
				//session.execute("create keyspace modulokeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };");
		        
				s = SimpleStatement.builder("CREATE TABLE modulokeyspace.modulotable (\n" +
		                "  key text PRIMARY KEY,\n" +
		                "  val text\n" +
		                ") WITH read_repair_chance = 0;")
						.setExecutionProfile(slowProfile)
						.build();
				session.execute(s);
				//session.execute("CREATE TABLE modulokeyspace.modulotable (\n" +
		        //        "  key text PRIMARY KEY,\n" +
		        //        "  val text\n" +
		        //        ") WITH read_repair_chance = 0;");
		        
				session.close();
		        System.out.println("Table is created.");
		        break;
			} catch (AllNodesFailedException anfe) {
				anfe.printStackTrace();
				anfe.getErrors().get(anfe.getErrors().keySet().iterator().next()).printStackTrace();
			}
		}
	}

	@Override
	public void startEnsemble() {
		System.out.println("startEnsemble");
		
		for (int i = 0; i < numNode; i++) {
			startNode(i);
		}
		
		/*
		ProcessBuilder builder = new ProcessBuilder();
		builder.directory(new File(workingDir));
		
		for (int i = 0; i < numNode; ++i) {
			String[] rmLogCmd = { "rm", "-f", workingDir + "/cass_dir/" + i + "/log/system.log"};
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
			String[] cassandraCmd = {"/bin/sh", "-c", "%s/cassandra%d", "-f"};
			cassandraCmd[2]	= String.format(cassandraCmd[2], 
					cassHomeDir + "/bin", i);
			System.out.println("Starting node " + i);
	        try {
	        	System.out.println("Invoking command=\n" + String.join(" ", cassandraCmd));
				builder.command(cassandraCmd).start();
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// get PID from the ps output and remember it
		for (int i = 0; i < numNode; ++i) {
			while(true) {
				int newPID = getPIDFromPSOutForNode(i);
				if (newPID > 0) {
					pidMap[i] = newPID; 
					System.out.println("pidMap[" + i + "]=" + pidMap[i]);
					break;
				} else {
					System.out.println("We did not yet get the output of the command ps...=\n");
					try {
						Thread.sleep(3000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
	        }
		}
		
		// get Status and State from the nodetool status output 
		for (int i = 0; i < numNode; ++i) {
			int waitForUNStatusStateCount = 0;
			while(true) {
				String statusState = getStatusStateFromNodetoolStatusOutForNode(i);
				boolean waitMoreFlag = false;
				if (statusState.length() > 0) {
					if (statusState.contentEquals("UN")) {
						System.out.println("NodeID=" + i + " is running as Up and Normal");
						waitForUNStatusStateCount++;
						if (waitForUNStatusStateCount > 10) {
							System.err.println("Error: Node did not reach UN status state");
							System.exit(1);
						}
						break;
					} else {
						waitMoreFlag = true;
					}
				} else {
					waitMoreFlag = true;
				}
				if (waitMoreFlag) {
					try {
						Thread.sleep(3000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
		
		Arrays.fill(isNodeOnline, true);
		*/
		
		System.out.println("All nodes are up and normal");
		
		/*
		ProcessBuilder psBuilder = new ProcessBuilder();
		String[] psCmd = { "/bin/sh", "-c", "%s/nodetool -h %s -p %d status" };
		int baseIPEnding = 1;
		//String nodeIPString = "127.0.0." + (baseIPEnding + nodeID);
		String nodeIPString = "127.0.0.1"; // Cassandra by default accept nodetool request from localhost 127.0.0.1 only
		String nodeIPStringPrefix = "127.0.0.";
		psCmd[2] = String.format(psCmd[2],
				cassHomeDir + "/bin",
				nodeIPString,
				defaultJMXPort + 100);
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
		// gathered the output from the nodetool status command ran above
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(psProc.getInputStream()));
			StringBuilder strBuilder = new StringBuilder();
			String line = null;
			while ((line = reader.readLine()) != null) {
				strBuilder.append(line);
				strBuilder.append(System.getProperty("line.separator"));
			}
			String result = strBuilder.toString();
			System.out.println("nodetool output=" + result);
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	*/
		
		initializeTable();
		
	}

	@Override
	public void stopEnsemble() {
		System.out.println("stopEnsemble");
		for (int i = 0; i < numNode; i++) {
			stopNode(i);
		}
	}

	@Override
	public void startNode(int id) {
		System.out.println("startNode");
		ProcessBuilder builder = new ProcessBuilder();
		builder.directory(new File(workingDir));
		
		String[] rmLogCmd = { "rm", "-f", workingDir + "/cass_dir/" + id + "/log/system.log"};
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
		String[] cassandraCmd = {"/bin/sh", "-c", "%s/cassandra%d", "-f"};
		cassandraCmd[2]	= String.format(cassandraCmd[2], 
				cassHomeDir + "/bin", id);
		System.out.println("Starting node " + id);
        try {
        	System.out.println("Invoking command=\n" + String.join(" ", cassandraCmd));
			builder.command(cassandraCmd).start();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
        
        // get PID from the ps output and remember it
        while(true) {
			int newPID = getPIDFromPSOutForNode(id);
			if (newPID > 0) {
				pidMap[id] = newPID; 
				System.out.println("pidMap[" + id + "]=" + pidMap[id]);
				break;
			} else {
				System.out.println("We did not yet get the output of the command ps...=\n");
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
        }
        // get Status and State from the nodetool status output
        isNodeOnline[id] = true;
        int leader = findLeader();
        int waitForUNStatusStateCount = 0;
		while(true) {
			String statusState = getStatusStateFromNodetoolStatusOutForNode(leader, id);
			boolean waitMoreFlag = false;
			if (statusState.length() > 0) {
				if (statusState.contentEquals("UN")) {
					System.out.println("NodeID=" + id + " is running as Up and Normal");
					waitForUNStatusStateCount++;
					if (waitForUNStatusStateCount > 10) {
						System.err.println("Error: Node did not reach UN status state");
						System.exit(1);
					}
					break;
				} else {
					waitMoreFlag = true;
				}
			} else {
				waitMoreFlag = true;
			}
			if (waitMoreFlag) {
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
     	System.out.println("node " + id + " is up and normal");
	}

	@Override
	public void stopNode(int id) {
		System.out.println("stopNode " + id);
		killNode(id);
		isNodeOnline[id] = false;
	}

	// explicitly kill the process associated with the given node id
	private void killNode(int nodeID) {
		System.out.println("killNode " + nodeID);
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
			
			// run ps command to see if the CassandraDaemon process is still running
			ProcessBuilder psBuilder = new ProcessBuilder();
			String[] psCmd = { "/bin/sh", "-c", "ps aux | grep CassandraDaemon | grep -v grep | grep -v ps | awk '{print $2}' | grep \" %d \"" };
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
	public void resetTest() {
		System.out.println("resetTest");
		if (TestingEngine.programMode) {
			System.out.println("Program Mode is enabled. We don't need to reset anything here.");
			return;
		}
		// delete cass_dir directory in the working directory
        ProcessBuilder builder = new ProcessBuilder();
        builder.directory(new File(workingDir));
        String[] rmDataCmd = { "rm", "-rf", workingDir + "/cass_dir" };
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
        // wait until there is no cass_dir directory
        while (true) {
	        File cassDir = new File(workingDir + "/cass_dir");
			if (!cassDir.exists()) {
				System.out.println("verified that cass_dir directory is removed!");
				break;
			}
        }
	}

	@Override
	public byte[] readData(int nodeID, String key) {
		System.out.println("readData nodeID=" + nodeID + " key=" + key);
		CqlSessionBuilder builder = CqlSession.builder();
		builder.addContactPoint(new InetSocketAddress(("127.0.0." + (1 + nodeID)), 9042));
		builder.withLocalDatacenter("datacenter1");
		CqlSession session = builder.build();
		
		DriverConfig config = session.getContext().getConfig();
		DriverExecutionProfile slowProfile = config.getProfile("slow");
		String queryStr = "SELECT * FROM modulokeyspace.modulotable " +
                "WHERE key = '" + key + "';";
		SimpleStatement s =
				  SimpleStatement.builder(queryStr)
				      .setExecutionProfileName("slow")
				      .build();
		ResultSet results = session.execute(queryStr);
		Row row = results.one();
        byte[] valRead = row.getString("val").getBytes();
		return valRead;
	}

	@Override
	public void createData(int nodeID, String key, int value) {
		System.out.println("createData node=" + nodeID + " key=" + key + " value=" + value);
		
		CqlSessionBuilder builder = CqlSession.builder();
		//builder.addContactPoint(new InetSocketAddress("127.0.0.1", 9042));
		//builder.withLocalDatacenter("datacenter1");
		CqlSession session = builder.build();

		DriverConfig config = session.getContext().getConfig();
		DriverExecutionProfile slowProfile = config.getProfile("slow");
		String queryStr = "INSERT INTO modulokeyspace.modulotable (key, val) " +
                "VALUES ('" + key + "', '" + value + "');";
		SimpleStatement s =
				  SimpleStatement.builder(queryStr)
				      .setExecutionProfileName("slow")
				      .build();
		s.setConsistencyLevel(ConsistencyLevel.ALL);
    	try {
    		session.execute(s);
    	} catch (Exception e) {
    		e.printStackTrace();
			System.exit(1);
    	}
		session.close();
		
	}

	@Override
	public void writeData(int nodeID, String key, int value) {
		System.out.println("writeData node=" + nodeID + " key=" + key + " value=" + value);
		
		CqlSessionBuilder builder = CqlSession.builder();
		builder.addContactPoint(new InetSocketAddress(("127.0.0." + (1 + nodeID)), 9042));
		builder.withLocalDatacenter("datacenter1");
		CqlSession session = builder.build();
		
		DriverConfig config = session.getContext().getConfig();
		DriverExecutionProfile slowProfile = config.getProfile("slow");
		String queryStr = "UPDATE modulokeyspace.modulotable " +
                "SET val = '" + value + "' " +
                "WHERE key = '" + key + "';";
		SimpleStatement s =
				  SimpleStatement.builder(queryStr)
				      .setExecutionProfileName("slow")
				      .build();
		s.setConsistencyLevel(ConsistencyLevel.ANY);
		session.executeAsync(queryStr);
		
		
//		try {
//			System.out.println("pausing in writeData");
//			Thread.sleep(600000);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
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
//		try {
//			System.out.println("pausing in waitForResync");
//			Thread.sleep(600000);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		System.out.println("resync wait using sleep");
		try {
			Thread.sleep(waitForResyncDuration);
		} catch (InterruptedException e) {
			e.printStackTrace();
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

	@Override
	public void waitBeforeVerification() {
		System.out.println("waitBeforeVerification entered. will wait for " + (waitBeforeVerificationDuration)
					+ " ms");
		try {
			Thread.sleep(waitBeforeVerificationDuration);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
}
