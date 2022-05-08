package strata.conex.mongo;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.BsonTimestamp;
import org.bson.Document;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoSocketWriteException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.TaggableReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import strata.conex.SystemController;
import strata.conex.TestingEngine;

public class MongoSystemController extends SystemController {

	final int basePort = 27017;

	final int defaultHeartbeatTimeoutSecs = 10; // HARD-CODED
	final int augmentedHeartbeatTimeoutSecs = 600; // HARD-CODED
	
	protected String sutSrcDir;
	protected String deployScriptDir;
	protected String sutLogConfig;
	
	protected enum CommitWaitMode {CWM_SLEEP, CWM_PROBE};
	protected enum ResyncWaitMode {RWM_SLEEP, RWM_PROBE};
	protected int waitForCommitDuration; // = 3000; // ms
	protected int waitForResyncDuration; // = 3000; // ms
	public CommitWaitMode cwmMode; //= CommitWaitMode.CWM_SLEEP; // HARD-CODED
	public ResyncWaitMode rwmMode; //= ResyncWaitMode.RWM_PROBE; // HARD-CODED
	
	protected String[] logFileNames;
	final String logFileName = "mongod.log"; // HARD-CODED
	
	public boolean[] isNodeOnline;
	public int actualLeader = -1;
	
	public int[] pidMap; // indexed by node IDs
	
	// mongo handles
	MongoClient[] mongoClients;
	MongoDatabase[] databases;
	MongoCollection<Document>[] collections;
	
	public MongoSystemController(int numNode, String workingdir) {
		super(numNode, workingdir);
		isNodeOnline = new boolean[numNode];
		mongoClients = new MongoClient[numNode];
		databases = new MongoDatabase[numNode];
		collections = new MongoCollection[numNode];
		logFileNames = new String[numNode];
		pidMap = new int[numNode];
		System.out.println("MongoSystemController is constructed");
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
		for (int i = 0; i < numNode; i++) {
			logFileNames[i] = workingDir + "/log" + "/" + i + "/" + logFileName;
		}
	}

	@Override
	public int findLeader() {
		System.out.println("findLeader");
		String[] resStrArray = new String[numNode];
    	for (int i = 0 ; i < logFileNames.length ; i++) {
    		//System.out.println("Considering logfile=" + logFileNames[i]);
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
                	if (line.contains("transition to")) {
                    	//System.out.println("found state transition log message");
                		String[] token = line.split("\\s+");
                		if (token.length > 7) {
	                        if (token[6].trim().equals("primary")) {
	                        	//System.out.println("found PRIMARY node");
	                            state = "LEADING";
	                        } else if (token[6].trim().equals("SECONDARY")) {
	                        	//System.out.println("found SECONDARY node");
	                            state = "FOLLOWING";
	                        } else {
	                            state = "LOOKING";
	                        }
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
	
	private boolean findCommitMessage(int nodeID, String key, String value) {
		System.out.println("findCommitMessage");
		System.out.println("nodeID=" + nodeID + " key=" + key + " value=" + value);
		try  {
			String logFileName = logFileNames[nodeID];
			BufferedReader reader = new BufferedReader(new FileReader(logFileName));
            String line;
            while ((line = reader.readLine()) != null) {
            	if (line.contains("_id: " + "\"" + key + "\"") 
            			&& line.contains("value: " + "\"" + value + "\"")) {
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
	public void bootstrapClients() {
		System.out.println("bootstrapClients");
		for (int i = 0; i < numNode; i++) {
			MongoClientOptions options = MongoClientOptions
                    .builder()
                    .readPreference(TaggableReadPreference.secondaryPreferred(new TagSet(new Tag("nodeID", "" + i))))
                    .build();
			mongoClients[i] = new MongoClient("127.0.0.1:" + (basePort + i), options); 
		}
		//try { Thread.sleep(300); } catch (InterruptedException e) { e.printStackTrace(); }
		
		// additionally, instantiate the handles.
		for (int i = 0; i < numNode; i++) {
			// get handle to "mydb" database
			if (databases[i] == null) {
				databases[i] = mongoClients[i].getDatabase("mydb").withReadPreference(ReadPreference.secondaryPreferred());
			}
			// get a handle to the "test" collection
			if (collections[i] == null) {
				collections[i] = databases[i].getCollection("test").withReadPreference(ReadPreference.secondaryPreferred());
			}
		}
	}
	
	private void bootstrapClient(int nodeID) {
		System.out.println("bootstrapClient for nodeID=" + nodeID);
		MongoClientOptions options = MongoClientOptions.builder()
				.readPreference(TaggableReadPreference.secondaryPreferred(new TagSet(new Tag("nodeID", "" + nodeID))))
				.build();
		mongoClients[nodeID] = new MongoClient("127.0.0.1:" + (basePort + nodeID), options); 
		//try { Thread.sleep(300); } catch (InterruptedException e) { e.printStackTrace(); }
		
		// additionally, instantiate the handles.
		// get handle to "mydb" database
		if (databases[nodeID] == null) {
			databases[nodeID] = mongoClients[nodeID].getDatabase("mydb").withReadPreference(ReadPreference.secondaryPreferred());
		}
		// get a handle to the "test" collection
		if (collections[nodeID] == null) {
			collections[nodeID] = databases[nodeID].getCollection("test").withReadPreference(ReadPreference.secondaryPreferred());
		}
		System.out.println("bootstrapClient for nodeID=" + nodeID + " completed successfully");
	}	

	private void takedownClient(int nodeID) {
		mongoClients[nodeID].close();
		mongoClients[nodeID] = null;
		databases[nodeID] = null;
    	collections[nodeID] = null;
	}
	
	@Override
	public void takedownClients() {
		System.out.println("takedownClients");
		
		for (int i = 0; i < numNode; i++) {
			mongoClients[i].close();
			mongoClients[i] = null;
			databases[i] = null;
        	collections[i] = null;
		}
        
		//try { Thread.sleep(31000); } catch (InterruptedException e) { e.printStackTrace(); }
		
	}
	
	private int getPid(int nodeID) {
		int returnPid = -1;
		// wait until the pid file for the process associated with the given nodeID to be created
        while (true) {
	        File logDirFile = new File(workingDir + "/data/" + nodeID + "/mongod.pid");
			if (logDirFile.exists()) {
				System.out.println("ok, pid file for nodeID=" + nodeID 
						+ " is created. Now we read pid from the mongod.pid file!");
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
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
        }
		return returnPid;
	}

	@Override
	public void startEnsemble() {
		System.out.println("startEnsemble");
		ProcessBuilder builder = new ProcessBuilder();
		builder.directory(new File(workingDir));
		for (int i = 0; i < numNode; ++i) {
			String[] rmLogCmd = { "rm", "-f", workingDir + "/log/" + i + "/mongod.log"};
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
			String[] mongodCmd = {"%s/mongod", "-f", "%s"};
			mongodCmd[0] = String.format(mongodCmd[0], sutSrcDir);
			mongodCmd[2] = String.format(mongodCmd[2], workingDir + "/conf/" + i + "/mongod.conf");
			System.out.println("Starting node " + i);
	        try {
	        	System.out.println("Invoking command=\n" + String.join(" ", mongodCmd));
				nodeProc[i] = builder.command(mongodCmd).start();
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
	        int newPid = getPid(i);
	        pidMap[i] = newPid;
		}
		
		//try { Thread.sleep(10000); } catch (InterruptedException e) { e.printStackTrace(); }
		int counter = countMongodProcesses();
		while (counter != numNode) {
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			counter = countMongodProcesses();
		}
		
		initializeReplicaSet();
		//System.out.println("after starting ensemble we begin to wait for 60000 sec (100 min)");
		// try { Thread.sleep(31000); } catch (InterruptedException e) { e.printStackTrace(); }
		//System.out.println("after starting ensemble we are done with waiting");
		Arrays.fill(isNodeOnline, true);

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
		System.out.println("");
		
		
		// reconfig 
		reconfigOptionalParams();
		
	}

	private void reconfigOptionalParams() {
		
		// disable chainingAllowed
		MongoClient tmpClient = new MongoClient("127.0.0.1:" + basePort); //(basePort + numNode - 1));
		MongoDatabase adminDB = tmpClient.getDatabase("admin"); // gotta be admin to run the replication command below
		Document rsGetConfigDoc = new Document("replSetGetConfig", 1);
		Document rsConfigDoc = (Document) (runCommandWrapper(adminDB, rsGetConfigDoc).get("config"));
		System.out.println("Received replSetConfig as follow=" + rsConfigDoc.toJson());
		Document rsSettings = (Document) rsConfigDoc.get("settings");
		System.out.println("rsSettings are=" + rsSettings);
		rsSettings.put("chainingAllowed", false);
		rsConfigDoc.put("version", (rsConfigDoc.getInteger("version") + 1));
		System.out.println("Reconfig as follow=" + rsConfigDoc.toJson());
		//adminDB.runCommand(new Document("replSetReconfig", rsConfigDoc));
		runCommandWrapper(adminDB, new Document("replSetReconfig", rsConfigDoc));
		System.out.println("Reconfig is done");
		//sanity check
		rsConfigDoc = (Document) (runCommandWrapper(adminDB, rsGetConfigDoc).get("config"));
		System.out.println("SanityCheck. replSetConfig is reconfiged as follow=" + rsConfigDoc.toJson());
	}
	
	@Override
	public void stopEnsemble() {
		System.out.println("stopEnsemble");
		for (Process node : nodeProc) {
            node.destroy();
        }
        //for (Process node : nodeProc) {
        //    try {
        //        node.waitFor();
        //    } catch (InterruptedException e) {
        //    	e.printStackTrace();
        //    	System.exit(1);
        //    }
        //}
        for (int i = 0; i < numNode; i++) {
        	nodeProc[i] = null;
        }
        System.gc();
        
		// try { Thread.sleep(31000); } catch (InterruptedException e) { e.printStackTrace(); }

		// explicitly kill each process
		for (int i = 0; i < numNode; i++) {
			killNode(i);
		}
		
        Arrays.fill(isNodeOnline, false);
	}

	@Override
	public void startNode(int id) {
		System.out.println("startNode " + id);
		ProcessBuilder builder = new ProcessBuilder();
		builder.directory(new File(workingDir));
		
		String[] rmLogCmd = { "rm", "-f", workingDir + "/log/" + id + "/mongod.log" };
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
		String[] mongodCmd = {"%s/mongod", "-f", "%s"};
		mongodCmd[0] = String.format(mongodCmd[0], sutSrcDir);
		mongodCmd[2] = String.format(mongodCmd[2], workingDir + "/conf/" + id + "/mongod.conf");
		System.out.println("Starting node " + id);
        try {
        	System.out.println("Invoking command=\n" + String.join(" ", mongodCmd));
			nodeProc[id] = builder.command(mongodCmd).start();
			System.out.println("node is started");
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		isNodeOnline[id] = true;
		int newPid = getPid(id);
        pidMap[id] = newPid;
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
			
			// run ps command to see if the mongod process is still running
			ProcessBuilder psBuilder = new ProcessBuilder();
			String[] psCmd = { "/bin/sh", "-c", "ps aux | grep mongod | grep -v grep | grep -v ps | awk '{print $2}' | grep \" %d \"" };
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
		
		// additionally, delete the pid file as well
        ProcessBuilder builder = new ProcessBuilder();
        builder.directory(new File(workingDir));
        String[] rmPidFileCmd = { "rm", "-f", workingDir + "/data/" + nodeID + "/mongod.pid"};
        try {
        	System.out.println("Invoking cmd=\n" + String.join(" ", rmPidFileCmd));
            Process p = builder.command(rmPidFileCmd).start();
            p.waitFor();
            //Thread.sleep(310000);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}
	
	@Override
	public void stopNode(int id) {
		System.out.println("stopNode " + id);
		nodeProc[id].destroy();
		//try {
		//	nodeProc[id].waitFor();
		//} catch (InterruptedException e) {
		//	e.printStackTrace();
		//}
		isNodeOnline[id] = false;
		
		killNode(id);
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
            //Thread.sleep(31000);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(1);
		}
        
        System.gc();
        
        // wait until there is no log directory
        while (true) {
	        File logDirFile = new File(workingDir + "/log");
			//System.out.println("checking directory for nodes' log files");
			if (!logDirFile.exists()) {
				System.out.println("verified that log directory is removed!");
				break;
			}
        }
		// wait until there is no data directory
        while (true) {
	        File dataDirFile = new File(workingDir + "/data");
			//System.out.println("checking directory for nodes' data files");
			if (!dataDirFile.exists()) {
				System.out.println("verified that data directory is removed!");
				break;
			}
        }
        
        //try {	Thread.sleep(6000000); } catch(InterruptedException e) { e.printStackTrace(); }
	}
	
	private int countMongodProcesses() {
		int processCounter = 0;
		ProcessBuilder psBuilder = new ProcessBuilder();
		//String[] psCmd = { "ps", "aux", "|", "grep", "mongo" };
		//String[] psCmd = { "ps", "aux" };
		String[] psCmd = {"/bin/sh", "-c", "ps aux | grep mongod | grep -v grep | grep -v ps"};
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
				if (line.contains("mongod -f")) {
					processCounter++;
				}
			}
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String result = builder.toString();
		System.out.println("initializeReplicaSet() result String=\n" + result);
		return processCounter;
	}
	
	private Document runCommandWrapper(MongoDatabase targetDB, Document commandDoc) {
		while(true) {
			try {
				Document retDoc = null;
				retDoc = (Document) targetDB.runCommand(commandDoc);
				return retDoc;
			} catch (MongoCommandException ex) {
				// retry after sleeping 3 sec
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (MongoTimeoutException ex) {
				// retry after sleeping 3 sec
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	private void initializeReplicaSet() {
		// assertion check
		
		// initialize
		MongoClient tmpClient = new MongoClient("127.0.0.1:" + basePort); //(basePort + numNode - 1));
		MongoDatabase adminDB = tmpClient.getDatabase("admin"); // gotta be admin to run the replication command below
		Document rsInitConfigDoc = new Document("_id", "repl1");
		ArrayList<Document> memberListDoc = new ArrayList<Document>(); 
		for (int i = 0; i < numNode; i++) {
			Document memberDoc = new Document("_id", i);
			memberDoc.append("host", "127.0.0.1:" + (basePort + i)).append("tags", new Document("nodeID", "" + i));
			memberListDoc.add(memberDoc);
		}
		rsInitConfigDoc.put("members", memberListDoc);
		runCommandWrapper(adminDB, 
						new Document("replSetInitiate", rsInitConfigDoc));
		
		tmpClient.close();
	}

	@Override
	public byte[] readData(int nodeID, String key) {
		takedownClient(nodeID);
		bootstrapClient(nodeID);
		MongoCollection<Document> colec = collections[nodeID];
		System.out.println("Try to read data from node=" + nodeID + " for the key=" + key);
		
		//MongoCursor<Document> tmpDocIter = colec.find().iterator();
		//while(tmpDocIter.hasNext()) {
		//	Document docToken = tmpDocIter.next();
		//	System.out.println("doc=" + docToken.toJson());
		//}
		System.out.println("read preference=" + colec.getReadPreference().toDocument().toJson());
		Block<Document> printBlock = new Block<Document>() {
		       @Override
		       public void apply(final Document document) {
		           System.out.println(document.toJson());
		       }
		};
		//FindIterable<Document> iter = colec.find(eq("_id", key));
		MongoCursor<Document> cursor = null;
		int retryCnt = 0;
		while (true) {
			try {
				cursor = colec.find(eq("_id", key)).iterator();
				break;
			} catch (MongoSocketReadException msre) {
				System.err.println("MongoSocketReadException caught. Bootstrap the client again and retry");
				takedownClient(nodeID);
				bootstrapClient(nodeID);
			} catch (MongoNotPrimaryException mnpe) {
				System.err.println("MongoNotPrimaryException caught. Bootstrap the client again and retry");
				takedownClient(nodeID);
				bootstrapClient(nodeID);
			} catch (IllegalStateException ise) {
				System.err.println("IllegalStateException caught. Bootstrap the client again and retry");
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
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		System.out.println("forEach document found.. print:");
		Document myDoc = null;
		if (!cursor.hasNext()) {
			System.out.println("wierd cursor returned has no document..");
			myDoc = new Document("value", "null");
		} else {
			//iter.forEach(printBlock);
			//Document myDoc = iter.first();
			myDoc = cursor.next();
		}
		System.out.println("cursor srvaddress:" + cursor.getServerAddress().toString());
		System.out.println();

		
		String valStr = myDoc.getString("value");
		System.out.println("readData read value (string)=" + valStr);
		Document eqFilterDoc = new Document("_id", key);
		Document explainFindDoc = new Document("find", "test").append("filter", eqFilterDoc);
		Document explainDoc = new Document("explain", explainFindDoc);
		Document stats = databases[nodeID].runCommand(explainDoc, TaggableReadPreference.secondaryPreferred(new TagSet(new Tag("nodeID", "" + nodeID))));
		System.out.println("stats=" + stats.toJson());
		
		//try {	Thread.sleep(6000000); } catch (InterruptedException e) { e.printStackTrace(); }
		
		return valStr.getBytes();
		// return null;
	}

	@Override
	public void createData(int nodeID, String key, int value) {
		Document doc = new Document("_id", key).append("value", "" + value);
		try {
        	//collections[nodeID].insertOne(doc);
			//collections[actualLeader].insertOne(doc);
			MongoClientOptions options = MongoClientOptions.builder().writeConcern(WriteConcern.JOURNALED).build();
			ArrayList<ServerAddress> svrList = new ArrayList<ServerAddress>();
			for (int i = 0; i < numNode; i++) {
				svrList.add(new ServerAddress("127.0.0.1", basePort + i));
			}
			MongoClient mongoClient = new MongoClient(svrList, options);
			mongoClient.getDatabase("mydb").getCollection("test").insertOne(doc);
			mongoClient.close();
        } catch (MongoWriteException mwe) {
        	System.err.println("MongoWriteException occurred");
        	mwe.printStackTrace();
        } catch (MongoWriteConcernException mwce) {
        	System.err.println("MongoWriteConcernException occurred");
        	mwce.printStackTrace();
        } catch (MongoException me) {
        	System.err.println("MongoException occurred");
        	me.printStackTrace();
        }
        System.out.println("createData to client=" + nodeID + " key=" + key + " value=" + value);
        //MongoCursor<Document> iter = collections[nodeID].find().iterator();
		//System.out.println("Contains following docs");
		//while (iter.hasNext()) {
		//	System.out.println("doc=" + iter.next().toJson());
		//}
		//iter.close();
        //try {
        //	if (key.equals("/testDivergenceResync4")) {
        //		Thread.sleep(6000000);
        //	}
		//} catch (InterruptedException e) {
		//	// TODO Auto-generated catch block
		//	e.printStackTrace();
		//}
        
        //try {	Thread.sleep(6000000); } catch (InterruptedException e) { e.printStackTrace(); }
	}

	@Override
	public void writeData(int nodeID, String key, int value) {
		
		// Original BEGIN
		/*
		// update
		// We prefer to send write request to the leader.
		boolean finishWriting = false;
		int retryCnt = 0;
		while (true) {
			try {
				collections[actualLeader].updateOne(eq("_id", key), set("value", "" + value));
			} catch (MongoSocketWriteException mswe) {
				System.err.println("MongoSocketWriteException caught in writeData. Bootstrap the client again and retry");
				retryCnt++;
				if (retryCnt > 1) {
					System.out.println("We have retried several times. Now give up.");
					finishWriting = true;
					break;
				}
				takedownClient(actualLeader);
				bootstrapClient(actualLeader);
			} catch (MongoSocketReadException msre) {
				System.err.println("MongoSocketReadException caught in writeData. Bootstrap the client again and retry");
				retryCnt++;
				if (retryCnt > 1) {
					System.out.println("We have retried several times. Now give up.");
					finishWriting = true;
					break;
				}
				takedownClient(actualLeader);
				bootstrapClient(actualLeader);
			} catch (MongoTimeoutException mte) {
				System.err.println("MongoTimeoutException caught in writeData. Bootstrap the client again and retry");
				retryCnt++;
				if (retryCnt > 1) {
					System.out.println("We have retried several times. Now give up.");
					finishWriting = true;
					break;
				}
				takedownClient(actualLeader);
				bootstrapClient(actualLeader);
			}
			if (finishWriting) {
				break;
			}
		}
		System.out.println("writeData key=" + key + " value=" + value);
		*/
		// Original END
		
		// Multiple Big writes BEGIN
		String paddingCnt = "";
		for (int i = 0; i < 1000; i++) {
			paddingCnt += "0000000000";
		}
		boolean finishWriting = false;
		for (int i = 0; i < 10; i++) {
			int retryCnt = 0;
			while (true) {
				try {
					String valueStr = value + "." + i + "." + paddingCnt + "." + i;
					collections[actualLeader].updateOne(eq("_id", key), set("value", valueStr));
					System.out.println("writeData key=" + key + " value=" + valueStr);
					break;
				} catch (MongoSocketWriteException mswe) {
					System.err.println("MongoSocketWriteException caught in writeData. Bootstrap the client again and retry");
					retryCnt++;
					if (retryCnt > 1) {
						System.out.println("We have retried several times. Now give up.");
						finishWriting = true;
						break;
					}
					takedownClient(actualLeader);
					bootstrapClient(actualLeader);
				} catch (MongoSocketReadException msre) {
					System.err.println("MongoSocketReadException caught in writeData. Bootstrap the client again and retry");
					retryCnt++;
					if (retryCnt > 1) {
						System.out.println("We have retried several times. Now give up.");
						finishWriting = true;
						break;
					}
					takedownClient(actualLeader);
					bootstrapClient(actualLeader);
				} catch (MongoTimeoutException mte) {
					System.err.println("MongoTimeoutException caught in writeData. Bootstrap the client again and retry");
					retryCnt++;
					if (retryCnt > 1) {
						System.out.println("We have retried several times. Now give up.");
						finishWriting = true;
						break;
					}
					takedownClient(actualLeader);
					bootstrapClient(actualLeader);
				}
			}
			if (finishWriting) {
				break;
			}
		}
		// Multiple Big writes END
		
		
		//try {	Thread.sleep(6000000); } catch (InterruptedException e) { e.printStackTrace(); }
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
			for (int i = 0; i < commitNodes.size(); i++) {
				boolean foundMessage = false;
				foundMessage = findCommitMessage(commitNodes.get(i), key, "" + val);
				while (!foundMessage) {
					try {
						Thread.sleep(50);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					foundMessage = findCommitMessage(commitNodes.get(i), key, "" + val);
				}
			}
		} else {
			System.err.println("ERROR: unknown commit wait mode");
		}
		return true;
	}

	@Override
	protected boolean waitForResync(int[] restartNodeIDs) {
		System.out.println("waitForResync nodes=" + Arrays.toString(restartNodeIDs));
		
		// set defaultHeartbeatTimeoutSecs back to the default value
		int onlineNodeID = 0;
		for (int i = 0; i < numNode; i++) {
			if (isNodeOnline[i]) {
				onlineNodeID = i;
				break;
			}
		}
		MongoClient tmpClient = new MongoClient("127.0.0.1:" + (basePort + onlineNodeID));
		MongoDatabase adminDB = tmpClient.getDatabase("admin"); // gotta be admin to run the replication command below
		Document rsGetConfigDoc = new Document("replSetGetConfig", 1);
		Document rsConfigDoc = (Document) (runCommandWrapper(adminDB, rsGetConfigDoc).get("config"));
		System.out.println("Received replSetConfig as follow=" + rsConfigDoc.toJson());
		Document rsSettings = (Document) rsConfigDoc.get("settings");
		int currentHBTimeoutSecs = rsSettings.getInteger("heartbeatTimeoutSecs");
		if (currentHBTimeoutSecs != defaultHeartbeatTimeoutSecs) {
			rsSettings.put("heartbeatTimeoutSecs", defaultHeartbeatTimeoutSecs);
			rsConfigDoc.put("settings", rsSettings);
			rsConfigDoc.put("version", (rsConfigDoc.getInteger("version") + 1));
			System.out.println("Reconfig as follow=" + rsConfigDoc.toJson());
			runCommandWrapper(adminDB, (new Document("replSetReconfig", rsConfigDoc).append("force", "true")));
			Document rsConfigDocReconfiged = (Document) (runCommandWrapper(adminDB, rsGetConfigDoc).get("config"));
			System.out.println("Reconfigured replSetConfig is=" + rsConfigDocReconfiged.toJson());
		}
		Document rsGetStatusDoc = new Document("replSetGetStatus", 1);
		Document rsStatusDoc = (Document) (runCommandWrapper(adminDB, rsGetStatusDoc));
		System.out.println("Received replSetStatus is as follow=" + rsStatusDoc.toJson());

		tmpClient.close();
		
		if (rwmMode.equals(ResyncWaitMode.RWM_SLEEP)) {
			System.out.println("resync wait using sleep");
			try {
				Thread.sleep(waitForResyncDuration);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} else if (rwmMode.equals(ResyncWaitMode.RWM_PROBE)) {
			
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

			// need to make sure all nodes participating in resync are online.
			tmpClient = new MongoClient("127.0.0.1:" + (basePort + actualLeader));
			adminDB = tmpClient.getDatabase("admin"); // gotta be admin to run the replication command below
			rsGetStatusDoc = new Document("replSetGetStatus", 1);
			rsStatusDoc = null;
			List<Document> members = null;
			for (int i = 0; i < restartNodeIDs.length; i++) {
				boolean verifiedNodeOnline = false;
				while (!verifiedNodeOnline) {
					rsStatusDoc = runCommandWrapper(adminDB, rsGetStatusDoc);
					members = (List<Document>) rsStatusDoc.get("members");
					Document memDoc = null;
					for (int j = 0; j < numNode; j++) {
						memDoc = members.get(j);
						//System.out.println("\n memDoc=" + memDoc.toJson());
						int nodeID = memDoc.getInteger("_id"); 
						if (nodeID == restartNodeIDs[i]) {
							System.out.println("Found the member entry for the node=" + nodeID);
							break;
						}
					}
					double nodeHealth = memDoc.getDouble("health");
					System.out.println("node=" + i + " health=" + nodeHealth);
					if (nodeHealth != 1.0) {
						System.out.println("node=" + i + " is still not on.. health=" + nodeHealth);
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					} else {
						System.out.println("node=" + i + " is now on.. health=" + nodeHealth);
						verifiedNodeOnline = true;
					}
				}
			}
			tmpClient.close();
			
			int i = 0;
			while (true) {
				System.out.println("---- syncing wait " + (i++) + "-th times ----");
				
				tmpClient = new MongoClient("127.0.0.1:" + (basePort + actualLeader));
				adminDB = tmpClient.getDatabase("admin"); // gotta be admin to run the replication command below
				rsGetStatusDoc = new Document("replSetGetStatus", 1);
				rsStatusDoc = runCommandWrapper(adminDB, rsGetStatusDoc);
				System.out.println("Received replSetStatus as follow=" + rsStatusDoc.toJson());
				
				/*
				// RESYNC SPEC 1: Wait For Resync using State BEGIN 
				members = (List<Document>) rsStatusDoc.get("members");
				boolean[] isSynced = new boolean[numNode];
				Arrays.fill(isSynced, false);
				for (int j = 0; j < numNode; j++) {
					Document memDoc = members.get(j);
					//System.out.println("\n memDoc=" + memDoc.toJson());
					int nodeID = memDoc.getInteger("_id"); 
					double nodeHealth = memDoc.getDouble("health");
					if (nodeHealth > 0) {
						String stateStr = (String) memDoc.get("stateStr");
						if (stateStr.equals("PRIMARY") || stateStr.equals("SECONDARY")) {
							isSynced[nodeID] = true;
						} 
					} else {
						// we consider offline node is already synced as it cannot affect resync result
						isSynced[nodeID] = true; 
					}
				}
				//System.out.println("\n isSynced array=" + Arrays.toString(isSynced));
				boolean resyncCheckResult = true;
				for (int j = 0; j < numNode; j++) {
					resyncCheckResult &= isSynced[j];
				}
				// Wait For Resync using State END
				*/
				
				
				/*
				// RESYNC SPEC 2: Wait For Resync using PRIMARY's optime as the lastCommittedOpTime BEGIN
				
				// extract lastCommittedOpTime
				//Document opTimesDoc = (Document) rsStatusDoc.get("optimes");
				////System.out.println("\n opTimesDoc=" + opTimesDoc.toJson());
				//Document lastCommittedOpTimeDoc = (Document) opTimesDoc.get("lastCommittedOpTime");
				////System.out.println("\n lastCommittedOpTimeDoc=" + lastCommittedOpTimeDoc.toJson());
				
				// loop and check if every memeber's ts is same as what we extracted
				members = (List<Document>) rsStatusDoc.get("members");
				
				BsonTimestamp lastCommittedOpTime = null;
				for (int j = 0; j < numNode; j++) {
					Document memDoc = members.get(j);
					double nodeHealth = memDoc.getDouble("health");
					if (nodeHealth > 0) {
						String stateStr = (String) memDoc.get("stateStr");
						if (stateStr.equals("PRIMARY")) {
							lastCommittedOpTime = (BsonTimestamp) memDoc.get("optime");
						} 
					}
				}
				if (lastCommittedOpTime == null) {
					System.err.println("Very wierd. Couldn't extract optime of primary");
					System.exit(1);
				} else {
					System.out.println("\n lastCommittedOpTimeDoc=" + lastCommittedOpTime.toString());
				}
				
				boolean[] isSynced = new boolean[numNode];
				Arrays.fill(isSynced, false);
				for (int j = 0; j < numNode; j++) {
					Document memDoc = members.get(j);
					//System.out.println("\n memDoc=" + memDoc.toJson());
					int nodeID = memDoc.getInteger("_id"); 
					double nodeHealth = memDoc.getDouble("health");
					if (nodeHealth > 0) {
						BsonTimestamp nodeOpTime = (BsonTimestamp) memDoc.get("optime");
						//System.out.println("\n nodeOpTimeDoc=" + nodeOpTimeDoc.toJson());
						//System.out.println("nodeID=" + nodeID + " lastCommittedOpTimeDoc=" + lastCommittedOpTimeDoc.toJson()
						//		+ " nodeOpTimeDoc=" + nodeOpTimeDoc.toJson());
						if (lastCommittedOpTime.compareTo(nodeOpTime) <= 0) {
							isSynced[nodeID] = true;
						} 
					} else {
						// we consider offline node is already synced as it cannot affect resync result
						isSynced[nodeID] = true; 
					}
				}
				//System.out.println("\n isSynced array=" + Arrays.toString(isSynced));
				boolean resyncCheckResult = true;
				for (int j = 0; j < numNode; j++) {
					resyncCheckResult &= isSynced[j];
				}
				
				// Wait For Resync using PRIMARY's optime as the lastCommittedOpTime END
				*/
				
				// RESYNC SPEC 3: Wait For Resync using highest optime as the lastCommittedOpTime BEGIN
				
				// extract lastCommittedOpTime
				//Document opTimesDoc = (Document) rsStatusDoc.get("optimes");
				////System.out.println("\n opTimesDoc=" + opTimesDoc.toJson());
				//Document lastCommittedOpTimeDoc = (Document) opTimesDoc.get("lastCommittedOpTime");
				////System.out.println("\n lastCommittedOpTimeDoc=" + lastCommittedOpTimeDoc.toJson());
				
				// loop and check if every memeber's ts is same as what we extracted
				members = (List<Document>) rsStatusDoc.get("members");
				
				BsonTimestamp lastCommittedOpTime = new BsonTimestamp(0);
				for (int j = 0; j < numNode; j++) {
					Document memDoc = members.get(j);
					double nodeHealth = memDoc.getDouble("health");
					if (nodeHealth > 0) {
						BsonTimestamp nextTimestamp = (BsonTimestamp) memDoc.get("optime");
						if (lastCommittedOpTime.compareTo(nextTimestamp) < 0) {
							lastCommittedOpTime = nextTimestamp;
						}
					}
				}
				System.out.println("lastCommittedOpTime=" + lastCommittedOpTime);
				
				boolean[] isSynced = new boolean[numNode];
				Arrays.fill(isSynced, false);
				for (int j = 0; j < numNode; j++) {
					Document memDoc = members.get(j);
					//System.out.println("\n memDoc=" + memDoc.toJson());
					int nodeID = memDoc.getInteger("_id"); 
					double nodeHealth = memDoc.getDouble("health");
					if (nodeHealth > 0) {
						BsonTimestamp nodeOpTime = (BsonTimestamp) memDoc.get("optime");
						//System.out.println("\n nodeOpTimeDoc=" + nodeOpTimeDoc.toJson());
						//System.out.println("nodeID=" + nodeID + " lastCommittedOpTimeDoc=" + lastCommittedOpTimeDoc.toJson()
						//		+ " nodeOpTimeDoc=" + nodeOpTimeDoc.toJson());
						if (lastCommittedOpTime.compareTo(nodeOpTime) <= 0) {
							isSynced[nodeID] = true;
						} 
					} else {
						// we consider offline node is already synced as it cannot affect resync result
						isSynced[nodeID] = true; 
					}
				}
				System.out.println("\n isSynced array=" + Arrays.toString(isSynced));
				boolean resyncCheckResult = true;
				for (int j = 0; j < numNode; j++) {
					resyncCheckResult &= isSynced[j];
				}
				
				// RESYNC SPEC 3: Wait For Resync using highest optime as the lastCommittedOpTime END
				
				if (resyncCheckResult) {
					System.out.println("Verified all online nodes are resync'ed");
					
					tmpClient.close();
					break;
				} else if (i > 6600) {
					System.out.println("Retry Count is more than 6600. Give up waiting for resync to be finished..");
					
					break;
				} else {
					System.out.println("Resync is still not finished.. wait longer");
				}
				tmpClient.close();
				
				try {	Thread.sleep(100); } catch (InterruptedException e) { e.printStackTrace(); }
				
			}
		} else {
			System.err.println("ERROR: unknown resync wait mode");
		}
		
		
		//try {	Thread.sleep(6000000); } catch (InterruptedException e) { e.printStackTrace(); }
		
		return true;
	}
	
	@Override
	public void beforeDivergencePath() {
		// Because MongoDB will step down the primary if there are not enough nodes to form the quorum
		// , it is safer to set longer heartbeat here to be more deterministic. Otherwise, the write
		// to the leftover nodes will not be successful because the primary may no longer exist.
	
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
		System.out.println("[beforeDivergencePath] found the leader=" + leaderID);
		
		MongoClient tmpClient = new MongoClient("127.0.0.1:" + (basePort + leaderID));
		MongoDatabase adminDB = tmpClient.getDatabase("admin"); // gotta be admin to run the replication command below
		Document rsGetConfigDoc = new Document("replSetGetConfig", 1);
		Document rsConfigDoc = (Document) (runCommandWrapper(adminDB, rsGetConfigDoc).get("config"));
		System.out.println("Received replSetConfig as follow=" + rsConfigDoc.toJson());
		Document rsSettings = (Document) rsConfigDoc.get("settings");
		System.out.println("rsSettings are=" + rsSettings);
		//defaultHeartbeatTimeoutSecs = (int) rsSettings.get("heartbeatTimeoutSecs");
		//defaultHeartbeatTimeoutSecs = (int) rsConfigDoc.get("settings.heartbeatTimeoutSecs");
		
		rsSettings.put("heartbeatTimeoutSecs", augmentedHeartbeatTimeoutSecs);
		rsConfigDoc.put("settings", rsSettings);
		//rsConfigDoc.put("settings.heartbeatTimeoutSecs", augmentedHeartbeatTimeoutSecs);
		rsConfigDoc.put("version", (rsConfigDoc.getInteger("version") + 1));
		System.out.println("Reconfig as follow=" + rsConfigDoc.toJson());
		runCommandWrapper(adminDB, new Document("replSetReconfig", rsConfigDoc));
		
		//sanity check
		rsConfigDoc = (Document) (runCommandWrapper(adminDB, rsGetConfigDoc).get("config"));
		System.out.println("SanityCheck. replSetConfig is reconfiged as follow=" + rsConfigDoc.toJson());
		
		tmpClient.close();
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
		// TODO Auto-generated method stub
		
	}
	
}