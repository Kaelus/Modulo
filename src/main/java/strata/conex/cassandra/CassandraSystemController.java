package strata.conex.cassandra;

import java.util.ArrayList;

import strata.conex.SystemController;

public class CassandraSystemController extends SystemController {

	protected final static String caSrcDir = "/home/ben/project/cass-hacking/cassandra"; // HARD-CODED
	protected final static String libClasspath 
		= ":/home/ben/project/cass-hacking/cassandra/build/lib/jars/*"
		+ ":/home/ben/project/cass-hacking/cassandra/lib/*"
		+ ":/usr/lib/jvm/java-8-oracle/lib/tools.jar"
		+ ":/home/ben/project/cass-hacking/cassandra/build/classes/thrift"
		+ ":/home/ben/project/cass-hacking/cassandra/test/conf";
	
	protected String updatedClasspath = null;
	
	final int clientPorts[] = new int[numNode];
	//Cluster[] clusters;
	//Session[] sessions;
	String[] endpoints;
	
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
            "-Dcassandra-foreground=no", "-cp", "%s/conf/%d:" + System.getenv("CLASSPATH"),
        	"-Dlog4j.defaultInitOverride=true",
            "-Did=%d", "org.apache.cassandra.service.CassandraDaemon" }; // HARD-CODED
	
	public CassandraSystemController(int numNode, String workingdir) {
		super(numNode, workingdir);
		//clusters = new Cluster[numNode];
		//sessions = new Session[numNode];
		//endpoints = new String[numNode];
	}
	
	@Override
	public void sysCtrlParseConfigInit(String configFile) {
		// TODO Auto-generated method stub
		
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
		
	}

	@Override
	public void takedownClients() {
		
	}

	@Override
	public void startEnsemble() {
		
	}

	@Override
	public void stopEnsemble() {
		
	}

	@Override
	public void startNode(int id) {
		
	}

	@Override
	public void stopNode(int id) {
		
	}

	@Override
	public void resetTest() {
		
	}

	@Override
	public byte[] readData(int nodeID, String key) {
		return null;
	}

	@Override
	public void createData(int nodeID, String key, int value) {
		
	}

	@Override
	public void writeData(int nodeID, String key, int value) {
		
	}

	@Override
	public int[] sortTargetState(int[] targetState) {
		return null;
	}

	@Override
	protected boolean waitForCommit(ArrayList<Integer> commitNodes, String key, int val) {
		return false;
	}

	@Override
	protected boolean waitForResync(int[] restartNodeIDs) {
		return false;
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
