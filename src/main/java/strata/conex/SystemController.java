package strata.conex;

import java.util.ArrayList;

public abstract class SystemController {
	
	protected int numNode;
	protected String workingDir;
	protected Process[] nodeProc; 
	
	public SystemController(int numNode, String workingdir) {
		this.numNode = numNode;
		this.workingDir = workingdir;
		this.nodeProc = new Process[numNode];
	}
	
	abstract public void sysCtrlParseConfigInit(String configFile);
	abstract public void prepareTestingEnvironment();
	abstract public int findLeader();
	abstract public void bootstrapClients();
	abstract public void takedownClients();
	abstract public void startEnsemble();
	abstract public void stopEnsemble();
	abstract public void startNode(int id);
	abstract public void stopNode(int id);
	abstract public void resetTest();
	abstract public byte[] readData(int nodeID, String key);
	abstract public void createData(int nodeID, String key, int value);
	abstract public void writeData(int nodeID, String key, int value);
	abstract public int[] sortTargetState(int[] targetState);
	abstract protected boolean waitForCommit(ArrayList<Integer> commitNodes, String key, int val);
	abstract protected boolean waitForResync(int[] restartNodeIDs);
	abstract public void beforeDivergencePath();
	abstract public void afterDivergencePath();
	abstract public void beforeResyncPath();
	abstract public void afterResyncPath();
	abstract public void waitBeforeVerification();
	
}
