package strata.common;

import java.util.Arrays;

public class AbstractState {
	public int numAsyncOp;
	public int[] nodeState;
	public boolean[] onlineStatus;
	public int[] syncSources;
	/* syncTargets: for each index, we store the integer of the number of digits that is same as 
	 * the number of nodes. 
	 * Each integer at each index represents to which nodes the corresponding node at the indes
	 * is serving as the sync source. 
	 * e.g., for n-nodes, for the index 0 containing 4(0b0100), it represents the situation where the
	 * node 0 is being the sync source for the sync target node 2.
	 * In addition, for the index 1 containing 5(0b0101), it represents the situation where the node 1
	 * is being the sync source for the sync target node 0 and 2.
	 **/ 
	public int[] syncTargets; 
	
	public AbstractState(int naop, int nn) {
		this.numAsyncOp = naop;
		this.nodeState = new int[nn];
		this.onlineStatus = new boolean[nn];
		this.syncSources = new int[nn];
		this.syncTargets = new int[nn];
		Arrays.fill(nodeState, 0);
		Arrays.fill(onlineStatus, true);
		Arrays.fill(syncSources, -1);
		Arrays.fill(syncTargets, 0);
	}
	
	public AbstractState (int naop, int[] ns, boolean[] os) {
		this.numAsyncOp = naop;
		this.nodeState = ns;
		this.onlineStatus = os;
		this.syncSources = new int[ns.length];
		this.syncTargets = new int[ns.length];
		Arrays.fill(syncSources, -1);
		Arrays.fill(syncTargets, 0);
	}
	
	public AbstractState (int naop, int[] ns, boolean[] os, int[] ss, int[] st) {
		this.numAsyncOp = naop;
		this.nodeState = ns;
		this.onlineStatus = os;
		this.syncSources = ss;
		this.syncTargets = st;
	}
	
	public String toString() {
		String retStr = "AbstractState ";
		retStr += "numAsyncOp=" + numAsyncOp + " ";
		retStr += "nodeState=" + Arrays.toString(nodeState) + " ";
		retStr += "onlineState=" + Arrays.toString(onlineStatus) + " ";
		retStr += "syncSources=" + Arrays.toString(syncSources) + " ";
		retStr += "syncTargets=" + Arrays.toString(syncTargets);
		return retStr;
	}
}