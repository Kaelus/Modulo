package strata.common;

import java.util.Arrays;

public class AbstractState {
	public int numAsyncOp;
	public int[] nodeState;
	public boolean[] onlineStatus;
	
	public AbstractState(int naop, int nn) {
		this.numAsyncOp = naop;
		this.nodeState = new int[nn];
		this.onlineStatus = new boolean[nn];
		Arrays.fill(nodeState, 0);
		Arrays.fill(onlineStatus, true);
	}
	
	public AbstractState (int naop, int[] ns, boolean[] os) {
		this.numAsyncOp = naop;
		this.nodeState = ns;
		this.onlineStatus = os;
	}
	
	public String toString() {
		String retStr = "AbstractState ";
		retStr += "numAsyncOp=" + numAsyncOp + " ";
		retStr += "nodeState=" + Arrays.toString(nodeState) + " ";
		retStr += "onlineState=" + Arrays.toString(onlineStatus);
		return retStr;
	}
}