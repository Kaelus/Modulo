package strata.common;

import java.util.Arrays;

public class ResyncPath extends Path {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2426957171781398849L;

	public int[] devNode;
	final static int resyncIncrease = 10000;
	
	public ResyncPath() {
		type = Path.PathType.RESYNC;
	}
	
	public ResyncPath(int[] dn) {
		type = Path.PathType.RESYNC;
		devNode = dn;
	}
	
	public void setDevNode(int[] dn) {
		this.devNode = dn;
	}
	
	public int[] getDevNode() {
		return devNode;
	}
	
	protected static int getHighestNodeState(int[] nodeState) {
		int highest = 0;
		for (int i = 0; i < nodeState.length; i++) {
			if (highest < nodeState[i]) {
				highest = nodeState[i];
			}
		}
		return highest;
	}
	
	protected static int numNodeOnline(boolean[] onlineStatus) {
		int countOnline = 0;
		for (boolean b : onlineStatus) {
			if (b) {
				countOnline++;
			}
		}
		return countOnline;		
	}
	
	public boolean apply(AbstractState curState) {
		int prevNumOnline = numNodeOnline(curState.onlineStatus);
		
		/** update onlineStatus */
		for (int nid : devNode) {
			curState.onlineStatus[nid] = true;
		}
		/** update nodeState */
		int quorumSize = curState.onlineStatus.length/2 + 1;
		int currNumNodeOnline = numNodeOnline(curState.onlineStatus);
		int[] participants = new int[currNumNodeOnline];
		int[] participantNodeState = new int[currNumNodeOnline];
		//System.out.println("participants are=");
		int j = 0;
		for (int i = 0; i < curState.onlineStatus.length; i++) {
			if (curState.onlineStatus[i]) {
				participantNodeState[j] = curState.nodeState[i];
				participants[j++] = i;	
				//System.out.println("" + i);
			}
		}
		int highestState = getHighestNodeState(participantNodeState);
		//System.out.println("highestState=" + highestState);
		int resyncState = 0;
		if (prevNumOnline < quorumSize) { //NOTE: prevNumOnline is based on the previous onlineStatus before upating
			resyncState = ((highestState / resyncIncrease) + 1) * resyncIncrease;
		} else {
			resyncState = highestState;
		}
		for (int i = 0; i < participants.length; i++) {
			curState.nodeState[participants[i]] = resyncState;
		}
		return true;
	}
	
	public String toString() {
		String retStr = "";
		return super.toString() + " devNode=" + Arrays.toString(devNode);
	}
	
	public int hashCode() {
		int prime = 17;
		int result = 1;
		result += result*prime + type.hashCode();
		result += result*prime + Arrays.hashCode(devNode);
		return  result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ResyncPath other = (ResyncPath) obj;
		if (devNode == null) {
			if (other.devNode != null)
				return false;
		} else if (!Arrays.equals(devNode, other.devNode)) 
			return false;
		return true;
	}
}
