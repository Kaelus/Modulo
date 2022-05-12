package strata.common;

import java.util.ArrayList;
import java.util.Arrays;

public class ResyncPath extends Path {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2426957171781398849L;

	public int[] devNode;
	public int[] targetSyncSourcesChange;
	public int[] targetSyncTargetsChange;
	final static int resyncIncrease = 10000;
	
	public ResyncPath() {
		type = Path.PathType.RESYNC;
	}
	
	public ResyncPath(int[] dn) {
		type = Path.PathType.RESYNC;
		devNode = dn;
	}
	
	public ResyncPath(int[] dn, int[] tssc, int[] tstc) {
		type = Path.PathType.RESYNC;
		this.devNode = dn;
		this.targetSyncSourcesChange = tssc;
		this.targetSyncTargetsChange = tstc;
	}
	
	public void setDevNode(int[] dn) {
		this.devNode = dn;
	}
	
	public int[] getDevNode() {
		return devNode;
	}
	
	public void setTargetSyncSourcesChange(int[] tssc) {
		this.targetSyncSourcesChange = tssc;
	}
	
	public void setTargetSyncTargetsChange(int[] tstc) {
		this.targetSyncTargetsChange = tstc;
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
		System.out.println("[before applying] curState=" + curState.toString());
		System.out.println("applying resync path=" + this.toString());
		
		/** update onlineStatus */
		for (int nid : devNode) {
			curState.onlineStatus[nid] = true;
		}
		for (int i = 0; i < curState.nodeState.length; i++) {
			ArrayList<Integer> chain = new ArrayList<Integer>();
			int currID = i;
			boolean containResNode = false;
			while (true) {
				chain.add(currID);
				if (!containResNode) {
					for (int nid : devNode) {
						if (nid == currID) {
							containResNode = true;
						}
					}
				}
				if (curState.syncSources[currID] == -1) {
					break;
				} else {
					currID = curState.syncSources[currID];
				}
			}
			if (containResNode) {
				for (int nodeToBeOnline : chain) {
					curState.onlineStatus[nodeToBeOnline] = true;
				}
			}
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
		int resyncState = ((highestState / resyncIncrease) + 1) * resyncIncrease;
		for (int i = 0; i < participants.length; i++) {
			curState.nodeState[participants[i]] = resyncState;
		}
		/** update syncSources and syncTargets */
		for (int i = 0; i < curState.syncSources.length; i++) {
			curState.syncSources[i] = this.targetSyncSourcesChange[i];
			curState.syncTargets[i] = this.targetSyncTargetsChange[i];
			if (curState.syncTargets[i] < 0) {
				System.err.println("syncTarget cannot contain a negative integer");
				System.exit(1);
			}
		}
		System.out.println("[after applying] curState=" + curState.toString());
		
		return true;
	}
	
	public String toString() {
		String retStr = "";
		retStr += super.toString() + " devNode=" + Arrays.toString(devNode) + " ";
		retStr += "Sources=" + Arrays.toString(this.targetSyncSourcesChange) + " ";
		retStr += "Targets=" + Arrays.toString(this.targetSyncTargetsChange);
		return retStr;
	}
	
	public int hashCode() {
		int prime = 17;
		int result = 1;
		result += result*prime + type.hashCode();
		result += result*prime + Arrays.hashCode(devNode);
		result += result*prime + Arrays.hashCode(targetSyncSourcesChange);
		result += result*prime + Arrays.hashCode(targetSyncTargetsChange);
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
			if (other.devNode != null) {
				return false;
			}
		} else if (!Arrays.equals(devNode, other.devNode)) {
			return false;
		}
		if (this.targetSyncSourcesChange == null) {
			if (other.targetSyncSourcesChange != null) {
				return false;
			}
		} else if (!Arrays.equals(targetSyncSourcesChange, other.targetSyncSourcesChange)) {
			return false;
		}
		if (this.targetSyncTargetsChange == null) {
			if (other.targetSyncTargetsChange != null) {
				return false;
			}
		} else if (!Arrays.equals(targetSyncTargetsChange, other.targetSyncTargetsChange)) {
			return false;
		}
		return true;
	}
}
