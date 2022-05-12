package strata.common;

import java.util.Arrays;

public class ResyncPath extends Path {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2426957171781398849L;

	// ONLINE - online resync
	// OFFLINE_1 - offline resync
	// OFFLINE_2 - offline resync and swap the primary
	public enum ResyncType {ONLINE, OFFLINE_1, OFFLINE_2};
	public ResyncType resyncType;
	
	public int[] devNode;
	public int[] targetSyncSourcesChange;
	public int[] targetSyncTargetsChange;
	final static int resyncIncrease = 10000;
	
	public ResyncPath() {
		type = Path.PathType.RESYNC;
		resyncType = ResyncType.ONLINE;
	}
	
	public ResyncPath(int[] dn) {
		type = Path.PathType.RESYNC;
		devNode = dn;
		resyncType = ResyncType.ONLINE;
	}
	
	public ResyncPath(int[] dn, int[] tssc, int[] tstc) {
		type = Path.PathType.RESYNC;
		this.devNode = dn;
		this.targetSyncSourcesChange = tssc;
		this.targetSyncTargetsChange = tstc;
		resyncType = ResyncType.ONLINE;
	}
	
	public ResyncPath(int[] dn, int[] tssc, int[] tstc, ResyncType rt) {
		type = Path.PathType.RESYNC;
		this.devNode = dn;
		this.targetSyncSourcesChange = tssc;
		this.targetSyncTargetsChange = tstc;
		this.resyncType = rt;
	}
	
	public void setDevNode(int[] dn) {
		this.devNode = dn;
	}
	
	public int[] getDevNode() {
		return devNode;
	}
	
	public ResyncType getResyncType() {
		return this.resyncType;
	}
	
	public void setTargetSyncSourcesChange(int[] tssc) {
		this.targetSyncSourcesChange = tssc;
	}
	
	public void setTargetSyncTargetsChange(int[] tstc) {
		this.targetSyncTargetsChange = tstc;
	}

	public void setResyncType(ResyncType rt) {
		this.resyncType = rt;
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
		
		System.out.println("ResyncPath.=" + this.toString());
		System.out.println("curState=" + curState.toString());
		
		if (resyncType.equals(ResyncType.ONLINE)) {
			/** update onlineStatus */
			for (int nid : devNode) {
				curState.onlineStatus[nid] = true;
				curState.failureState[nid] = DeviationPath.FailureType.NONE;
			}
			/** update nodeState */
			//int quorumSize = curState.onlineStatus.length/2 + 1;
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
		} else if (resyncType.equals(ResyncType.OFFLINE_1)) {
			// Assertions:
			// 1. devNode should be of size 1 as we enable OFFLINE resync only 
			//      when there is at least one online node
			// 2. failureState for the devNode[0] should be none-NONE
			// 3. onlineStatus for the devNode[0] should be false
			// 4. exactly one primary should exist (i.e., online node with NONE failure scenario and sync source is -1)
			if (devNode.length != 1) {
				System.err.println("ResyncPath Assert: devNode should be of size 1");
				System.exit(1);
			}
			if (curState.failureState[devNode[0]].equals(DeviationPath.FailureType.NONE)) {
				System.err.println("ResyncPath Assert: failureState should be none-NONE for node=" + devNode[0]);
				System.exit(1);
			}
			if (curState.onlineStatus[devNode[0]]) {
				System.err.println("ResyncPath Assert: onlineStatus should be false for node=" + devNode[0]);
				System.exit(1);
			}
			int primaryID = -1;
			int primaryCnt = 0;
			for (int i = 0; i < curState.onlineStatus.length; i++) {
				if (curState.onlineStatus[i]) {
					if (curState.failureState[i].equals(DeviationPath.FailureType.NONE)) {
						if (curState.syncSources[i] == -1) {
							primaryID = i;
							primaryCnt++;
						}
					}
				}
			}
			if (primaryCnt != 1) {
				System.err.println("ResyncPath Assert: exactly one primary should exist (i.e., online node with NONE failure scenario and sync source is -1)");
				System.err.println("primryID=" + primaryID + " primaryCnt=" + primaryCnt);
				System.err.println("curState" + curState.toString());
				System.err.println("ResyncPath=" + this.toString());
				System.exit(1);
			}
			
			// do followings as an action for offline resync 
			curState.failureState[devNode[0]] = DeviationPath.FailureType.DISCON;
			curState.nodeState[devNode[0]] = curState.nodeState[primaryID];
			
		} else if (resyncType.equals(ResyncType.OFFLINE_2)) {
			// Assertions:
			// 1. devNode should be of size 1 as we enable OFFLINE resync only 
			//      when there is at least one online node
			// 2. failureState for the devNode[0] should be none-NONE
			// 3. onlineStatus for the devNode[0] should be false
			// 4. exactly one primary should exist (i.e., online node with NONE failure scenario and sync source is -1)
			if (devNode.length != 1) {
				System.err.println("ResyncPath Assert: devNode should be of size 1");
				System.exit(1);
			}
			if (curState.failureState[devNode[0]].equals(DeviationPath.FailureType.NONE)) {
				System.err.println("ResyncPath Assert: failureState should be none-NONE for node=" + devNode[0]);
				System.exit(1);
			}
			if (curState.onlineStatus[devNode[0]]) {
				System.err.println("ResyncPath Assert: onlineStatus should be false for node=" + devNode[0]);
				System.exit(1);
			}
			int primaryID = -1;
			int primaryCnt = 0;
			for (int i = 0; i < curState.onlineStatus.length; i++) {
				if (curState.onlineStatus[i]) {
					if (curState.failureState[i].equals(DeviationPath.FailureType.NONE)) {
						if (curState.syncSources[i] == -1) {
							primaryID = i;
							primaryCnt++;
						}
					}
				}
			}
			if (primaryCnt != 1) {
				System.err.println("ResyncPath Assert: exactly one primary should exist (i.e., online node with NONE failure scenario and sync source is -1)");
				System.err.println("primryID=" + primaryID + " primaryCnt=" + primaryCnt);
				System.err.println("curState" + curState.toString());
				System.err.println("ResyncPath=" + this.toString());
				System.exit(1);
			}
			
			// do followings as an action for offline resync 2 
			curState.failureState[devNode[0]] = DeviationPath.FailureType.NONE;
			curState.onlineStatus[devNode[0]] = true;
			curState.nodeState[devNode[0]] = curState.nodeState[primaryID];
			// assert: we should have only one node that is currently online that is the primaryID which currently should not be the part of any replication chain
			// assert: restarted node should not be a part of replication chain
			for (int i = 0; i < curState.onlineStatus.length; i++) {
				if (curState.syncSources[i] == primaryID) {
					System.err.println("Assert: we should have only one node that is currently online that is the primaryID which currently should not be the part of any replication chain");
					System.exit(1);
				}
				if (curState.syncSources[i] == devNode[0] || curState.syncSources[devNode[0]] != -1) {
					System.err.println("Assert: restarted node should not be a part of replication chain");
					System.exit(1);
				}
			}
			curState.failureState[primaryID] = DeviationPath.FailureType.DISCON;
			curState.onlineStatus[primaryID] = false;
			//swapPartitionGroup(curState, primaryID);
		}
		
		System.out.println("curState after applied=" + curState.toString());
		
		return true;
	}
	
	/*
	public void swapPartitionGroup(AbstractState curState, int curNodeID) {
		curState.failureState[curNodeID] = DeviationPath.FailureType.DISCON;
		curState.onlineStatus[curNodeID] = false;
		for (int i = 0; i < curState.onlineStatus.length; i++) {
			if (curState.syncSources[i] == curNodeID) {
				swapPartitionGroup(curState, i);
			}
		}
	}
	*/
	
	public String toString() {
		String retStr = "";
		retStr += super.toString() + " devNode=" + Arrays.toString(devNode) + " ";
		retStr += "Sources=" + Arrays.toString(this.targetSyncSourcesChange) + " ";
		retStr += "Targets=" + Arrays.toString(this.targetSyncTargetsChange) + " ";
		retStr += "ResyncType=" + resyncType.toString();
		return retStr;
	}
	
	public int hashCode() {
		int prime = 17;
		int result = 1;
		result += result*prime + type.hashCode();
		result += result*prime + Arrays.hashCode(devNode);
		result += result*prime + Arrays.hashCode(targetSyncSourcesChange);
		result += result*prime + Arrays.hashCode(targetSyncTargetsChange);
		result += result*prime + resyncType.hashCode();
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
		if (resyncType == null) {
			if (other.resyncType != null) {
				return false;
			}
		} else if (!resyncType.equals(other.resyncType)) {
			return false;
		}
			
		return true;
	}
}
