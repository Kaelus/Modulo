package strata.common;

import java.util.ArrayList;
import java.util.Arrays;

public class DeviationPath extends Path{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6774739653394914442L;
	
	public int[] targetState;
	
	public DeviationPath() {
		type = Path.PathType.DEVIATION;
	}
	
	public DeviationPath(int[] ts) {
		type = Path.PathType.DEVIATION;
		targetState = ts;
	}
	
	public void setTargetState(int[] ts) {
		this.targetState = ts;
	}
	
	public int[] getTargetState() {
		return targetState;
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
	
	public boolean apply(AbstractState curState) {
		curState.numAsyncOp -= getHighestNodeState(targetState);
		//System.out.println("before syncSources=" + Arrays.toString(curState.syncSources));
		//System.out.println("before syncTargets=" + Arrays.toString(curState.syncTargets));
		System.out.println("[before applying] curState=" + curState.toString());
		System.out.println("applying divergence path=" + this.toString());
		
		// Assumption: targetState should be already sorted (when this divergence action is created) so that the index 
		// is directly mapped to the node ID and the value at the index represents target state change for the node
		for (int i = 0; i < curState.nodeState.length; i++) {
			if (curState.onlineStatus[i]) {
				int curSyncSource = curState.syncSources[i];
				if (curSyncSource > -1 && targetState[curSyncSource] > targetState[i]) {
					curState.syncSources[i] = -1;
					curState.syncTargets[curSyncSource] -= (1 << i);
					if (curState.syncTargets[curSyncSource] < 0) {
						System.err.println("in DeviationPath.apply. syncTargets should not contain a negative integer");
						System.err.println("i=" + i + " curSyncSource=" + curSyncSource);
						System.err.println("syncSources after=" + Arrays.toString(curState.syncSources));
						System.err.println("syncTargets after=" + Arrays.toString(curState.syncTargets));
						System.exit(1);
					}
					//curState.onlineStatus[i] = false;
				}
				if (targetState[i] > 0) {
					curState.nodeState[i] += targetState[i];
				}
			}
			curState.onlineStatus[i] = false;
		}
		//System.out.println("syncSources after=" + Arrays.toString(curState.syncSources));
		//System.out.println("syncTargets after=" + Arrays.toString(curState.syncTargets));
		//System.out.println("onlineStatus=" + Arrays.toString(curState.onlineStatus));
		System.out.println("[after applying] curState=" + curState.toString());
		
		return true;
	}
	
	public int findLeader(AbstractState curState) {
		System.out.println("findLeader");
		
		int numNode = curState.nodeState.length;
		boolean[] isNodeOnline = curState.onlineStatus;
		
		int leaderID = -1;
		for (int i = 0; i < numNode; i++) {
			if (isNodeOnline[i] && (curState.syncSources[i] == -1)) {
				leaderID = i;
			}
		}
		
		// sanity check. should not return -1
		if (leaderID == -1) {
			System.err.println("DeviationPath findLeader should not return -1");
			System.exit(1);
		}
		return leaderID;
	}
	
	public int[] sortTargetState(AbstractState curState, int[] targetState) {
		System.out.println("sortTargetState=" + Arrays.toString(targetState));
		
		int numNode = curState.nodeState.length;
		boolean[] isNodeOnline = curState.onlineStatus;
		int actualLeader = -1;
		
		System.out.println("  Note: node online status=" + Arrays.toString(isNodeOnline));
		
		// sanity checks:
		// 1. check if the length of the targetState equals to the number of nodes
		// 2. see if the number of non-zero values in targetState equals to the number of online nodes 
		// 3. check if targetState is sorted in the increasing order
		// 4. check if there is only one primary for online nodes; there is no 'online' node partitioned from the primary
		// 5. see if there is a primary among online nodes
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
		int maxSeen = -1;
		for (int i = 0; i < numNode; i++) {
			if (maxSeen > targetState[i]) {
				System.err.println("Assert: targetState should be sorted in the increasing order");
				System.exit(1);
			} else {
				maxSeen = targetState[i];
			}
		}
		int primaryFound = -1; 
		for (int i = 0; i < numNode; i++) {
			if (curState.onlineStatus[i]) {
				if (curState.syncSources[i] == -1) {
					if (primaryFound > 0) {
						System.err.println("Assert: There should be only one primary among online nodes; there is no 'online' node partitioned from the primary");
						System.exit(1);
					} else {
						primaryFound = i;
					}
				}
			}
		}
		if (primaryFound < 0) {
			System.err.println("Assert: There should be a primary among online nodes");
			System.exit(1);
		}

		// find all replication chains
		ArrayList<ArrayList<Integer>> replChains = new ArrayList<ArrayList<Integer>>();
		for (int i = 0; i < numNode; i++) {
			int curNode = i;
			ArrayList<Integer> chain = new ArrayList<Integer>();
			while (true) {
				if (curState.syncSources[curNode] != -1) {
					
				} else {
					break;
				}
			}
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
		actualLeader = findLeader(curState);
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

	
	public String toString() {
		String retStr = "";
		return super.toString() + 
				" targetState=" + Arrays.toString(targetState);
	}
	
	public int hashCode() {
		int prime = 17;
		int result = 1;
		result += result*prime + type.hashCode();
		result += result*prime + Arrays.hashCode(targetState);
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DeviationPath other = (DeviationPath) obj;
		if (targetState == null) {
			if (other.targetState != null)
				return false;
		} else if (!Arrays.equals(targetState, other.targetState)) 
			return false;
		return true;
	}
}
