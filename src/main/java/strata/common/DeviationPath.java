package strata.common;

import java.util.ArrayList;
import java.util.Arrays;

public class DeviationPath extends Path{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6774739653394914442L;
	
	public enum FailureType {NONE, CRASH, DISCON};
	public FailureType[] failureScenario;
	
	public int[] targetState;
	
	public DeviationPath() {
		type = Path.PathType.DEVIATION;
	}
	
	public DeviationPath(int[] ts) {
		type = Path.PathType.DEVIATION;
		targetState = ts;
	}
	
	public DeviationPath(int[] ts, FailureType[] fscen) {
		type = Path.PathType.DEVIATION;
		this.targetState = ts;
		this.failureScenario = fscen;
		
		/*
		// assert: target failureScenario shouldn't be composed of only NONE
		boolean isAllNode = true;
		for (int i = 0; i < failureScenario.length; i++) {
			if (!failureScenario[i].equals(DeviationPath.FailureType.NONE)) {
				isAllNode = false;
				break;
			}
		}
		if (isAllNode) {
			System.err.println("DeviationPath(int[] ts, FailureType[] fscen) Assert: target failureScenario shouldn't be composed of only NONE");
			System.err.println("DeviationPath=" + toString());
			//System.err.println("curState=" + curState.toString());
			System.exit(1);
		}
		*/
	}
	
	public void setTargetState(int[] ts) {
		this.targetState = ts;
	}
	
	public int[] getTargetState() {
		return targetState;
	}
	
	public void setFailureScenario(FailureType[] fscen) {
		this.failureScenario = fscen;
	}
	
	public FailureType[] getFailureScenario() {
		return this.failureScenario;
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
		
		System.out.println("DeviationPath=" + this.toString());
		System.out.println("curState=" + curState.toString());
		//System.out.println("Deviation Path. before. curState.syncSources=" + Arrays.toString(curState.syncSources));
		//System.out.println("Deviation Path. before. curState.syncTargets=" + Arrays.toString(curState.syncTargets));
		//System.out.println("Deviation Path. before. curState.failureState=" + Arrays.deepToString(curState.failureState));
		//System.out.println("Deviation Path. before. target failureState=" + Arrays.deepToString(this.failureScenario));	
		//System.out.println("Deviation Path.  before. curState.onlineStatus=" + Arrays.toString(curState.onlineStatus));

		// assert current offline node's failureState shouldn't be changed
		for (int i = 0; i < curState.nodeState.length; i++) {
			if (!curState.onlineStatus[i]) {
				if (!this.failureScenario[i].equals(curState.failureState[i])) {
					System.err.println("DeviationPath Assert: offline node's failureState shouldn't be changed");
					System.err.println("DeviationPath=" + toString());
					System.err.println("curState=" + curState.toString());
					System.exit(1);
				}
			}
		}
		// assert: target failureScenario shouldn't be composed of only NONE
		boolean isAllNode = true;
		for (int i = 0; i < failureScenario.length; i++) {
			if (!failureScenario[i].equals(DeviationPath.FailureType.NONE)) {
				isAllNode = false;
				break;
			}
		}
		if (isAllNode) {
			System.err.println("DeviationPath.apply Assert: target failureScenario shouldn't be composed of only NONE");
			System.err.println("DeviationPath=" + toString());
			System.err.println("curState=" + curState.toString());
			System.exit(1);
		}
		
		int[] syncSourceClone = curState.syncSources.clone();
		// Assumption: targetState should be already sorted (when this divergence action is created) so that the index 
		// is directly mapped to the node ID and the value at the index represents target state change for the node
		for (int i = 0; i < curState.nodeState.length; i++) {
			//System.out.println("Considering node=" + i 
			//		+ " whose failureScenario is=" + this.failureScenario[i].toString()
			//		+ ". the node has onlineState=" + curState.onlineStatus[i]
			//		+ " the node has syncSources=" + curState.syncSources[i]
			//		+ " the node has syncTargets=" + curState.syncTargets[i]);
			// inject failures. 1. Crash and 2. Disconnect
			if (this.failureScenario[i].equals(DeviationPath.FailureType.CRASH)) {
				if (curState.onlineStatus[i]) {
					int curSyncSource = syncSourceClone[i];
					//System.out.println("curSyncSource is=" + curSyncSource);
					if (curSyncSource > -1) {
						curState.syncSources[i] = -1;
						curState.syncTargets[curSyncSource] -= (1 << i);
						//System.out.println("curState.syncSources[i]=" + curState.syncSources[i]);
						//System.out.println("curState.syncTargets[curSyncSource]=" + curState.syncTargets[curSyncSource]);
						if (curState.syncTargets[curSyncSource] < 0) {
							System.err.println("[1] in DeviationPath.apply. syncTargets should not contain a negative integer");
							System.err.println("i=" + i + " curSyncSource=" + curSyncSource);
							System.err.println("syncSources after=" + Arrays.toString(curState.syncSources));
							System.err.println("syncTargets after=" + Arrays.toString(curState.syncTargets));
							System.exit(1);
						}
					}
					curState.onlineStatus[i] = false;
					curState.failureState[i] = this.failureScenario[i];
				}
			} else if (this.failureScenario[i].equals(DeviationPath.FailureType.DISCON)) {
				if (curState.onlineStatus[i]) {
					int curSyncSource = syncSourceClone[i];
					//System.out.println("curSyncSource is=" + curSyncSource);
					//System.out.println("targetState[curSyncSource]=" + targetState[curSyncSource]);
					//System.out.println("targetState[i]=" + targetState[i]);
					//if (curSyncSource > -1 && 
					//		(targetState[curSyncSource] > targetState[i] 
					//				| this.failureScenario[curSyncSource] != this.failureScenario[i])) {
					if (curSyncSource > -1) {
						curState.syncSources[i] = -1;
						curState.syncTargets[curSyncSource] -= (1 << i);
						System.out.println("Deviation Path. curState.syncSources[i]=" + curState.syncSources[i]);
						System.out.println("Deviation Path. curState.syncTargets[curSyncSource]=" + curState.syncTargets[curSyncSource]);
						if (curState.syncTargets[curSyncSource] < 0) {
							System.err.println("[2] in DeviationPath.apply. syncTargets should not contain a negative integer");
							System.err.println("i=" + i + " curSyncSource=" + curSyncSource);
							System.err.println("syncSources after=" + Arrays.toString(curState.syncSources));
							System.err.println("syncTargets after=" + Arrays.toString(curState.syncTargets));
							System.exit(1);
						}
					}
					curState.onlineStatus[i] = false;
					curState.failureState[i] = this.failureScenario[i];
				}
			} else if (this.failureScenario[i].equals(DeviationPath.FailureType.NONE)) {
				if (curState.onlineStatus[i]) {
					int curSyncSource = syncSourceClone[i];
					//System.out.println("curSyncSource is=" + curSyncSource);
					if (curSyncSource > -1) {
						curState.syncSources[i] = -1;
						curState.syncTargets[curSyncSource] -= (1 << i);
						//System.out.println("curState.syncSources[i]=" + curState.syncSources[i]);
						//System.out.println("curState.syncTargets[curSyncSource]=" + curState.syncTargets[curSyncSource]);
						if (curState.syncTargets[curSyncSource] < 0) {
							System.err.println("[3] in DeviationPath.apply. syncTargets should not contain a negative integer");
							System.err.println("i=" + i + " curSyncSource=" + curSyncSource);
							System.err.println("syncSources after=" + Arrays.toString(curState.syncSources));
							System.err.println("syncTargets after=" + Arrays.toString(curState.syncTargets));
							System.exit(1);
						}
						curState.onlineStatus[i] = false;
						curState.failureState[i] = DeviationPath.FailureType.DISCON;
					}
				}
			}
			if (curState.onlineStatus[i]) {
				if (targetState[i] > 0) {
					curState.nodeState[i] += targetState[i];
				}
			}
			//curState.onlineStatus[i] = false;
		}

		//curState.failureState = this.failureScenario;
		
		//System.out.println("Deviation Path. curState.syncSources after=" + Arrays.toString(curState.syncSources));
		//System.out.println("Deviation Path. curState.syncTargets after=" + Arrays.toString(curState.syncTargets));
		//System.out.println("Deviation Path. curState.failureState after=" + Arrays.deepToString(curState.failureState));
		//System.out.println("Deviation Path. target failureState after=" + Arrays.deepToString(this.failureScenario));	
		//System.out.println("Deviation Path. curState.onlineStatus after=" + Arrays.toString(curState.onlineStatus));
		
		System.out.println("curState after applied=" + curState.toString());
		
		
		return true;
	}
	
	public String toString() {
		String retStr = "";
		return super.toString() + 
				" targetState=" + Arrays.toString(targetState) +
				" failureScenario=" + Arrays.deepToString(failureScenario) + " @failureScenario=" + failureScenario;
	}
	
	public int hashCode() {
		int prime = 17;
		int result = 1;
		result += result*prime + type.hashCode();
		result += result*prime + Arrays.hashCode(targetState);
		result += result*prime + Arrays.deepHashCode(failureScenario);
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
			if (other.targetState != null) {
				return false;
			}
		} else if (!Arrays.equals(targetState, other.targetState)) { 
			return false;
		}
		if (failureScenario == null) {
			if (other.failureScenario != null) {
				return false;
			}
		} else if (!Arrays.deepEquals(failureScenario, other.failureScenario)) {
			return false;
		}
		
		return true;
	}
}
