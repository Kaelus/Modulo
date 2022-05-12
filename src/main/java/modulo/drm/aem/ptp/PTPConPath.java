package modulo.drm.aem.ptp;

import modulo.drm.aem.AbstractConvergencePath;
import modulo.drm.aem.AbstractState;
import modulo.drm.aem.Path;
import modulo.drm.aem.ptp.PointToPointAbstractState.FailureType;

public class PTPConPath  extends AbstractConvergencePath{

	/**
	 * 
	 */
	private static final long serialVersionUID = 5006678762844995332L;

	public int[] divReplica;
	public int[] targetSyncSourcesChange;
	public int[] targetSyncTargetsChange;
	final static int resyncIncrease = 10000;
	
	public PTPConPath() {
        type = Path.PathType.CONVERGENCE;
	}

	protected static int getHighestNodeState(int[] replicaState) {
		int highest = 0;
		for (int i = 0; i < replicaState.length; i++) {
			if (highest < replicaState[i]) {
				highest = replicaState[i];
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
	
	@Override
	public boolean apply(AbstractState curSt) {
		PointToPointAbstractState curState = (PointToPointAbstractState) curSt;
		for (int nid : divReplica) {
			curState.onlineStatus[nid] = true;
			curState.failureState[nid] = FailureType.NONE;
		}
		/** update nodeState */
		// int quorumSize = curState.onlineStatus.length/2 + 1;
		int curNumReplicaOnline = numNodeOnline(curState.onlineStatus);
		int[] participants = new int[curNumReplicaOnline];
		int[] participantReplicaState = new int[curNumReplicaOnline];
		// System.out.println("participants are=");
		int j = 0;
		for (int i = 0; i < curState.onlineStatus.length; i++) {
			if (curState.onlineStatus[i]) {
				participantReplicaState[j] = curState.replicaState[i];
				participants[j++] = i;
				// System.out.println("" + i);
			}
		}
		int highestState = getHighestNodeState(participantReplicaState);
		// System.out.println("highestState=" + highestState);
		int resyncState = ((highestState / resyncIncrease) + 1) * resyncIncrease;
		for (int i = 0; i < participants.length; i++) {
			curState.replicaState[participants[i]] = resyncState;
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

		return true;

	}

}
