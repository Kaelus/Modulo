package modulo.drm.aem.ptp;

import java.util.Arrays;

import modulo.drm.aem.AbstractState;

public class PointToPointAbstractState extends AbstractState {

	public int[] syncSources;
	public int[] syncTargets;

	public enum FailureType {NONE, CRASH, DISCON};
	
	public FailureType[] failureState;

	public PointToPointAbstractState(int no, int nr) {
		super(no, nr);
		this.syncSources = new int[nr];
		this.syncTargets = new int[nr];
		this.failureState = new FailureType[nr];
		Arrays.fill(syncSources, -1);
		Arrays.fill(syncTargets, 0);
		Arrays.fill(failureState, FailureType.NONE);
	}
}
