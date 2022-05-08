package strata.common;

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
		//System.out.println("ts =" + Arrays.toString(ts));
		//System.out.println("numAsyncOp=" + numAsyncOp);
		//System.out.println("before applying deviation nodestate is=" + Arrays.toString(nodeStateForSubpath));
		for (int i = 0; i < curState.nodeState.length; i++) {
			curState.nodeState[i] += targetState[i];
		}
		Arrays.fill(curState.onlineStatus, false);
		return true;
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
