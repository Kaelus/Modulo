package strata.common;

import java.io.Serializable;

public class PathTuple implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7325411439544825268L;
	
	public int state;
	public Path path;

	public PathTuple(int state, Path path) {
		this.state = state;
		this.path = path;
	}

	public String toString() {
		String retStr = "PathTuple ";
		retStr += "state=" + state + " ";
		retStr += "path=" + path.toString();
		return retStr;
	}

	public PathTuple getSerializable() {
		PathTuple retPT = null;
		if (path instanceof DeviationPath) {
			DeviationPath dp = new DeviationPath(((DeviationPath) path).targetState);
			retPT = new PathTuple(this.state, dp);
		} else if (path instanceof ResyncPath) {
			ResyncPath rp = new ResyncPath(((ResyncPath) path).devNode);
			retPT = new PathTuple(this.state, rp);
		}
		return retPT;
	}
}
