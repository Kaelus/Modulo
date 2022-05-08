package strata.common;

import java.io.Serializable;

public abstract class Path implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3819031346648737983L;

	public enum PathType {DEVIATION, RESYNC};
	
	public PathType type;
	
	public void setType(PathType t) {
		this.type = t;
	}
	
	public PathType getType() {
		return type;
	}
	
	public abstract boolean apply(AbstractState curState);
	
	public String toString() {
		String retStr = "";
		return "type=" + type.toString();
	}
	
}
