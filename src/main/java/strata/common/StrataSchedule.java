package strata.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class StrataSchedule implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6230473693465694966L;
	
	public List<Path> sched;
	
	public StrataSchedule() {
		sched = new ArrayList<Path>();
	}
	
	public void setSchedule(List<Path> sched) {
		this.sched = sched;
	}
	
	public List<Path> getSchedule() {
		return sched;
	}
	
	public String toString() {
		String retStr = "";
		for (Path p : sched) {
			retStr += p.toString() + "\n";
		}
		return retStr;
	}
	
	private boolean compareListOfSchedules(List<Path> mySched, List<Path> otherSched) {
		if (mySched == null || otherSched == null) {
			return false;
		}
		if (mySched.size() != otherSched.size()) {
			return false;
		}
		for (int i = 0; i < mySched.size(); i++) {
			if (!mySched.get(i).equals(otherSched.get(i))) {
				System.out.println("NOPE given schedules are not equal");
				return false;
			}
		}
		return true;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StrataSchedule other = (StrataSchedule) obj;
		if (sched == null) {
			if (other.sched != null)
				return false;
		} else if (!compareListOfSchedules(sched, other.sched))
			return false;
		return true;
	}
}
