package strata.common;

public abstract class AbstractEvent {

	public static enum EventLabel {
		AOP, CRASH, RESTART, COMMIT, SNAP
	}
	
	int eventId;
	EventLabel type;
	
	public AbstractEvent(int eventId, EventLabel type) {
		this.eventId = eventId;
		this.type = type;
	}
	
	public String toString() {
		String retStr = "";
		retStr += "eventId=" + eventId + " ";
		retStr += " type=" + type.toString() + " ";
		return retStr;
	}
}
