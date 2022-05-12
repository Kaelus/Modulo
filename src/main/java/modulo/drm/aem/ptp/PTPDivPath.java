package modulo.drm.aem.ptp;

import modulo.drm.aem.AbstractDivergencePath;
import modulo.drm.aem.AbstractState;
import modulo.drm.aem.Path;
import modulo.drm.aem.ptp.PointToPointAbstractState.FailureType;

public class PTPDivPath extends AbstractDivergencePath {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8775694095563248465L;

	public FailureType[] failureScenario;
	
	public PTPDivPath(int[] ts, FailureType[] fscen) {
        type = Path.PathType.DIVERGENCE;
        this.targetState = ts;
        this.failureScenario = fscen;
	}

	
	@Override
	public boolean apply(AbstractState curState) {
		// TODO Auto-generated method stub
		return false;
	}

}
