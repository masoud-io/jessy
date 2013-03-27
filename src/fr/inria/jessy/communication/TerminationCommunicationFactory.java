package fr.inria.jessy.communication;

import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.membership.Group;
import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.ConstantPool.GENUINE_TERMINATION_MODE;
import fr.inria.jessy.DistributedJessy;
import fr.inria.jessy.consistency.ConsistencyFactory;
import fr.inria.jessy.transaction.termination.DistributedTermination;

/**
 * This class provides a factory for generating the {@link TerminationCommunication} needed in {@link DistributedTermination}
 * @author Masoud Saeida Ardekani
 *
 */
public class TerminationCommunicationFactory {
	private static TerminationCommunication _instance;

	private static String consistencyTypeName;
	
	static {
		consistencyTypeName = ConsistencyFactory.getConsistencyTypeName();
	}

	public static TerminationCommunication initAndGetConsistency(
			Group group, 
			Learner fractalLearner,
			UnicastLearner nettyLearner,
			DistributedJessy j) {
		if (_instance != null)
			return _instance;
				
		if (consistencyTypeName.equals("rc") ) {
			_instance = new TrivialTerminationCommunication(fractalLearner, nettyLearner,j);
		} 
		else if (ConstantPool.TERMINATION_COMMUNICATION_TYPE==GENUINE_TERMINATION_MODE.GENUINE){
			_instance=new GenuineTerminationCommunication(group, fractalLearner,nettyLearner,j);
		}
		else if (ConstantPool.TERMINATION_COMMUNICATION_TYPE==GENUINE_TERMINATION_MODE.LIGHT_GENUINE){
			_instance=new LightGenuineTerminationCommunicationWithFractal(group, fractalLearner,nettyLearner,j);
		}
		return _instance;
	}

	public static TerminationCommunication getTerminationCommunicationInstance() {
		return _instance;
	}
}

