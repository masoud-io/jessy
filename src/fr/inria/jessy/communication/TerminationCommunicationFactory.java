package fr.inria.jessy.communication;

import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.membership.Group;
import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.ConstantPool.GENUINE_TERMINATION_MODE;
import fr.inria.jessy.ConstantPool.MULTICAST_MODE;
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

	public static TerminationCommunication initAndGetConsistency(Group group, Learner fractalLearner, UnicastLearner nettyLearner) {
		if (_instance != null)
			return _instance;

		if (consistencyTypeName.equals("rc")) {
			_instance = new TrivialTerminationCommunication(fractalLearner, nettyLearner);
		} 
		else if (ConstantPool.TERMINATION_COMMUNICATION_TYPE==GENUINE_TERMINATION_MODE.GENUINE){
			_instance=new GenuineTerminationCommunication(group, fractalLearner, nettyLearner);
		}
		else if (ConstantPool.TERMINATION_COMMUNICATION_TYPE==GENUINE_TERMINATION_MODE.LIGHT_GENUINE){
			if(ConstantPool.JESSY_LIGHT_GENUINE_MULTICAST_MODE==MULTICAST_MODE.FRACTAL){
				_instance=new LightGenuineTerminationCommunicationWithFractal(group, fractalLearner, nettyLearner);
			}
			else if(ConstantPool.JESSY_LIGHT_GENUINE_MULTICAST_MODE==MULTICAST_MODE.NETTY){
				{
					_instance=new LightGenuineTerminationCommunicationWithNetty(group, fractalLearner, nettyLearner);
				}
			}
		}
		return _instance;
	}

	public static TerminationCommunication getTerminationCommunicationInstance() {
		return _instance;
	}

}
