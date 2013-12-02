package fr.inria.jessy.communication;

import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.membership.Group;
import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.ConstantPool.ATOMIC_COMMIT_TYPE;
import fr.inria.jessy.ConstantPool.GENUINE_TERMINATION_MODE;
import fr.inria.jessy.DistributedJessy;
import fr.inria.jessy.transaction.termination.DistributedTermination;

/**
 * This class provides a factory for generating the {@link TerminationCommunication} needed in {@link DistributedTermination}
 * 
 * @author Masoud Saeida Ardekani
 *
 */
public class TerminationCommunicationFactory {
	private static TerminationCommunication _instance;

	public static TerminationCommunication initAndGetConsistency(
			Group group, 
			Learner fractalLearner,
			UnicastLearner nettyLearner,
			DistributedJessy j) {
		
		if (_instance != null)
			return _instance;
				
		if (ConstantPool.PROTOCOL_ATOMIC_COMMIT==ATOMIC_COMMIT_TYPE.TWO_PHASE_COMMIT) {
			_instance = new TrivialTerminationCommunication(fractalLearner, nettyLearner,j);
		} 
		else if (ConstantPool.PROTOCOL_ATOMIC_COMMIT==ATOMIC_COMMIT_TYPE.ATOMIC_CYLCLIC_MULTICAST){
			_instance=new LightGenuineTerminationCommunicationWithFractal(group, fractalLearner,nettyLearner,j,true);
		}
		else if (ConstantPool.PROTOCOL_ATOMIC_COMMIT==ATOMIC_COMMIT_TYPE.ATOMIC_MULTICAST && ConstantPool.TERMINATION_COMMUNICATION_TYPE==GENUINE_TERMINATION_MODE.GENUINE){
			_instance=new GenuineTerminationCommunication(group, fractalLearner,nettyLearner,j);
		}
		else if (ConstantPool.PROTOCOL_ATOMIC_COMMIT==ATOMIC_COMMIT_TYPE.ATOMIC_MULTICAST && ConstantPool.TERMINATION_COMMUNICATION_TYPE==GENUINE_TERMINATION_MODE.LIGHT_GENUINE){
			_instance=new LightGenuineTerminationCommunicationWithFractal(group, fractalLearner,nettyLearner,j,false);
		}
		
		System.out.println("Atomic Commitment is :" + ConstantPool.PROTOCOL_ATOMIC_COMMIT);
		
		return _instance;
	}

	public static TerminationCommunication getTerminationCommunicationInstance() {
		return _instance;
	}
}

