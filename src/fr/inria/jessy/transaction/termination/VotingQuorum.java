package fr.inria.jessy.transaction.termination;

import java.util.Collection;
import java.util.HashSet;

import net.sourceforge.fractal.utils.PerformanceProbe.ValueRecorder;

import org.apache.log4j.Logger;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.transaction.TransactionState;

/**
 * This class receives {@link Vote} from different participations in the
 * transaction termination phase. It returns {@link TransactionState.COMMITTED}
 * if it receives <code>true</code> votes from a member of all groups concerned
 * by the transaction. If it receives a <code>false</code> vote from a member of
 * a group, it will returns {@link TransactionState.ABORTED_BY_VOTING}.
 * Otherwise, it will simply waits until receiving enough votes.
 * <p>
 * A set of concerned groups is provided by {@code groups} argument of the
 * constructor.
 * 
 * @author Masoud Saeida Ardekani
 * @author Pierre Sutra
 * 
 */
public class VotingQuorum {

	private static Logger logger = Logger.getLogger(VotingQuorum.class);
	
	private TransactionState result = TransactionState.COMMITTING;
	private TransactionHandler transactionHandler;
	private Collection<String> voters;
	private boolean notified=false;

	public VotingQuorum(TransactionHandler th) {
		transactionHandler = th;
		voters = new HashSet<String>();
	}

	/**
	 * Add a received {@code vote}.
	 * <p>
	 * It returns {@link TransactionState.COMMITTED} if it receives
	 * <code>true</code> votes from a member of all groups concerned by the
	 * transaction. If it receives a <code>false</code> vote from a member of a
	 * group, it will returns {@link TransactionState.ABORTED_BY_VOTING}.
	 * 
	 * @param vote
	 */
	public synchronized void addVote(Vote vote) {
		if (ConstantPool.logging)
			logger.debug("adding vote for "+transactionHandler+" for "+vote.getVoterGroupName()+" with result "+vote.isAborted());
		
		if (vote.isAborted() == false) {
			result = TransactionState.ABORTED_BY_VOTING;
		} else{
			voters.add(vote.getVoterGroupName());
		}
		
		notified=true;
		notify();
	}

	/**
	 * It simply waits until it receives enough votes from members of all groups
	 * concerned by the transaction.
	 * 
	 * @return either {@link TransactionState.COMMITTED} or
	 *         {@link TransactionState.ABORTED_BY_VOTING}
	 */
	public synchronized TransactionState waitVoteResult(Collection<String> groups) {

		while( result == TransactionState.COMMITTING
			   && voters.size() < groups.size() ){
			try {
				notified=false;
				wait(ConstantPool.JESSY_VOTING_QUORUM_TIMEOUT);
				if (!notified){
					return TransactionState.ABORTED_BY_TIMEOUT;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		if( result == TransactionState.COMMITTING ){
			if (ConstantPool.logging)
				logger.debug("Has enought YES votes for  "+transactionHandler + " . Returning Committed. Groups are: " + groups + " . voters are : " + voters);
			return TransactionState.COMMITTED;
		}
		
		if (ConstantPool.logging)
			logger.debug("DOES NOT have enought YES votes for  "+transactionHandler + " . Returning Abort_by_Voting. Groups are: " + groups + " . voters are : " + voters);
		return TransactionState.ABORTED_BY_VOTING;
	}

}
