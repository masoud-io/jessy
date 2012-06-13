package fr.inria.jessy.consistency.local.si.transaction;

import org.apache.log4j.Logger;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.entity.SampleEntityClass;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;
import fr.inria.jessy.vector.ScalarVector;

public class T3x extends Transaction{
	
	private static Logger logger = Logger
	.getLogger(T3x.class);
	
	public T3x(Jessy jessy) throws Exception {
		super(jessy);
		setRetryCommitOnAbort(false);
	}
	
	@Override
	public ExecutionHistory execute() {
		
		try {
			
			SampleEntityClass se=read(SampleEntityClass.class, "1");	
			
			logger.debug("lastCommittedTransactionSeqNumber:"+ScalarVector.lastCommittedTransactionSeqNumber.get()+" "+se.getLocalVector());
			se.setData("3x");
			
			logger.debug("transaction T3x started with SelfValue:"+se.getLocalVector().getSelfValue()+" "+se.getLocalVector());
			
			write(se);
			
			Thread.sleep(2000);
			
			return commitTransaction();	
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		
	}

}
