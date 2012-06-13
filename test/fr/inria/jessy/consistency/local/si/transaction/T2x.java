package fr.inria.jessy.consistency.local.si.transaction;

import org.apache.log4j.Logger;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.entity.SampleEntityClass;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;

public class T2x extends Transaction{
	
	private static Logger logger = Logger
	.getLogger(T2x.class);
	
	public T2x(Jessy jessy) throws Exception {
		super(jessy);
		setRetryCommitOnAbort(false);
	}

	@Override
	public ExecutionHistory execute() {

		try {
			
			Thread.sleep(500);
			
			SampleEntityClass se=read(SampleEntityClass.class, "1");			
			se.setData("2x");
			write(se);
			
			logger.debug("transaction T2x started with :"+se.getLocalVector().getSelfValue()+" "+se.getLocalVector());
			
			return commitTransaction();	
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		
	}

}
