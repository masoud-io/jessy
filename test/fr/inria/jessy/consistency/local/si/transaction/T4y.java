package fr.inria.jessy.consistency.local.si.transaction;

import org.apache.log4j.Logger;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.entity.Sample2EntityClass;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;

public class T4y extends Transaction{
	
	private static Logger logger = Logger
	.getLogger(T4y.class);
	
	public T4y(Jessy jessy) throws Exception {
		super(jessy);
		setRetryCommitOnAbort(false);
	}
	
	@Override
	public ExecutionHistory execute() {

		try {
			
			Thread.sleep(500);
			
			Sample2EntityClass se=read(Sample2EntityClass.class, "2");			
			se.setData("4y");
			write(se);
			
			logger.debug("transaction T4y started with :"+se.getLocalVector().getSelfValue()+" "+se.getLocalVector());
			
			return commitTransaction();	
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		
	}

}
