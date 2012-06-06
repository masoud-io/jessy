package fr.inria.jessy.local.si.transaction;

import org.apache.log4j.Logger;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.entity.SampleEntityClass;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;

public class T1x extends Transaction{
	
	private static Logger logger = Logger
	.getLogger(T1x.class);

	public T1x(Jessy jessy) throws Exception{
		super(jessy);
		setRetryCommitOnAbort(false);
	}

	@Override
	public ExecutionHistory execute() {

		try {
			
			SampleEntityClass se=read(SampleEntityClass.class, "1");			
			se.setData("1x");
			write(se);
			
			logger.debug("transaction T1x started with :"+se.getLocalVector().getSelfValue()+" "+se.getLocalVector());
			
			Thread.sleep(2000);
			
			return commitTransaction();	
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		
	}

}
