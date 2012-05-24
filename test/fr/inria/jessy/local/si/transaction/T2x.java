package fr.inria.jessy.local.si.transaction;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.entity.SampleEntityClass;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;

public class T2x extends Transaction{
	
	
	
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
			
			return commitTransaction();	
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		
	}

}
