package fr.inria.jessy.local.nmsi.transaction;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.entity.Sample2EntityClass;
import fr.inria.jessy.entity.SampleEntityClass;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;

public class SampleTransactionMultiObj2 extends Transaction {

	public SampleTransactionMultiObj2(Jessy jessy) throws Exception{
		super(jessy);
		setRetryCommitOnAbort(false);
	}

	@Override
	public ExecutionHistory execute() {
		try {
	
			Thread.sleep(500);
			
			SampleEntityClass se2=read(SampleEntityClass.class, "1");
			
			Sample2EntityClass se=read(Sample2EntityClass.class, "2");			
			
			se.setData("Second Trans");
			write(se);
			
			Thread.sleep(3000);
			
			return commitTransaction();			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		
	}

}

 
