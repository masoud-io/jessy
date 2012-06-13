package fr.inria.jessy.distributed.psi.transaction;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.entity.SampleEntityClass;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;

public class SampleTransactionMultiObj1 extends Transaction {

	public SampleTransactionMultiObj1(Jessy jessy) throws Exception{
		super(jessy);
	}

	@Override
	public ExecutionHistory execute() {
		try {
	
			Thread.sleep(2000);
			
			SampleEntityClass se=read(SampleEntityClass.class, "1");			
			se.setData("Second Trans");
			write(se);
			
			
			return commitTransaction();			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		
	}

}

 
