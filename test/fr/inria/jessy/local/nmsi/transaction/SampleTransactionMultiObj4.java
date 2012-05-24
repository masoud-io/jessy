package fr.inria.jessy.local.nmsi.transaction;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.entity.Sample2EntityClass;
import fr.inria.jessy.entity.SampleEntityClass;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;

public class SampleTransactionMultiObj4 extends Transaction {

	public SampleTransactionMultiObj4(Jessy jessy) throws Exception{
		super(jessy);
	}

	@Override
	public ExecutionHistory execute() {
		try {
	
			Thread.sleep(500);
			
			SampleEntityClass se=read(SampleEntityClass.class, "1");			
			
			Thread.sleep(4000);
			
			Sample2EntityClass se2=read(Sample2EntityClass.class, "2");
			
			
			return commitTransaction();			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		
	}

}

 
