package fr.inria.jessy.transaction;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.entity.Sample2EntityClass;

public class SampleTransactionMultiObj2 extends Transaction {

	public SampleTransactionMultiObj2(Jessy jessy) throws Exception{
		super(jessy);
	}

	@Override
	public ExecutionHistory execute() {
		try {
	
			Thread.sleep(1000);
			
			Sample2EntityClass se=read(Sample2EntityClass.class, "1");			
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

 
