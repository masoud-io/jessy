package fr.inria.jessy.local.si.transaction;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.entity.Sample2EntityClass;
import fr.inria.jessy.entity.SampleEntityClass;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;

public class T1 extends Transaction{

	public T1(Jessy jessy) throws Exception{
		super(jessy);
	}
	
	@Override
	public ExecutionHistory execute() {

		try {
			
			read(SampleEntityClass.class, "1");			
			
			Thread.sleep(2000);
			
			read(Sample2EntityClass.class, "1");
			
			return commitTransaction();	
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		
	}

}
