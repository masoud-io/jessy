package fr.inria.jessy.local.si.transaction;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.entity.SampleEntityClass;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;

public class T6x extends Transaction{
	
	public T6x(Jessy jessy) throws Exception {
		super(jessy);
	}
	
	@Override
	public ExecutionHistory execute() {

		try {
			
			Thread.sleep(1000);
			
			SampleEntityClass se=read(SampleEntityClass.class, "1");			
			se.setData("6x");
			write(se);
			
			Thread.sleep(2000);
			
			return commitTransaction();	
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		
	}

}
