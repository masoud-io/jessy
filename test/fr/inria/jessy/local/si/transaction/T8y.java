package fr.inria.jessy.local.si.transaction;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.entity.Sample2EntityClass;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;

public class T8y extends Transaction{
	
	public T8y(Jessy jessy) throws Exception {
		super(jessy);
	}
	
	@Override
	public ExecutionHistory execute() {

		try {
			
			Thread.sleep(1000);
			
			Sample2EntityClass se=read(Sample2EntityClass.class, "1");			
			se.setData("8y");
			write(se);
			
			Thread.sleep(2000);
			
			return commitTransaction();	
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		
	}

}
