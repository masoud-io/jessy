package fr.inria.jessy.local.si.transaction;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.entity.Sample2EntityClass;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;

public class T4y extends Transaction{
	
	public T4y(Jessy jessy) throws Exception {
		super(jessy);
	}
	
	@Override
	public ExecutionHistory execute() {

		try {
			
			Thread.sleep(500);
			
			Sample2EntityClass se=read(Sample2EntityClass.class, "1");			
			se.setData("4y");
			write(se);
			
			return commitTransaction();	
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		
	}

}
