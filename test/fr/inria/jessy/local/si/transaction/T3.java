package fr.inria.jessy.local.si.transaction;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.entity.Sample2EntityClass;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;

public class T3 extends Transaction{

	public T3(Jessy jessy) throws Exception{
		super(jessy);
	}

	@Override
	public ExecutionHistory execute() {

		try {
			
			Thread.sleep(1000);
			
			Sample2EntityClass se=new Sample2EntityClass("1", "1y");
//			se.setData("1y");
			write(se);
			
			return commitTransaction();	
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		
	}
}
