package fr.inria.jessy.local.si.transaction;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.entity.Sample2EntityClass;
import fr.inria.jessy.entity.SampleEntityClass;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;

public class InitTransaction extends Transaction{

	public InitTransaction(Jessy jessy) throws Exception {
		super(jessy);
	}

	@Override
	public ExecutionHistory execute() {

		try {


			SampleEntityClass x=new SampleEntityClass("1", "0x");			
			create(x);

			Sample2EntityClass y=new Sample2EntityClass("1", "0y");			
			create(y);

			return commitTransaction();			
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		

	}

}
