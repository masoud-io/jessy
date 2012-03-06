package fr.inria.jessy.transaction;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.entity.Sample2EntityClass;

public class Sample2EntityInitTransaction extends Transaction {

	public Sample2EntityInitTransaction(Jessy jessy) throws Exception{
		super(jessy);
	}

	@Override
	public ExecutionHistory execute() {
		try {
				
			
			Sample2EntityClass se=new Sample2EntityClass("1", "sample2entity2_INITIAL");			
			write(se);
			
			return commitTransaction();			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		
	}

}
