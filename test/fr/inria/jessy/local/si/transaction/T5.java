package fr.inria.jessy.local.si.transaction;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.entity.Sample2EntityClass;
import fr.inria.jessy.entity.SampleEntityClass;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;

public class T5 extends Transaction{

	public T5(Jessy jessy) throws Exception {
		super(jessy);
		setRetryCommitOnAbort(false);
	}

	@Override
	public ExecutionHistory execute() {
		try {
			
			read(SampleEntityClass.class, "1");
			
			read(Sample2EntityClass.class, "2");
			
			return commitTransaction();	
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}	
	}
}
