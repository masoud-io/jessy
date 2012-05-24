package fr.inria.jessy.local.si.transaction;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.entity.Sample2EntityClass;
import fr.inria.jessy.entity.SampleEntityClass;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;
import fr.inria.jessy.vector.ScalarVector;

public class T1 extends Transaction{

	public T1(Jessy jessy) throws Exception{
		super(jessy);
		setRetryCommitOnAbort(false);
	}
	
	@Override
	public ExecutionHistory execute() {

		try {
			
			SampleEntityClass en=read(SampleEntityClass.class, "1");
			System.out.println(en.getLocalVector());
			Thread.sleep(2000);
			
			Sample2EntityClass en2=read(Sample2EntityClass.class, "2");
			
			return commitTransaction();	
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		
	}

}
