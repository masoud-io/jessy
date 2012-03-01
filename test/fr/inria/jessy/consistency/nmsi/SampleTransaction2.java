package fr.inria.jessy.consistency.nmsi;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.store.Sample2EntityClass;
import fr.inria.jessy.store.SampleEntityClass;
import fr.inria.jessy.transaction.*;

public class SampleTransaction2 extends Transaction {

	public SampleTransaction2(Jessy jessy) throws Exception{
		super(jessy);
	}

	@Override
	public ExecutionHistory execute() {
		try {
	
			Thread.sleep(500);
			
			SampleEntityClass se=read(SampleEntityClass.class, "1");			
			se.setData("Second Trans");
			write(se);

			Thread.sleep(5000);
			
			return commitTransaction();			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		
	}

}
