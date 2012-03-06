package fr.inria.jessy.consistency.nmsi.transaction;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.store.Sample2EntityClass;
import fr.inria.jessy.store.SampleEntityClass;
import fr.inria.jessy.transaction.*;

public class SampleInitTransactionSingleObj extends Transaction {

	public SampleInitTransactionSingleObj(Jessy jessy) throws Exception{
		super(jessy);
	}

	@Override
	public ExecutionHistory execute() {
		try {
				
			
			SampleEntityClass se=new SampleEntityClass("1", "sampleentity1_INITIAL");			
			write(se);
			
			return commitTransaction();			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		
	}

}
