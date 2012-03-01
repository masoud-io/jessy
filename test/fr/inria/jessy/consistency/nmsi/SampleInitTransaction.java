package fr.inria.jessy.consistency.nmsi;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.store.Sample2EntityClass;
import fr.inria.jessy.store.SampleEntityClass;
import fr.inria.jessy.transaction.*;

public class SampleInitTransaction extends Transaction {

	public SampleInitTransaction(Jessy jessy) throws Exception{
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
