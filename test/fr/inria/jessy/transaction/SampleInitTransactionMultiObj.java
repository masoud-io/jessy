package fr.inria.jessy.transaction;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.entity.SampleEntityClass;

public class SampleInitTransactionMultiObj extends Transaction {

	public SampleInitTransactionMultiObj(Jessy jessy) throws Exception{
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
