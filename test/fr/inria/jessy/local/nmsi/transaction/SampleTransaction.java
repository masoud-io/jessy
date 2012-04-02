package fr.inria.jessy.local.nmsi.transaction;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.entity.Sample2EntityClass;
import fr.inria.jessy.entity.SampleEntityClass;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;

public class SampleTransaction extends Transaction {

	public SampleTransaction(Jessy jessy) throws Exception{
		super(jessy);
	}

	@Override
	public ExecutionHistory execute() {
		try {
	
			SampleEntityClass se=new SampleEntityClass("1", "sampleentity1");			
			write(se);

			SampleEntityClass readentity=read(SampleEntityClass.class, "1");			
			if (readentity.getData()=="sampleentity1"){
				write(new Sample2EntityClass("2", "sampleentity2-2"));
			}
			
			
			return commitTransaction();			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		
	}


}
