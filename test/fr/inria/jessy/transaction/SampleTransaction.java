package fr.inria.jessy.transaction;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.store.Sample2EntityClass;
import fr.inria.jessy.store.SampleEntityClass;
import fr.inria.transaction.*;

public class SampleTransaction extends Transaction {

	public SampleTransaction(Jessy jessy,TransactionHandler transactionHandler) {
		super(jessy,transactionHandler);
	}

	@Override
	public boolean execute() {
		try {
			// First, we have to define the entities read or written inside the transaction
			getJessy().addEntity(SampleEntityClass.class);
			getJessy().addEntity(Sample2EntityClass.class);

		
			SampleEntityClass se=new SampleEntityClass("1", "sampleentity1");
			Sample2EntityClass se2=new Sample2EntityClass("1", "sampleentity2");
			
			


			return true;
		} catch (Exception ex) {
			return false;
		}		
	}

}
