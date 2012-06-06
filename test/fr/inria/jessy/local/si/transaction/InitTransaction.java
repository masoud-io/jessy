package fr.inria.jessy.local.si.transaction;

import org.apache.log4j.Logger;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.entity.Sample2EntityClass;
import fr.inria.jessy.entity.SampleEntityClass;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;

public class InitTransaction extends Transaction{

	private static Logger logger = Logger
	.getLogger(InitTransaction.class);
	
	public InitTransaction(Jessy jessy) throws Exception {
		super(jessy);
	}

	@Override
	public ExecutionHistory execute() {

		try {


			SampleEntityClass x=new SampleEntityClass("1", "0x");			
			create(x);

			Sample2EntityClass y=new Sample2EntityClass("2", "0y");			
			create(y);

			logger.debug("transaction InitTransaction started with :"+x.getLocalVector().getSelfValue()+" "+x.getLocalVector());

			return commitTransaction();			
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		

	}

}
