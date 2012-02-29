package fr.inria.jessy.consistency.nmsi;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.store.Sample2EntityClass;
import fr.inria.jessy.store.SampleEntityClass;
import fr.inria.jessy.transaction.*;

public class SampleTransaction1 extends Transaction {

	public SampleTransaction1(Jessy jessy) throws Exception{
		super(jessy);
	}

	@Override
	public boolean execute() {
		try {
	
			SampleEntityClass se=read(SampleEntityClass.class, "1");			
			se.setData("Second Trans");
			write(se);

			Thread.sleep(10000);
			
			return commitTransaction();			
		} catch (Exception ex) {
			ex.printStackTrace();
			return false;
		}		
	}

}
