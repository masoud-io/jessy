package fr.inria.jessy.transaction;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.store.Sample2EntityClass;
import fr.inria.jessy.store.SampleEntityClass;
import fr.inria.jessy.transaction.*;

public class SampleTransaction extends Transaction {

	public SampleTransaction(Jessy jessy) {
		super(jessy);
	}

	@Override
	public boolean execute() {
		try {
	
			SampleEntityClass se=new SampleEntityClass("1", "sampleentity1");
			Sample2EntityClass se2=new Sample2EntityClass("1", "sampleentity2");

			write(se);
			write(se2);
			
			SampleEntityClass readentity=read(SampleEntityClass.class, "1");
			
			if (readentity.getData()=="sampleentity1"){
				write(new Sample2EntityClass("2", "sampleentity2-2"));
			}
			
			return commitTransaction();			
		} catch (Exception ex) {
			return false;
		}		
	}

}
