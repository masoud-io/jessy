package fr.inria.jessy.transaction;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.store.Sample2EntityClass;
import fr.inria.jessy.store.SampleEntityClass;
import fr.inria.jessy.transaction.*;

public class SampleTransaction2 extends Transaction {

	public SampleTransaction2(Jessy jessy) throws Exception{
		super(jessy);
	}

	@Override
	public boolean execute() {
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
			return false;
		}		
	}

}
