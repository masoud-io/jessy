package fr.inria.jessy.benchmark.tpcc;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.entity.SampleEntityClass;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;

public class ReadAndWriteOnSampleEntity extends Transaction {

	
	
	private SampleEntityClass _se;
	
	private String _entityId ="1";

	public ReadAndWriteOnSampleEntity(Jessy jessy) throws Exception {
		super(jessy);

	}
	
	public SampleEntityClass readAndWrite(){
		execute();
		return _se;
	}

	@Override
	public ExecutionHistory execute() {

		try {
			
			System.out.println("reading SampleEntityClass 1");
			_se=read(SampleEntityClass.class, _entityId);			
			System.out.println("readed, SampleEntityClass 1");
			
			System.out.println("updating SampleEntityClass");
			write(_se);
			System.out.println("SampleEntityClass updated");

			return commitTransaction();

		} catch (Exception e) {
			e.printStackTrace();
			
			return abortTransaction();
		}
	}	
	
}
