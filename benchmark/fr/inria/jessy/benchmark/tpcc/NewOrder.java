package fr.inria.jessy.benchmark.tpcc;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.benchmark.tpcc.entities.*;
import fr.inria.jessy.transaction.*;

import java.util.*;

public class NewOrder extends Transaction {
	
	public NewOrder(Jessy jessy) throws Exception{
		super(jessy);
	}

	@Override
	public ExecutionHistory execute() {
		try {
			int i, j, k;
			Random rand = new Random(System.currentTimeMillis());			
			Warehouse wh;			
			District dis;			
			Customer cus;			
			Item it;
			

			
			return commitTransaction();			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		
	}

}
