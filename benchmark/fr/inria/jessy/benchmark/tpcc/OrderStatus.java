package fr.inria.jessy.benchmark.tpcc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.benchmark.tpcc.entities.Customer;
import fr.inria.jessy.benchmark.tpcc.entities.District;
import fr.inria.jessy.benchmark.tpcc.entities.Order;
import fr.inria.jessy.benchmark.tpcc.entities.Order_line;
import fr.inria.jessy.store.ReadRequestKey;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;

public class OrderStatus extends Transaction {
	
	private String W_ID;
	private String D_ID;
	private String C_W_ID; 
	private String C_D_ID; 
	private String C_ID;
	private String C_LAST;
	private String O_ID; 
	
	private int warhouseNumber;

	public OrderStatus(Jessy jessy, int warhouseNumber) throws Exception {
		super(jessy);
		this.warhouseNumber = warhouseNumber;
		// TODO Auto-generated constructor stub
	}

	@Override
	public ExecutionHistory execute() {
		
    	try {
    		District district;
        	Customer customer;
        	Order order;
        	Order_line ol;
        	NURand nur;
    		int i,j,random;
        	
    		String[] lastnames = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
    		
        	Random rand = new Random(System.currentTimeMillis());
        	int y = rand.nextInt(100-1)+1;  /* determine by C_LAST or by C_ID */
        	W_ID = Integer.toString(this.warhouseNumber);   /* warehouse number (W_ID) is constant  */
			
			D_ID = Integer.toString(rand.nextInt(10 - 1) + 1);  /* The district number (D_ID) is randomly selected within [1 ..10] */
			
			this.C_D_ID = D_ID;
			this.C_W_ID = W_ID;
			
			/* selection Customer */
			if(y>60) {   /* by C_ID */
				nur = new NURand(1023,1,3000);
        		C_ID = Integer.toString(nur.calculate());
        		customer = read(Customer.class, "C_W_"+C_W_ID + "_" + "C_D_"+C_D_ID + "_" + "C_"+C_ID);
			}
			else {       /* by C_LAST */
				nur = new NURand(255,0,999);
				random = nur.calculate();
				
        		C_LAST = lastnames[random/100]+lastnames[(random%100)/10]+lastnames[random%10];
       		
        		/* retrieve with SK C_LAST */
        		Collection<Customer> collection = null;
        		ReadRequestKey<String> request_C_W_ID = new ReadRequestKey<String>("C_W_ID", C_W_ID);
        		ReadRequestKey<String> request_C_D_ID = new ReadRequestKey<String>("C_D_ID", C_D_ID);
        		ReadRequestKey<String> request_C_LAST = new ReadRequestKey<String>("C_LAST", C_LAST);
     
        		List<ReadRequestKey<?>> request = new ArrayList<ReadRequestKey<?>>();
        		request.add(request_C_W_ID);
        		request.add(request_C_D_ID);
        		request.add(request_C_LAST);
        		collection = read(Customer.class, request); 
        		if (collection.size() == 0) {
					System.out.println("COLLECTION SIZE IS ZERO: ***  "
							+ request_C_W_ID.getKeyValue() + ":"
							+ request_C_D_ID.getKeyValue() + ":"
							+ request_C_LAST.getKeyValue());
					System.exit(0);
				} 
         		
         		List<Customer> list = new ArrayList<Customer>();
         		
         		/* Save the results in a List for sort */
         		list.addAll(collection);
                
         		if(list.size()>1) {    /* if more than one results */
         			
         			/* Sort the results by C_FIRST in ascending order */
            		for(int a=0;a<list.size();a++) {
                    	
                        int smallest=a;
                        int b;
                        for(b=a;b<list.size();b++) {
                        	
                            if(list.get(b).getC_FIRST().compareTo(list.get(smallest).getC_FIRST())<0) {
                                smallest=b;
                            }
                        }
                        swap(list,a,smallest);
                               
                    }
         		}
        		
        		/* the row at position n/2 of the results set is selected */
        		customer = list.get(list.size()/2);
			}
			/* should we make a READ operation on District? but we need D_Next_O_ID to determine the number of orders. 
			 * So there will be a involved selection operation on the District table not mentioned in the benchmark */
			int tmp = 0;			
			district = read(District.class, "D_W_"+ W_ID + "_" + "D_"+ D_ID);
			
			for(j=1;j<district.getD_NEXT_O();j++) {
				order = read(Order.class, "O_W_"+ C_W_ID + "_" + "O_D_"+ C_D_ID + "_" + "O_"+ j);
				if(order.getO_C_ID().equals(customer.getC_ID())) 
					tmp = j;
			}
			/* Selection of the most recent order */
			order = read(Order.class, "O_W_"+ C_W_ID + "_" + "O_D_"+ C_D_ID + "_" + "O_"+ tmp);
			
			/* Selection Order_line */
			for(i=1;i<=order.getO_OL_CNT();i++) {
				ol = read(Order_line.class, "OL_W_"+C_W_ID + "_" + "OL_D_"+C_D_ID + "_" + "OL_O_"+order.getO_ID() + "_" + "OL_"+i);
			}
			
			return commitTransaction();	
			
		} catch (Exception e) {
			e.printStackTrace();
			return abortTransaction();
		}	
	}
    	
    public void swap(List<Customer> list,int from,int to) {
    		
    	Customer tmp = list.get(from);
    	list.set(from, list.get(to));
    	list.set(to, tmp);
    		
    }

}
