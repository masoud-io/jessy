package fr.inria.jessy.benchmark.tpcc;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.benchmark.tpcc.entities.*;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;

import java.util.*;
import java.util.Random;

/**
 * @author WANG Haiyun & ZHAO Guang
 * 
 */

public class StockLevel extends Transaction {

	private String W_ID;
	private String D_ID;

	public StockLevel(Jessy jessy, int warhouseNumber, int districtNumber) throws Exception {
		super(jessy);
		W_ID = "" + warhouseNumber;
		D_ID = "" + districtNumber;
	}

	@Override
	public ExecutionHistory execute() {

		try {
			District district;
			Order order;
			Order_line ol = null;
			Stock stock;
			int threshold;
			int low_stock = 0;
			int i, j;

			/* Selection District */
			//this is where causes the null pointer problem, I have to use parser to make it right, or it returns null
			district = read(District.class, "D_W_"+Integer.parseInt(W_ID)+"_D_"+Integer.parseInt(D_ID));

			Random rand = new Random(System.currentTimeMillis());

			/*
			 * The threshold of minimum quantity in stock (threshold ) is
			 * selected at random within [10 .. 20]
			 */
			threshold = rand.nextInt(20 - 10) + 10;

			List<String> listItems = new ArrayList<String>();

			for (i = district.getD_NEXT_O() - 20; i < district.getD_NEXT_O(); i++) {

				/* Selection Order */
				order = read(Order.class, "O_W_" + W_ID + "_" + "O_D_" + D_ID
						+ "_" + "O_" + i);
				for (j = 1; j <= order.getO_OL_CNT(); j++) {

					/* Selection Order_line */
					ol = read(Order_line.class, "OL_W_" + W_ID + "_" + "OL_D_"
							+ D_ID + "_" + "OL_O_" + order.getO_ID() + "_"
							+ "OL_" + j);

					/* Stocks must be counted only for distinct items */
					if(ol != null)
						if (!listItems.contains(ol.getOL_I_ID())) {
	
							/* add into the item list */
							listItems.add(ol.getOL_I_ID());
	
							/* Selection Stock */
							stock = read(Stock.class, "S_W_" + W_ID + "_" + "S_I_"
									+ ol.getOL_I_ID());
							if (stock.getS_QUANTITY() < threshold)
								low_stock++;
						}

				}

			}

			return commitTransaction();

		} catch (Exception e) {
			e.printStackTrace();
			return abortTransaction();
		}

	}

}
