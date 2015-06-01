package alertHybrid;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;

import config.MyAttributeCompare;
import dao.DAO;
import entity.Attribute;

public class AlertOrder {
	public AlertOrder() {
		
	}
	
	
	@SuppressWarnings("unchecked")
	public ArrayList<Integer> select(int k, double C, DAO dao, ResultSet rs,
			ArrayList<Attribute> attributes,
			Hashtable<String, ArrayList<String>> conditions,
			Hashtable<String, String> path) {
		//Hashtable<String, String> path = (Hashtable<String, String>) oriPath.clone();
		boolean loopFlag = true;
		ArrayList<Integer> result = new ArrayList<Integer>();
		int queryCount = 0;
		int resultCount = 0;
		// Let's begin the real deal =.=
		// make attributes in increase order
		//MyAttributeCompare comparator = new MyAttributeCompare();
		//Collections.sort(attributes, comparator);
		
		// Yeah! Let's working on data!
		//int queryCount = 0;
		do {
			//rs = dao.alertOrderSelect(attributes, conditions, path, k);
			rs = dao.randomSelect(attributes, conditions, path, k);
			queryCount = queryCount + 1;
			int rowCount = 0;
			try {
				rs.last();
				rowCount = rs.getRow();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (rowCount <= k && rowCount > 0) {
				// valid query
				System.out.println("Valid query");
				
				double probability = C * rowCount * Math.pow(2.0, path.size()-1.0);
				if (probability > 1.0) {
					probability = 1.0;
				}
				loopFlag = false;
				resultCount = dao.save2SampleDBAOver(rs, probability, rowCount);
				//path = (Hashtable<String, String>) oriPath.clone();
				//path.clear();
			} else if (rowCount > k) {
				// overflow
				System.out.println("Overflow");
			} else if (rowCount == 0) {
				// underflow
				System.out.println("Underflow");
				//path = (Hashtable<String, String>) oriPath.clone();
				path.clear();
			}

		} while (loopFlag);
		result.add(queryCount);
		result.add(resultCount);
		return result;
	}
	
}
