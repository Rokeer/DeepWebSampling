package hiddenDBSampler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;

import config.MyAttributeCompare;
import dao.DAO;
import entity.Attribute;

public class HiddenDBSampler {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int sizeOfRequired = 2000;
		int k = 50;
		double C = 1.0/128.0;
		boolean loopFlag = true;
		DAO dao = new DAO("uscensus", "usdatanoid", "hdbsdb", "attrinfo");
		ResultSet rs = dao.getInfo();
		ArrayList<Attribute> attributes = new ArrayList<Attribute>();
		Hashtable<String, ArrayList<String>> conditions = new Hashtable<String, ArrayList<String>>();
		Hashtable<String, String> path = new Hashtable<String, String>();
		try {
			while (rs.next()) {
				
				ArrayList<String> values = new ArrayList<String>();
				String[] sValues = rs.getString(2).split(";");
				int count = 0;
				for (int i = 0; i < sValues.length; i++) {
					values.add(sValues[i]);
					count = count + 1;
				}
				attributes.add(new Attribute(rs.getString(1), count));
				conditions.put(rs.getString(1), values);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		/**
		try {
			while (rs.next()) {
				attributes.add(new Attribute(rs.getString(1)));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		for (int i = 0; i < attributes.size(); i++) {
			rs = dao.getValues(attributes.get(i).getName());
			ArrayList<String> values = new ArrayList<String>();
			int count = 0;
			try {
				while (rs.next()) {
					values.add(rs.getString(1));
					count = count + 1;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			attributes.get(i).setHasNodes(count);
			conditions.put(attributes.get(i).getName(), values);
		}
		**/
		dao.createSampleDB(attributes);

		// Let's begin the real deal =.=
		// make attributes in increase order
		MyAttributeCompare comparator = new MyAttributeCompare();
		Collections.sort(attributes, comparator);
		
		// Yeah! Let's working on data!
		int queryCount = 0;
		do {
			rs = dao.hiddenDBSelect(attributes, conditions, path, k);
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
				loopFlag = dao.save2SampleDB(rs, sizeOfRequired, probability);
				path.clear();
			} else if (rowCount > k) {
				// overflow
				System.out.println("Overflow");
			} else if (rowCount == 0) {
				// underflow
				System.out.println("Underflow");
				path.clear();
			}

		} while (loopFlag);
		System.out.println("Query: " + queryCount);
		/**
		for (int j = 0; j < attributes.size(); j++) {
			System.out.println(attributes.get(j).getName() + " "
					+ attributes.get(j).getHasNodes());
		}
		**/
	}

}
