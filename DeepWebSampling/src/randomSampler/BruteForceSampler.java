package randomSampler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;

import dao.DAO;
import entity.Attribute;

public class BruteForceSampler {
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int sizeOfRequired = 100;
		int k = 10;
		boolean loopFlag = true;
		DAO dao = new DAO("uscensus", "allusdata", "bfsdb", "attrinfo");
		ResultSet rs = dao.getInfo();
		ArrayList<Attribute> attributes = new ArrayList<Attribute>();
		Hashtable<String, ArrayList<String>> conditions = new Hashtable<String, ArrayList<String>>();
		Hashtable<String, String> path = new Hashtable<String, String>();
		Hashtable<Integer, String> ht = new Hashtable<Integer, String>();
		
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
			try {
				while (rs.next()) {
					values.add(rs.getString(1));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			conditions.put(attributes.get(i).getName(), values);
		}
		**/
		dao.createSampleDB(attributes);

		// Let's begin the real deal =.=
		int queryCount = 0;
		do {
			rs = dao.bruteForceSelect(attributes, conditions, path);
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
				loopFlag = dao.save2SampleDB(rs, sizeOfRequired, 1.0, ht);
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
		// System.out.println(conditions.get(attributes.get(0)));
	}
}
