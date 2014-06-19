package probSampler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;

import dao.DAO;
import entity.Attribute;

public class ProbSampler {
	
	/**
	 * parameters need to be adjusted
	 */
	private int s1 = 2000;
	private int k = 100;
	private double C = 1.0 / 256.0;
	
	/**
	 * other var will be used
	 */
	private DAO dao = new DAO("uscensus", "usdatanoid", "ahsdb", "attrinfo");
	private ResultSet rs = dao.getInfo();
	private ArrayList<Attribute> attributes = new ArrayList<Attribute>();
	private HashMap<String, ArrayList<String>> conditions = new HashMap<String, ArrayList<String>>();
	private int queryCount = 0;
	private HashMap<String, String> path = new HashMap<String, String>();
	private RandomApproach ra = new RandomApproach();
	private HashMap<HashMap<String, String>, Integer> validQuerys = new HashMap<HashMap<String, String>, Integer>();
	
	public ProbSampler() {
		try {
			while (rs.next()) {

				ArrayList<String> values = new ArrayList<String>();
				String[] sValues = rs.getString(2).split(";");
				for (int i = 0; i < sValues.length; i++) {
					values.add(sValues[i]);
				}
				attributes.add(new Attribute(rs.getString(1), sValues.length));
				conditions.put(rs.getString(1), values);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		
		// first we dont need to do the random select when programming, so i present a sample database already
		dao.createSampleDB(attributes);

		
		// Use Random Approach to select.
		ArrayList<Integer> tmp = new ArrayList<Integer>();
		int tmpInt = 0;
		
		for (int i = 0; i < s1; i++) {
			tmp = ra.select(k, C, dao, rs, attributes, conditions, path, validQuerys);
			queryCount = queryCount + tmp.get(0);
			tmpInt = i + tmp.get(1);
			i = tmpInt - 1;
		}
		s1 = tmpInt;
		
	}
}
