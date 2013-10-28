package probSampler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;

import tree.EdgeData;
import tree.NodeData;
import alertHybrid.AlertOrder;
import config.SlotMachine;
import config.Util;
import dao.DAO;
import edu.uci.ics.jung.graph.DelegateTree;
import entity.Attribute;

public class ProbSampler {
	static int queryCount = 0;
	static int queryByAH = 0;
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int sizeOfRequired = 10000;
		int k = 100;
		double C = 1.0/256.0;
		int s1 = 2000;
		int cs = 5;
		DAO dao = new DAO("uscensus", "usdatanoid", "psdb", "attrinfo");
		ResultSet rs = dao.getInfo();
		Util u = new Util();
		Hashtable<String, Hashtable<String, Integer>> preProInfo = new Hashtable<String, Hashtable<String, Integer>>();
		SlotMachine sm = new SlotMachine();
		ArrayList<Attribute> attributes = new ArrayList<Attribute>();
		Hashtable<String, ArrayList<String>> conditions = new Hashtable<String, ArrayList<String>>();
		//DelegateTree<NodeData, EdgeData> graphTree = new DelegateTree<NodeData, EdgeData>();
		Hashtable<String, String> path = new Hashtable<String, String>();
		AlertOrder ao = new AlertOrder();
		int allCount = 0;
		
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
		
		dao.createSampleDB(attributes);
		
		// Step 1 to 3
		// Use Alert-Order to select and estimate u and uj.
		ArrayList<Integer> tmp = new ArrayList<Integer>();
		for (int i = 0; i < s1; i++) {
			tmp = ao.select(k, C, dao, rs, attributes, conditions, path);
			queryCount = queryCount + tmp.get(0);
			i = i + tmp.get(0);
		}
		
		// after we select some records, we need to update the u and uj information
		// Let's begin the real deal =.=
		// first I need estimate all uj
		preProInfo.clear();
		allCount = s1;
		/**
		rs = dao.getAHCount("*", "*");
		try {
			while (rs.next()) {
				allCount = rs.getInt(1);
				//System.out.println("allcount :" + allCount);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		**/	
		ArrayList<String> tmpList = new ArrayList<String>();
		//Hashtable<String, Integer> tmpTable = new Hashtable<String, Integer>();
		String tmpAttribute = "";
		String tmpValue = "";
		for (int i = 0; i < attributes.size(); i++) {
			Hashtable<String, Integer> tmpTable = new Hashtable<String, Integer>();
			tmpAttribute = attributes.get(i).getName();
			
			tmpList = conditions.get(tmpAttribute);
			for (int j = 0; j < tmpList.size(); j++) {
				tmpValue = tmpList.get(j);
				rs = dao.getAHCount(tmpAttribute, tmpValue);
				
				try {
					while (rs.next()) {
						tmpTable.put(tmpValue, rs.getInt(1));
						//System.out.println("count: " + i + " " + tmpAttribute + ", Value Count: " + tmpList.size() + " " + tmpValue + " " + rs.getInt(1));
						
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
					
				//preProInfo.put(attributes.get(i), tmpTable);
			}
			preProInfo.put(tmpAttribute, tmpTable);
		}		
		
		
		
		System.out.println("********************************************************************");
		Hashtable<String, Hashtable<String, Integer>> preProInfoTran;
		
		for (int i = 1; i <= sizeOfRequired - s1; i++) {
			allCount = i + s1 - 1;
			preProInfoTran = (Hashtable<String, Hashtable<String, Integer>>) preProInfo.clone();
			path.clear();
			//HybridSAMP(sizeOfRequired-i+1, new DelegateTree<NodeData, EdgeData>(), preProInfoTran, preProInfo, u, allCount, k, null, dao, conditions, rs, sm, path, cs, ao, C, attributes);
			System.out.println("SampleDB count: " + (i + s1));
		}
		System.out.println("Query: " + queryCount);
		System.out.println("Query2: " + queryByAH);
	}
	
	public void probSAMP() {
		
	}

}
