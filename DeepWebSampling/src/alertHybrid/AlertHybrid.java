package alertHybrid;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;

import tree.EdgeData;
import tree.NodeData;
import config.SlotMachine;
import config.Util;
import dao.DAO;
import edu.uci.ics.jung.graph.DelegateTree;
import entity.Attribute;

public class AlertHybrid {
	static int queryCount = 0;
	static int queryByAH = 0;
	static int hey = 0;
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int sizeOfRequired = 10000;
		int k = 100;
		double C = 1.0/256.0;
		int s1 = 2000;
		int cs = 20;
		DAO dao = new DAO("uscensus", "usdatanoid", "ahsdb", "attrinfo");
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
			i = i + tmp.get(1) - 1;
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
		int addedResult = 0;
		for (int i = 1; i <= sizeOfRequired - s1; i++) {
			allCount = i + s1 - 1;
			preProInfoTran = (Hashtable<String, Hashtable<String, Integer>>) preProInfo.clone();
			path.clear();
			addedResult = HybridSAMP(sizeOfRequired-i+1, new DelegateTree<NodeData, EdgeData>(), preProInfoTran, preProInfo, u, allCount, k, null, dao, conditions, rs, sm, path, cs, ao, C, attributes);
			i = i + addedResult - 1;
			System.out.println("SampleDB count: " + (i + s1));
		}
		System.out.println("Query: " + queryCount);
		System.out.println("Query2: " + queryByAH);
		System.out.println(hey);
	}
	
	public static int HybridSAMP(int s,
			DelegateTree<NodeData, EdgeData> graphTree,
			Hashtable<String, Hashtable<String, Integer>> preProInfoTran,
			Hashtable<String, Hashtable<String, Integer>> preProInfo, Util u,
			int allCount, int k, NodeData current, DAO dao,
			Hashtable<String, ArrayList<String>> conditions, ResultSet rs,
			SlotMachine sm, Hashtable<String, String> path,
			int cs, AlertOrder ao, double C, ArrayList<Attribute> attributes) {

		int tmpCount = 0;
		int rowCount = 0;
		
		// Step 7 to 11
		rs = dao.alertHybridCheck(path, cs);
		try {
			rs.last();
			rowCount = rs.getRow();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(queryCount);
		if (rowCount < cs) {
			hey = hey + 1;
			// select and update preProInfo
			ArrayList<Integer> tmp = new ArrayList<Integer>();
			tmp = ao.select(k, C, dao, rs, attributes, conditions, path);
			queryCount = queryCount + tmp.get(0);
			
			//ao.select(k, C, dao, rs, attributes, conditions, path, tmpCount);
			preProInfo.clear();
			
			/**
			rs = dao.getAHCount("*", "*");
			try {
				while (rs.next()) {
					allCount = rs.getInt(1);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			**/
			
			allCount = allCount + 1;
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

			System.out.println("BANG!");
			return tmp.get(1);
		}
		if (graphTree.getVertexCount() == 0) {
			String root = u.calculateRLC(preProInfoTran, allCount, s, k,
					allCount);
			current = new NodeData(root, allCount);
			graphTree.setRoot(current);
			
			
			// step 8
			tmpCount = 0;
			for (int i = 0; i < conditions.get(current.getAttribute()).size() - 1; i++) {
				rs = dao.getAHPathCount(current.getAttribute(),
						conditions.get(current.getAttribute()).get(i), path);
				try {
					while (rs.next()) {
						tmpCount = tmpCount + rs.getInt(1);
						//System.out.println(conditions.get(current.getAttribute()).get(i)+" "+rs.getInt(1));
						graphTree.addChild(new EdgeData(conditions.get(current.getAttribute()).get(i), rs.getInt(1)), current, new NodeData("", rs.getInt(1)));
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			// no need 2 queury
			graphTree.addChild(new EdgeData(conditions.get(current.getAttribute()).get(conditions.get(current.getAttribute()).size() - 1), current.getCount() - tmpCount), current, new NodeData("", current.getCount() - tmpCount));
			
			
			current.setIsDone(true);
		} else if (current == null) {
			current = graphTree.getRoot();
		}
		if (!current.getIsDone()) {
			// calculateRLC and update this node
			current.setAttribute(u.calculateRLC(preProInfoTran, allCount, s, k,
					current.getCount()));
			
			
			// step 8
			tmpCount = 0;
			for (int i = 0; i < conditions.get(current.getAttribute()).size() - 1; i++) {
				rs = dao.getAHPathCount(current.getAttribute(),
						conditions.get(current.getAttribute()).get(i), path);
				try {
					while (rs.next()) {
						tmpCount = tmpCount + rs.getInt(1);
						graphTree.addChild(new EdgeData(conditions.get(current.getAttribute()).get(i), rs.getInt(1)), current, new NodeData("", rs.getInt(1)));
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			// no need 2 queury
			graphTree.addChild(new EdgeData(conditions.get(current.getAttribute()).get(conditions.get(current.getAttribute()).size() - 1), current.getCount() - tmpCount), current, new NodeData("", current.getCount() - tmpCount));
			
			
			current.setIsDone(true);
		}
		
		// prevent useless calculation
		preProInfoTran.remove(current.getAttribute());
		
		
		// Step 9
		EdgeData edge = sm.chanceSelect(graphTree.getChildEdges(current), current.getCount());
		NodeData next = graphTree.getOpposite(current, edge);
		// add2path
		path.put(current.getAttribute(), edge.getValue());
		
		rs = dao.alertHybridSelect(path, k);
		queryCount = queryCount + 1;
		queryByAH = queryByAH + 1;
		rowCount = 0;
		try {
			rs.last();
			rowCount = rs.getRow();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (rowCount > k && preProInfoTran.size() > 0) {
			System.out.println("Overflow");
			//System.out.println("Next Node: " + next.getAttribute()+" "+next.getCount()+" "+next.getIsDone());
			return HybridSAMP(s, graphTree, preProInfoTran, preProInfo, u, allCount, k, next, dao, conditions, rs, sm, path, cs, ao, C, attributes);
			
		} else if (preProInfoTran.size() == 0){
			System.out.println("Overflow, but all attributes are set");
			dao.countDecisionTreeSelect(path);
			return 1;
		} else {
			System.out.println("Valid query");
			dao.countDecisionTreeSelect(path);
			return 1;
		}
		
	}
	
	

}
