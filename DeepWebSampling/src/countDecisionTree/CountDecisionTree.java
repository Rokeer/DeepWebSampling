package countDecisionTree;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;

import config.SlotMachine;
import config.Util;
import tree.EdgeData;
import tree.NodeData;
import dao.DAO;
import edu.uci.ics.jung.graph.DelegateTree;
import entity.Attribute;

public class CountDecisionTree {

	static int queryCount = 0;
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int sizeOfRequired = 2000;
		int k = 50;
		DAO dao = new DAO("uscensus", "usdatanoid", "cdtsdb", "attrinfo");
		ResultSet rs = dao.getInfo();
		Util u = new Util();
		Hashtable<String, Hashtable<String, Integer>> preProInfo = new Hashtable<String, Hashtable<String, Integer>>();
		SlotMachine sm = new SlotMachine();
		//Hashtable<Attribute, Hashtable<String, Integer>> preProInfo = new Hashtable<Attribute, Hashtable<String, Integer>>();
		ArrayList<Attribute> attributes = new ArrayList<Attribute>();
		Hashtable<String, ArrayList<String>> conditions = new Hashtable<String, ArrayList<String>>();
		DelegateTree<NodeData, EdgeData> graphTree = new DelegateTree<NodeData, EdgeData>();
		Hashtable<String, String> path = new Hashtable<String, String>();
		System.out.println("checkpoint 0");
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
		System.out.println("checkpoint 1");
		dao.createSampleDB(attributes);
		System.out.println("checkpoint 1.5");
		// Let's begin the real deal =.=
		// first I need estimate all uj
		rs = dao.getCount("*", "*");
		int allCount = 0;
		try {
			while (rs.next()) {
				allCount = rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
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
				rs = dao.getCount(tmpAttribute, tmpValue);
				
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
		System.out.println("checkpoint 2");
		// pre process finish, new we can est uj via |u|*(count(ai=vi)/count(*))
		// but in calculating, i didn't calculate l(u,k). we considered this not influence the result
		
		// init a root node
		
		/**
		Set<String> set = preProInfo.keySet();
		Iterator<String> itr = set.iterator();
		String lalala = "";
		String lasodo = "";
		while (itr.hasNext()) {
			lalala = itr.next();
			Hashtable<String, Integer> dododo = preProInfo.get(lalala);
			Set<String> doset = dododo.keySet();
			Iterator<String> doitr = doset.iterator();
			while (doitr.hasNext()) {
				lasodo = doitr.next();
				System.out.println(lalala + " " + lasodo + " " + dododo.get(lasodo));
			}
		}
		**/
		
		
		
		Hashtable<String, Hashtable<String, Integer>> preProInfoTran;
		System.out.println("checkpoint 3");
		for (int i = 1; i <= sizeOfRequired; i++) {
			preProInfoTran = (Hashtable<String, Hashtable<String, Integer>>) preProInfo.clone();
			path.clear();
			dtSAMP(sizeOfRequired-i+1, graphTree, preProInfoTran, u, allCount, k, null, dao, conditions, rs, sm, path);
			System.out.println("SampleDB count: " + i);
		}
		System.out.println("Query: " + queryCount);
	}
	
	public static void dtSAMP(int s,
			DelegateTree<NodeData, EdgeData> graphTree,
			Hashtable<String, Hashtable<String, Integer>> preProInfoTran,
			Util u, int allCount, int k, NodeData current, DAO dao,
			Hashtable<String, ArrayList<String>> conditions, ResultSet rs, SlotMachine sm, Hashtable<String, String> path) {
		
		/**
		Set<String> set = conditions.keySet();
		Iterator<String> itr = set.iterator();
		String lalala = "";
		while (itr.hasNext()) {
			lalala = itr.next();
			ArrayList<String> dododo = conditions.get(lalala);
			for (int i = 0; i < dododo.size(); i++) {
				System.out.println(lalala + " " + dododo.get(i)); 
			}
		}
		**/
		
		int tmpCount = 0;
		// Step 1 to 7
		if (graphTree.getVertexCount() == 0) {
			String root = u.calculateRLC(preProInfoTran, allCount, s, k,
					allCount);
			current = new NodeData(root, allCount);
			graphTree.setRoot(current);
			
			// step 8
			tmpCount = 0;
			for (int i = 0; i < conditions.get(current.getAttribute()).size() - 1; i++) {
				queryCount = queryCount + 1;
				rs = dao.getPathCount(current.getAttribute(),
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
			/**
			if (current.getAttribute() == null) {
				System.out.println("wtf");
			}
			System.out.println(current.getAttribute());
			**/
			for (int i = 0; i < conditions.get(current.getAttribute()).size() - 1; i++) {
				queryCount = queryCount + 1;
				rs = dao.getPathCount(current.getAttribute(),
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
		
		
		
		if (next.getCount() > k && preProInfoTran.size() > 0) {
			System.out.println("Overflow");
			dtSAMP(s, graphTree, preProInfoTran, u, allCount, k, next, dao, conditions, rs, sm, path);
		} else if (preProInfoTran.size() == 0){
			System.out.println("Overflow, but all attributes are set");
			dao.countDecisionTreeSelect(path);
		} else {
			System.out.println("Valid query");
			dao.countDecisionTreeSelect(path);
		}
		
		
	}

}
