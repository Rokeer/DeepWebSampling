package probSampler;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import config.MyAttributeCompare;
import config.Util;
import dao.DAO;
import edu.uci.ics.jung.graph.UndirectedGraph;
import entity.Attribute;
import entity.Edge;
import entity.Node;

public class RandomApproach {
	public RandomApproach() {
		
	}
	
	@SuppressWarnings("unchecked")
	public ArrayList<Integer> select(int k, double C, DAO dao, ResultSet rs,
			ArrayList<Attribute> attributes,
			HashMap<String, ArrayList<String>> conditions,
			HashMap<String, String> path,
			HashMap<HashMap<String, String>, Integer> validQueries,
			ArrayList<HashMap<String, String>> underQueries,
			int sampleSize,
			UndirectedGraph<Node, Edge> graph, HashMap<String, Node> nodes) {
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
			// 1 is original method, 2 is select a node all over the graph
			Util.randomSelectAnotherAttributeValue(attributes, conditions, path, underQueries, nodes, 2);
			rs = dao.doSelect(path, k);
			
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
				validQueries.put((HashMap<String, String>) path.clone(), rowCount);
				double probability = C * rowCount * Math.pow(2.0, path.size()-1.0);
				if (probability > 1.0) {
					probability = 1.0;
				}
				loopFlag = false;
				Util.updateGraph(attributes, rs, nodes, graph, sampleSize+rowCount);
				resultCount = dao.save2SampleDBRandom(rs, probability);
				//path = (Hashtable<String, String>) oriPath.clone();
				path.clear();
			} else if (rowCount > k) {
				// overflow
				System.out.println("Overflow");
			} else if (rowCount == 0) {
				// underflow
				underQueries.add((HashMap<String, String>) path.clone());
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
