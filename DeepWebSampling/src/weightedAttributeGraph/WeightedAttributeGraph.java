package weightedAttributeGraph;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;



import java.util.Set;

import config.Util;
import config.WAGA;
import dao.DAO;
import edu.uci.ics.jung.graph.UndirectedGraph;
import entity.Attribute;
import entity.Edge;
import entity.Node;

public class WeightedAttributeGraph {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int sizeOfRequired = 2000;
		int k = 50;
		double a = 0.2;
		double b = 2.0;
		boolean loopFlag = true;
		DAO dao = new DAO("uscensus", "usdatanoid", "wagsdb", "attrinfo");
		WAGA waga = new WAGA();
		Util util = new Util();
		UndirectedGraph<Node, Edge> graph;
		ResultSet rs = dao.getInfo();
		Hashtable<String, Node> nodes = new Hashtable<String, Node>();
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
		// Step 3, 4
		dao.createSampleDB(attributes);
		
		int queryCount = 0;
		int rowCount = 0;
		//dao.initSelect(attributes, conditions);
		dao.randomSelect(attributes, conditions, path, k);
		do {

			
			rs = dao.weightedAttributeGraphSelect(path, k);
			queryCount = queryCount + 1;
			
			try {
				rs.last();
				rowCount = rs.getRow();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// Step 5
			// although lian said this will save time, it is useless when data set is small.
			// didn't test on large data set
			//dao.save2LocalDB(attributes, rs);

			// Step 6
			
			graph = waga.createGraph(rs, nodes, k);
			System.out.println("Graph Vertex count: " + graph.getVertexCount() + " Edge count: " + graph.getEdgeCount());

			double maxS = 0;
			double tmpS = 0;
			Node maxNode = null;
			
			
			// Step 7, 8, 9
			boolean damn = false;
			do {
			maxS = 0;
			tmpS = 0;
			for (Node n : graph.getVertices()) {
				tmpS = util.calculateS(graph, n, a, b);
				//System.out.println("Node: " + n.getAttribute() + " = " + n.getValue() + " S = " + tmpS);
				if (tmpS > maxS) {
					maxS = tmpS;
					maxNode = n;
				}
			}
			if (damn) {
				System.out.println("oops");
			}
			damn = true;
			
			// Step 10
			maxNode = nodes.get(maxNode.getAttribute()+maxNode.getValue());
			maxNode.setUsedTime(maxNode.getUsedTime() + 1);
			System.out.println("MaxNode: " + maxNode.getAttribute() + " = " + maxNode.getValue() + " Used Time: " + nodes.get(maxNode.getAttribute()+maxNode.getValue()).getUsedTime());
			
			} while (path.containsKey(maxNode.getAttribute()));
			//maxNode.setUsedTime(maxNode.getUsedTime() + 1);

			

			// Step 11 to 17
			if (rowCount <= k && rowCount > 0) {
				// valid query
				System.out.println("Valid query");
				loopFlag = dao.save2SampleDB(rs, sizeOfRequired, 1.0);
				path.clear();
			} else if (rowCount > k) {
				// overflow
				System.out.println("Overflow");
			} else if (rowCount == 0) {
				// underflow
				System.out.println("Underflow");
				Set<String> keys = path.keySet();
				Iterator<String> itr = keys.iterator();
				path.remove(itr.next());
			}
			path.put(maxNode.getAttribute(), maxNode.getValue());
			
		} while (loopFlag);
		System.out.println("Query: " + queryCount);
	}
	

}
