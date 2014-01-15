package probSampler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;

import alertHybrid.AlertOrder;
import config.GraphML;
import config.SlotMachine;
import config.Util;
import dao.DAO;
import edu.uci.ics.jung.graph.DelegateTree;
import edu.uci.ics.jung.graph.DirectedGraph;
import edu.uci.ics.jung.graph.DirectedSparseMultigraph;
import edu.uci.ics.jung.graph.UndirectedGraph;
import edu.uci.ics.jung.graph.UndirectedSparseMultigraph;
import entity.Attribute;
import entity.Edge;
import entity.Node;

public class ProbSampler {

	private int sizeOfRequired = 10000;
	private int k = 100;
	private double C = 1.0 / 256.0;
	private int s1 = 2000;
	private int cs = 5;
	private int queryCount = 0;
	private DAO dao = new DAO("uscensus", "usdatanoid", "cdtsdb", "attrinfo");
	private ResultSet rs = dao.getInfo();
	private Util u = new Util();
	private SlotMachine sm = new SlotMachine();
	private UndirectedGraph<Node, Edge> graph = new UndirectedSparseMultigraph<Node, Edge>();
	private Hashtable<Integer, String> localDB = new Hashtable<Integer, String>();
	private ArrayList<Attribute> attributes = new ArrayList<Attribute>();
	private Hashtable<String, ArrayList<String>> conditions = new Hashtable<String, ArrayList<String>>();
	private Hashtable<String, Node> nodes = new Hashtable<String, Node>();
	private Hashtable<String, String> path = new Hashtable<String, String>();
	private AlertOrder ao = new AlertOrder();
	private GraphML gml = new GraphML();
	
	private DelegateTree<Node, Edge> searchTree;
	//private int allCount = 0;
	private int estTotal = 0;
	
	public ProbSampler() {
		// TODO Auto-generated method stub
		

		
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
		
		//searchTree = gml.createSearchTree(attributes, conditions);
		
		/**
		dao.createSampleDB(attributes);

		
		// Use Alert-Order to select.
		ArrayList<Integer> tmp = new ArrayList<Integer>();
		int tmpInt = 0;
		
		for (int i = 0; i < s1; i++) {
			tmp = ao.select(k, C, dao, rs, attributes, conditions, path);
			queryCount = queryCount + tmp.get(0);
			tmpInt = i + tmp.get(1);
			i = tmpInt - 1;

		}
		//path.put("iRvetserv", "6");
		//path.put("iRrelchld", "0");
		//path.put("iWorklwk", "0");
		s1 = tmpInt;
		**/
		
		
		//allCount = s1;
		// Let's begin the real deal =.=
		
		
		// these path should from random sampler
		// since I didn't run random sampler so far
		// we add some path manually for the first time
		path.put("dAge", "4");
		path.put("dAncstry1", "1");
		path.put("iClass", "1");
		//path.put("iFertil", "4");
		path.put("dHour89", "4");
		//path.put("dHours", "4");
		
		estAll (path); //est database total
		path.clear();
		getEstValidQueryList(nodes, path);
		
		/**
		for (int i = 0; i < sizeOfRequired - s1; i++) {
			//allCount = i + s1;
			estAll (path); //est database total
			path.clear();
			getEstValidQueryList(nodes, path, estTotal);
			
			System.out.println("SampleDB count: " + (i + s1));
		}
		System.out.println("Query: " + queryCount);
		**/
	}
	
	@SuppressWarnings("unchecked")
	public void getEstValidQueryList(Hashtable<String, Node> nodes, Hashtable<String, String> path) {
		
		int tmpNum = 0;
		double probOfSelect = 0.0;
		String[] tmpStr;
		Hashtable<String, String> tmpPath = (Hashtable<String, String>) path.clone();
		//Hashtable<String, Node> tmpNodes = (Hashtable<String, Node>) nodes.clone();
		for (String key : nodes.keySet()) {
			
			tmpStr = key.split("\\*");
			
			if (!tmpPath.containsKey(tmpStr[0])) {
				tmpPath.put(tmpStr[0], tmpStr[1]);
				System.out.println(tmpStr[0]+"="+tmpStr[1]);
				probOfSelect = gml.getProbOfSelect(graph, this.nodes, tmpPath);
				tmpNum = (int) ((estTotal * 1.0) * probOfSelect);
				if (tmpNum > k / 2) {
					Hashtable<String, Node> tmpNodes = (Hashtable<String, Node>) nodes.clone();
					tmpNodes.remove(key);
					getEstValidQueryList(tmpNodes, tmpPath);
				} else if (tmpNum == 0) {
					System.out.println("-");
					return;
				} else {
					tmpPath.remove(tmpStr[0]);
					String sPath = "*";
					for (String pathKey : tmpPath.keySet()) {
						sPath = sPath + pathKey + " = " + tmpPath.get(pathKey) + ", ";
					}
					System.out.println(sPath.substring(0, sPath.length()-2));
					return;
				}
			}
			
		}
	}
	
	public void estAll (Hashtable<String, String> path) {
		rs = dao.getSample("*", "*");
		long a = System.currentTimeMillis();
		gml.getGraph(rs, nodes, graph, localDB);
		long b = System.currentTimeMillis();
		System.out.println(b-a);
		//graph = gml.getGraph(rs, nodes, graph, localDB);
		double probOfSelect = 0.0;
		int probPathCount = 0;
		probOfSelect = gml.getProbOfSelect(graph, nodes, path);
		rs = dao.getProbPathCount(path);
		try {
			while (rs.next()) {
				probPathCount = rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		estTotal = (int) (probPathCount / probOfSelect);
		System.out.println("Sample count = " + probPathCount + ", estTotal = " + estTotal);
	}

}
