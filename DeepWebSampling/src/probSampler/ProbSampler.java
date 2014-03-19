package probSampler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Random;

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


	double a = 0.2;
	private int m = 10;
	private int sizeOfRequired = 10000;
	private int k = 100;
	private double C = 1.0 / 256.0;
	private int s1 = 2000;
	private int cs = 5;
	private int queryCount = 0;
	private DAO dao = new DAO("uscensus", "usdatanoid", "ahsdb", "attrinfo");
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
		// first we dont need to do the random select when programming, so i present a sample database already
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
		path.put("dAncstry1", "0");
		path.put("iClass", "1");
		path.put("dHours", "0");
		path.put("iWorklwk", "0");
		
		ArrayList<Hashtable<String, String>> candidatePaths = new ArrayList<Hashtable<String, String>>();
		int probPathCount = 0;
		
		do {
			estAllWithSelect(path);
			estAll (path); //est database total
			path.clear();
			candidatePaths.clear();
			while (candidatePaths.size() < m){
				candidatePaths.add(getEstValidQuery(nodes));
			}
			path = getBestPath(candidatePaths);
			
			double probOfSelect = gml.getProbOfSelect(graph, this.nodes, path);
			int estNum = (int) ((estTotal * 1.0) * probOfSelect);
			System.out.println(estNum);
			
			rs = dao.getProbPathCount(path, 1);
			try {
				while (rs.next()) {
					probPathCount = rs.getInt(1);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
			for (String key : path.keySet()) {
				nodes.get(key + "*" + path.get(key)).addQueryTime();;
			}
			
			if(probPathCount < k) {
				rs = dao.weightedAttributeGraphSelect(path, k);
				queryCount = queryCount + 1;
				dao.save2SampleDB(rs, sizeOfRequired, 1.0, localDB);
			} else {
				ao.select(k, C, dao, rs, attributes, conditions, path);
			}
			
			rs = dao.getAHCount("*", "*");
			try {
				while (rs.next()) {
					probPathCount = rs.getInt(1);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		} while (probPathCount >= k);
		
		
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
	
	public Hashtable<String, String> getBestPath(ArrayList<Hashtable<String, String>> candidatePaths) {
		double bestScore = 0.0;
		double currentScore = 0.0;
		int bestI = 0;
		for (int i = 0; i < candidatePaths.size(); i++) {
			currentScore = calScore(candidatePaths.get(i));
			if (bestScore < currentScore) {
				bestScore = currentScore;
				bestI = i;
			}
		}
		return candidatePaths.get(bestI);
	}
	
	
	public double calScore (Hashtable<String, String> path) {
		double score = 0.0;
		for (String key : path.keySet()) {
			score = score + calculateS(nodes.get(key + "*" + path.get(key)), a);
		}
		score = score / (path.size() * (1.0));
		return score;
	}
	
	public double calculateS(Node n, double a) {
		double d = 0.0;
		//Collection<Edge> edgeSet = graph.getOutEdges(n);
		for (Edge e : graph.getOutEdges(n)) {
			d = d + (double) e.getWeight();
		}
		//double d = (double) graph.degree(n);
		double t = (double) n.getQueryTime();
		double h = calculateH(n, d);
		double s = d * Math.pow(a, t) * h;
		return s;
	}

	public double calculateH(Node n, double d) {
		double h = 0.0;
		double p = 0.0;

		for (Node node : graph.getNeighbors(n)) {
			p = ((double) graph.findEdge(n, node).getWeight()) / d;
			h = h + ((p * Math.log(p) / Math.log(2.0)));
		}

		h = h * -(1.0);
		return h;
	}
	
	public Hashtable<String, String> getEstValidQuery(Hashtable<String, Node> nodes) {
		Hashtable<String, String> path = new Hashtable<String, String> ();
		Random random = new Random();
		int estNum = estTotal;
		double probOfSelect = 0.0;
		int attCount = 0;
		int valueCount = 0;
		String attribute = "";
		String value = "";
		do {
			do {
				attCount = Math.abs(random.nextInt() % attributes.size());
			
				attribute = attributes.get(attCount).getName();
				valueCount = Math.abs(random.nextInt()
						% conditions.get(attribute).size());
				value = conditions.get(attribute).get(valueCount);
			} while (nodes.get(attribute +"*" + value) == null || path.get(attribute) != null);
			path.put(attribute, value);
			probOfSelect = gml.getProbOfSelect(graph, this.nodes, path);
			estNum = (int) ((estTotal * 1.0) * probOfSelect);
			if (estNum == 0) {
				path.remove(attribute);
			}
		} while (estNum > k / 2 || estNum == 0);
		return path;
	}
	
	/**
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
	**/
	
	public void estAllWithSelect (Hashtable<String, String> path) {
		rs = dao.getSample("*", "*");
		long a = System.currentTimeMillis();
		gml.getGraph(rs, nodes, graph, localDB);
		long b = System.currentTimeMillis();
		System.out.println(b-a);
		//graph = gml.getGraph(rs, nodes, graph, localDB);
		double probOfSelect = 0.0;
		int probPathCount = 0;
		probOfSelect = gml.getProbOfSelectWithTwoConditions(graph, nodes, path);
		rs = dao.getProbPathCountWithTowConditions(path, 0);
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
		rs = dao.getProbPathCount(path, 0);
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
