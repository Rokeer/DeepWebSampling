package probSampler;

import importdata.Measure;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;

import config.Util;
import dao.DAO;
import edu.uci.ics.jung.graph.UndirectedGraph;
import entity.Attribute;
import entity.Edge;
import entity.Node;

public class ProbSampler {

	/**
	 * parameters need to be adjusted
	 */
	private int requiredSize = 10000;
	private int s1 = 2000;
	private int k = 100;
	private double C = 1.0 / 256.0;
	private double smoothRatio = 0.2;
	private double alpha = 0.2;
	private int steps = 1;
	private double gaussThreshold = 0.000000005;

	/**
	 * other var will be used
	 */
	public static int save = 0;
	public int over = 0;
	public int under = 0;
	private int sampleSize = 0;
	private DAO dao = new DAO("uscensus", "usdatanoid", "ahsdb", "attrinfo");
	private ResultSet rs = dao.getInfo();
	private ArrayList<Attribute> attributes = new ArrayList<Attribute>();
	private HashMap<String, ArrayList<String>> conditions = new HashMap<String, ArrayList<String>>();
	private int queryCount = 0;
	private HashMap<String, String> path = new HashMap<String, String>();
	private RandomApproach ra = new RandomApproach();
	private HashMap<HashMap<String, String>, Integer> validQueries = new HashMap<HashMap<String, String>, Integer>();
	private ArrayList<HashMap<String, String>> underQueries = new ArrayList<HashMap<String, String>>();
	private UndirectedGraph<Node, Edge> graph;
	private HashMap<String, Node> nodes = new HashMap<String, Node>();
	private HashMap<Integer, String> overQueries = new HashMap<Integer, String>();
	private ArrayList<String> gaussModel = new ArrayList<String>();
	private HashMap<HashMap<String, String>, Integer> canBeDeleteValidQueries = new HashMap<HashMap<String, String>, Integer>();

	public ProbSampler(String sampleDB) {
		this.dao = new DAO("uscensus", "usdatanoid", sampleDB, "attrinfo");
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

		// first we dont need to do the random select when programming, so i
		// present a sample database already
		dao.createSampleDB(attributes);
		graph = Util.initGraph(attributes, conditions, nodes);

		// Use Random Approach to select.
		ArrayList<Integer> tmp = new ArrayList<Integer>();

		while (sampleSize < s1) {
			tmp = ra.select(k, C, dao, rs, attributes, conditions, path,
					validQueries, underQueries, sampleSize, graph, nodes);
			queryCount = queryCount + tmp.get(0);
			sampleSize = sampleSize + tmp.get(1);
		}

		// no estimate count so far
		// Util.estimateCount(validQueries, graph, nodes);

		// real deal damn it.
		// build Gausss model
		gaussModel = Util
				.getGaussModel(validQueries, graph, nodes, smoothRatio);

		
		double randomLength = 0;
		for (HashMap<String, String> path : validQueries.keySet()) {
			randomLength = randomLength + path.size();
		}
		randomLength = randomLength / (validQueries.size() * 1.0);
		
		while (sampleSize < requiredSize) {
			ArrayList<Attribute> cloneAttributes = (ArrayList<Attribute>) attributes
					.clone();
			coolSelect(cloneAttributes, steps, gaussModel);
			System.out.println(queryCount);
		}
		System.out.println("QueryCount = " + queryCount + ", Save = " + save);
		Measure.getMeasure(sampleDB);
		//dao.save2result(queryCount, save, Measure.getMeasure(sampleDB));
		System.out.println(over + ", " + under);
		
		double length = 0;
		for (HashMap<String, String> path : canBeDeleteValidQueries.keySet()) {
			length = length + path.size();
		}
		length = length / (canBeDeleteValidQueries.size() * 1.0);
		System.out.println("Random Length = " + randomLength + ", Length = " + length);
	}

	private void coolSelect(ArrayList<Attribute> cloneAttributes, int steps,
			ArrayList<String> gaussModel) {
		String av = "";
		int resultCount = 0;
		ArrayList<String> avs = new ArrayList<String>();
		double probOfValid = 0.0;
		double oldProb = 0.0;
		
		
		do {
			oldProb = probOfValid;
			if (path.size() == 0) {
				// if (startPoint.equals("")) {
				// select a start point
				av = Util.getNewAttributeValue(cloneAttributes, path,
						conditions, graph, nodes, smoothRatio, alpha,
						underQueries);
				// startPoint = av.split(",")[0];
				overQueries.clear();
			} else {
				av = Util.getNewAttributeValue(cloneAttributes, path,
						conditions, graph, nodes, smoothRatio, alpha,
						underQueries);
			}
			if (av.contains(",")) {
				path.put(av.split(",")[0], av.split(",")[1]);
				avs.add(av.split(",")[0]);
			}

			probOfValid = Util.applyGaussModel(path, graph, nodes, smoothRatio,
					gaussModel);
			if (probOfValid < oldProb) {
				path.remove(av.split(",")[0]);
				avs.remove(av.split(",")[0]);
			}

			// System.out.println("oldProb = " + oldProb);
			System.out.println("probOfValid = " + probOfValid);
			// } while (probOfValid < gaussThreshold);
		} while (probOfValid < gaussThreshold && probOfValid > oldProb);
		
		
		
		/**
		  for(int i = 0; i < steps; i++) { 
			  
			  if (path.size() == 0){ 
				  //if (startPoint.equals("")) { 
				  // select a start point 
				  av = Util.getNewAttributeValue(cloneAttributes, path, conditions, graph,
		  nodes, smoothRatio, alpha, underQueries); 
				  //startPoint = av.split(",")[0]; 
				  //overQueries.clear(); 
			  } else { 
				  av = Util.getNewAttributeValue(cloneAttributes, path, conditions, graph,
		  nodes, smoothRatio, alpha, underQueries); 
			  }
			  
			  path.put(av.split(",")[0], av.split(",")[1]);
			  avs.add(av.split(",")[0]); 
		  }
**/		 

		/**
		 * int i = 0; for (i = 0; i < cloneAttributes.size(); i++) { if
		 * (cloneAttributes.get(i).getName().equals(av.split(",")[0])) {
		 * cloneAttributes.remove(i); break; } }
		 **/

		// do select
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
			canBeDeleteValidQueries.put((HashMap<String, String>) path.clone(), rowCount);
			gaussModel = Util.getGaussModel(validQueries, graph, nodes,
					smoothRatio);
			 //double probability = C * rowCount * Math.pow(2.0,
			 //path.size()-1.0);
			 //if (probability > 1.0) {
			 //probability = 1.0;
			 //}
			double probability = 1.0;

			Util.updateGraph(attributes, rs, nodes, graph, sampleSize
					+ rowCount);
			resultCount = dao.save2SampleDBRandom(rs, probability);
			sampleSize = sampleSize + resultCount;
			// path = (Hashtable<String, String>) oriPath.clone();
			path.clear();
		} else if (rowCount > k) {
			// overflow
			System.out.println("Overflow");
			over++;
			// if (Util.haveOverlap(cloneAttributes, overQueries, rs, k)) {
			// if (steps > 1){
			// coolSelect(cloneAttributes, steps - 1);
			// } else {
			// coolSelect(cloneAttributes, steps);
			// }

			// } else {
			coolSelect(cloneAttributes, steps, gaussModel);
			// }

		} else if (rowCount == 0) {
			// underflow
			under++;
			underQueries.add((HashMap<String, String>) path.clone());
			System.out.println("Underflow");
			// path = (Hashtable<String, String>) oriPath.clone();
			System.out.println(path.size());
			for (int i = 0; i < avs.size(); i++) {
				path.remove(avs.get(i));
			}
			System.out.println(path.size());
			// path.remove(av.split(",")[0]);
			coolSelect(cloneAttributes, steps, gaussModel);
		}

	}
}
