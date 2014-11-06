package config;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Random;

import probSampler.ProbSampler;
import edu.uci.ics.jung.graph.UndirectedGraph;
import edu.uci.ics.jung.graph.UndirectedSparseMultigraph;
import entity.Attribute;
import entity.Edge;
import entity.Node;

public class Util {

	public static String ConstuctSelect(HashMap<String, String> path) {
		String result = "";
		for (String key : path.keySet()) {
			result = result + key + " = '" + path.get(key) + "' AND ";
		}
		result = result.substring(0, result.length() - 5);
		return result;
	}

	public static boolean ToDoOrNotToDo(double probability) {
		Random random = new Random();
		if ((probability - Math.abs(random.nextDouble())) >= 0) {
			return true;
		} else {
			return false;
		}
	}
	
	public static boolean isUnderflow(HashMap<String, String> path, ArrayList<HashMap<String, String>> underQueries) {
		HashMap<String, String> tmpPath;
		boolean flag = false;
		for (int i = 0; i < underQueries.size(); i++) {
			tmpPath = underQueries.get(i);
			// if we cannot match tmpPath to path, then we break the loop, and check if the next is underflow
			for(String key : tmpPath.keySet()) {
				if (path.containsKey(key)) {
					if (!path.get(key).equals(tmpPath.get(key))) {
						flag = true;
						break;
					}
				} else {
					flag = true;
					break;
				}
			}
			if (flag) {
				flag = false;
			} else {
				ProbSampler.save = ProbSampler.save + 1;
				return true;
			}
		}
		return false;
	}

	public static UndirectedGraph<Node, Edge> initGraph(
			ArrayList<Attribute> attributes,
			HashMap<String, ArrayList<String>> conditions,
			HashMap<String, Node> nodes) {
		UndirectedGraph<Node, Edge> graph = new UndirectedSparseMultigraph<Node, Edge>();
		String keyFirst = "";
		String keyLast = "";
		
		for (int i = 0; i < attributes.size(); i++) {
			for (int j = 0; j < attributes.get(i).getHasNodes(); j++) {
				keyFirst = attributes.get(i).getName() + ","
						+ conditions.get(attributes.get(i).getName()).get(j);
				if (!nodes.containsKey(keyFirst)) {
					nodes.put(keyFirst, new Node(attributes.get(i).getName(),
							conditions.get(attributes.get(i).getName()).get(j), 0));
					graph.addVertex(nodes.get(keyFirst));
				}
				
				for (int m = i + 1; m < attributes.size(); m++) {
					for (int n = 0; n < attributes.get(m).getHasNodes(); n++) {
						keyLast = attributes.get(m).getName()
								+ ","
								+ conditions.get(attributes.get(m).getName())
										.get(n);
						if (!nodes.containsKey(keyLast)) {
							nodes.put(keyLast, new Node(attributes.get(m).getName(),
									conditions.get(attributes.get(m).getName()).get(n), 0));
							graph.addVertex(nodes.get(keyLast));
						}
						if (graph.findEdge(nodes.get(keyFirst), nodes.get(keyLast)) == null) {
							graph.addEdge(new Edge(), nodes.get(keyFirst), nodes.get(keyLast));
						}
					}
				}
			}
		}
		return graph;
	}
	
	

	public static void updateGraph(ArrayList<Attribute> attributes, ResultSet rs,
			HashMap<String, Node> nodes, UndirectedGraph<Node, Edge> graph, int sampleSize) {
		
		try {
			rs.beforeFirst();
			
			String keyFirst = "";
			String keyLast = "";
			Node node;
			Edge edge;
			while (rs.next()) {

				// i'm not sure it is right so far
				for (int i = 0; i < attributes.size(); i++) {
					keyFirst = attributes.get(i).getName() + ","
							+ rs.getString(attributes.get(i).getName());
					nodes.get(keyFirst).addCount();
					for (int j = i + 1; j < attributes.size(); j++) {
						keyLast = attributes.get(j).getName() + ","
								+ rs.getString(attributes.get(j).getName());
						edge = graph.findEdge(nodes.get(keyLast), nodes.get(keyFirst));
						edge.setWeight(edge.getWeight() + 1);
					}

				}
				// System.out.println(localDB.size());

			}

			// System.out.println(localDB.size());
			// calculate prob
			for (String key : nodes.keySet()) {
				node = nodes.get(key);
				node.setProb((node.getCount() * 1.0) / (sampleSize * 1.0));
			}
			

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void randomSelectAnotherAttributeValue(ArrayList<Attribute> attributes,
			HashMap<String, ArrayList<String>> conditions,
			HashMap<String, String> path,
			ArrayList<HashMap<String, String>> underQueries,
			HashMap<String, Node> nodes, int method) {
		Random random = new Random();
		String attribute = "";
		String value = "";
		int attCount = 0;
		int valueCount = 0;
		boolean flag = false;
		do {
			if (method == 1) {
				do {
					attCount = Math.abs(random.nextInt() % attributes.size());
				} while (path.get(attributes.get(attCount).getName()) != null);
				attribute = attributes.get(attCount).getName();
				valueCount = Math.abs(random.nextInt()
						% conditions.get(attribute).size());
				value = conditions.get(attribute).get(valueCount);

			} else {
				do {
					attCount = Math.abs(random.nextInt() % nodes.size());
					Iterator<String> itr = nodes.keySet().iterator();
					for (int i = 0; i <= attCount; i++) {
						attribute = itr.next();
					}
					String[] tmp = attribute.split(",");
					//System.out.println(attCount + ", " + attribute);
					attribute = tmp[0];
					value = tmp[1];
				} while (path.get(attribute) != null);
				
			}
			path.put(attribute, value);
			flag = Util.isUnderflow(path, underQueries);
			if (flag) {
				System.out.println("Check Underflow");
				path.remove(attribute);
			}
		} while (flag);
		
	}


	public static double estimateCount (HashMap<HashMap<String, String>, Integer> validQueries,
			UndirectedGraph<Node, Edge> graph,
			HashMap<String, Node> nodes) {
		double result = 0;
		double smoothRatio = 0.0;
		double probOfSelect = 0.0;
		int probPathCount = 0;
		int a = 0;
		int b = 0;
		for (String key : nodes.keySet()) {
			if(nodes.get(key).getCount() == 0) {
				b++;
			} else {
				a++;
			}
		}
		smoothRatio = (a * 1.0) / (nodes.size() * 1.0);
		for (HashMap<String, String> path : validQueries.keySet()) {
			probOfSelect = getProbOfSelect(graph, nodes, path, smoothRatio);
			probPathCount = validQueries.get(path);
			result = result + ((probPathCount * 1.0) / probOfSelect);
		}
		result = result / (validQueries.size() * 1.0);
		System.out.println("smooth = " + smoothRatio + ", estTotal = " + result);
		
		return result;
	}
	
	
	public static double getProbOfSelect(UndirectedGraph<Node, Edge> graph,
			HashMap<String, Node> nodes,
			HashMap<String, String> path, double smoothRatio) {
		double prob = 0.0;
		boolean flag = true;
		Node current = null, next = null;
		Edge edge = null;
		double edgeProb = 0.0;
		//double pX = 1.0;
		
		/**
		String sPath = "*";
		for (String pathKey : path.keySet()) {
			sPath = sPath + pathKey + " = " + path.get(pathKey) + ", ";
		}
		
		
		System.out.println(sPath.substring(0, sPath.length()-2));
		**/
		
		for (String key : path.keySet()) {
			if (flag) {
				current = nodes.get(key + "," + path.get(key));
				prob = current.getProb() * smoothRatio;
				//System.out.println(key+"*"+path.get(key)+"*"+prob);
				flag = false;
			} else {
				next = nodes.get(key + "," + path.get(key));
				//System.out.println(graph.findEdge(current, next).getProb());
				edge = graph.findEdge(current, next);
				
				// check if the edge prob need to be smoothed
				if (edge.getWeight() == 0) {
					int count = 0;
					for (String nodeKey : nodes.keySet()) {
						if (nodeKey.startsWith(key)) {
							Node tmpNode = nodes.get(nodeKey);
							Edge tmpEdge = graph.findEdge(current, tmpNode);
							if (tmpEdge.getWeight() == 0) {
								count++;
							}
						}
					}
					edgeProb = (1.0 - smoothRatio) / (count * 1.0);
				} else if (edge.getNeedSmooth()) {
					edgeProb = (edge.getWeight()*1.0)/(current.getCount()*1.0) * smoothRatio;
				} else {
					edgeProb = (edge.getWeight()*1.0)/(current.getCount()*1.0);
				}
				
				
				prob = prob * edgeProb;
			}
		}
		return prob;
	}
	
	public static double getProbOfSelectWithStartPoint(UndirectedGraph<Node, Edge> graph,
			HashMap<String, Node> nodes,
			HashMap<String, String> path, double smoothRatio, String startPoint) {
		double prob = 0.0;
		Node current = null, next = null;
		Edge edge = null;
		double edgeProb = 0.0;
		//double pX = 1.0;
		
		current = nodes.get(startPoint + "," + path.get(startPoint));
		prob = current.getProb();
		for (String key : path.keySet()) {
			if (!key.equals(startPoint)) {
				
				next = nodes.get(key + "," + path.get(key));
				//System.out.println(graph.findEdge(current, next).getProb());
				edge = graph.findEdge(current, next);
				
				// check if the edge prob need to be smoothed
				if (edge.getWeight() == 0) {
					int count = 0;
					for (String nodeKey : nodes.keySet()) {
						if (nodeKey.startsWith(key)) {
							Node tmpNode = nodes.get(nodeKey);
							Edge tmpEdge = graph.findEdge(current, tmpNode);
							if (tmpEdge.getWeight() == 0) {
								count++;
							}
						}
					}
					edgeProb = (1.0 - smoothRatio) / (count * 1.0);
				} else if (edge.getNeedSmooth()) {
					edgeProb = (edge.getWeight()*1.0)/(current.getCount()*1.0) * smoothRatio;
				} else {
					edgeProb = (edge.getWeight()*1.0)/(current.getCount()*1.0);
				}
				
				
				prob = prob * edgeProb;
			}
		}
		
		return prob;
	}
	
	public static String getNewAttributeValue(ArrayList<Attribute> attributes, HashMap<String, String> path,
			HashMap<String, ArrayList<String>> conditions,
			UndirectedGraph<Node, Edge> graph,
			HashMap<String, Node> nodes,
			double smoothRatio, double alpha,
			ArrayList<HashMap<String, String>> underQueries) {
		HashMap<String, Double> slot = new HashMap<String, Double>();
		HashMap<String, String> clonePath = (HashMap<String, String>) path.clone();
		ArrayList<String> tmpConditions = new ArrayList<String>();
		Attribute attribute = null;
		Node node = null;
		for (int i = 0; i < attributes.size(); i++) {
			attribute = attributes.get(i);
			if (path.get(attribute.getName()) == null) {
				tmpConditions = conditions.get(attribute.getName());
				for(int j = 0; j < tmpConditions.size(); j++) {
					clonePath = (HashMap<String, String>) path.clone();
					clonePath.put(attribute.getName(), tmpConditions.get(j));
					node = nodes.get(attribute.getName()+","+tmpConditions.get(j));
					if (!Util.isUnderflow(clonePath, underQueries)){
						slot.put(attribute.getName()+","+tmpConditions.get(j), 
								getProbOfSelectWithStartPoint(graph, nodes, clonePath, 
										smoothRatio, attribute.getName())*Math.pow(alpha, node.getQueryTime()));
					}
					/**
					if(startPoint.equals("")){
						slot.put(attribute.getName()+","+tmpConditions.get(j), 
								getProbOfSelectWithStartPoint(graph, nodes, clonePath, 
										smoothRatio, attribute.getName())*Math.pow(alpha, node.getQueryTime()));
					} else {
						if (!Util.isUnderflow(clonePath, underQueries)){
							slot.put(attribute.getName()+","+tmpConditions.get(j), 
									getProbOfSelectWithStartPoint(graph, nodes, clonePath, 
											smoothRatio, startPoint)*Math.pow(alpha, node.getQueryTime()));
						}
					}
					**/
					
				}
			}
			
		}
		
		/**
		if(slot == null || slot.size() == 0)  
            return "1";  
        **/
		String result = chanceSelect(slot);
		return result;
		
	}
	
	public static String chanceSelect(HashMap<String, Double> keyChanceMap) {
		if(keyChanceMap == null || keyChanceMap.size() == 0)  
            return "1";  
		double sum = 0.0;
		for(String key : keyChanceMap.keySet()) {
			sum = sum + keyChanceMap.get(key);
		}
		double rand = new Random().nextDouble() * sum;  
        
        for(String key : keyChanceMap.keySet()) {
        	rand -= keyChanceMap.get(key);
        	if (rand <= 0) {
        		return key;
        	}
        }
        return "2";
	}
	
	public static boolean haveOverlap (ArrayList<Attribute> attributes, 
			HashMap<Integer, String> overQueries, ResultSet rs, int k) {
		String value = "";
		int v = 0;
		if (overQueries.size() == 0) {
			try {
				rs.beforeFirst();
				for (int i = 0; i < k; i++){
					rs.next();
					value = "";
					for (int j = 1; j < attributes.size(); j++) {
			            value = value + rs.getString(attributes.get(j).getName()) + "*";
			        }
					v = value.hashCode();
					//System.out.println(v);
					overQueries.put(v, "1");
				}
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return false;
		} else {
			try {
				rs.beforeFirst();
				for(int i = 0; i < k; i++) {
					rs.next();
					value = "";
					for (int j = 1; j < attributes.size(); j++) {
			            value = value + rs.getString(attributes.get(j).getName()) + "*";
			        }
					v = value.hashCode();
					//System.out.println(v);
					if(!overQueries.containsKey(v)){
						//System.out.println("delete something lol");
						return true;
					}
				}
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return false;
	}
	
	public static double calculateKL(ArrayList<Attribute> attributes,
			Hashtable<String, ArrayList<String>> conditions,
			Hashtable<String, Double> oriTable,
			Hashtable<String, Double> samTable) {
		double result = 0.0;
		String tmpAttribute = "";
		String tmpValue = "";
		ArrayList<String> tmpList = new ArrayList<String>();
		for (int i = 0; i < attributes.size(); i++) {
			tmpAttribute = attributes.get(i).getName();

			tmpList = conditions.get(tmpAttribute);
			for (int j = 0; j < tmpList.size(); j++) {
				tmpValue = tmpList.get(j);
				if (samTable.get(tmpAttribute + "*" + tmpValue) != 0.0) {
					result = result
							+ samTable.get(tmpAttribute + "*" + tmpValue)
							* Math.log(samTable.get(tmpAttribute + "*" + tmpValue)
									/ oriTable.get(tmpAttribute + "*" + tmpValue));
				}
				
				//System.out.println(samTable.get(tmpAttribute + "*" + tmpValue));
				//System.out.println(result);
			}
		}

		return result;
	}
	
	
	public static ArrayList<String> getGaussModel(HashMap<HashMap<String, String>, Integer> validQueries,
			UndirectedGraph<Node, Edge> graph,
			HashMap<String, Node> nodes, double smoothRatio) {
		HashMap<HashMap<String, String>, Double> tmpValidQueries = new HashMap<HashMap<String, String>, Double>();
		double countAvg = 0.0;
		double probAvg = 0.0;
		for (HashMap<String, String> path : validQueries.keySet()) {
			//System.out.println(path.size());
			countAvg = countAvg + Math.log10((path.size() * 1.0));
			tmpValidQueries.put(path, getProbOfSelect(graph, nodes, path, smoothRatio));
			probAvg = probAvg + Math.log10(tmpValidQueries.get(path));
			//probOfSelect = getProbOfSelect(graph, nodes, path, smoothRatio);
			//probPathCount = validQueries.get(path);
			//result = result + ((probPathCount * 1.0) / probOfSelect);
		}
		countAvg = countAvg / (validQueries.size() * 1.0);
		probAvg = probAvg / (validQueries.size() * 1.0);
		
		double countVar = 0.0;
		double probVar = 0.0;
		
		for (HashMap<String, String> path : validQueries.keySet()) {
			//System.out.println(tmpValidQueries.get(path));
			countVar = countVar + Math.pow(Math.log10((path.size() * 1.0)) - countAvg, 2);
			probVar = probVar + Math.pow(Math.log10(tmpValidQueries.get(path)) - probAvg, 2);
		}
		countVar = countVar / (validQueries.size() * 1.0);
		probVar = probVar / (validQueries.size() * 1.0);
		
		ArrayList<String> result = new ArrayList<String>();
		result.add(countAvg + "," + countVar);
		result.add(probAvg + "," + probVar);
		
		return result;
	}
	
	public static double applyGaussModel(HashMap<String, String> path,
			UndirectedGraph<Node, Edge> graph,
			HashMap<String, Node> nodes, double smoothRatio,
			ArrayList<String> gaussModel) {
		double result = 1.0;
		double countAvg = Double.parseDouble(gaussModel.get(0).split(",")[0]);
		double probAvg = Double.parseDouble(gaussModel.get(1).split(",")[0]);
		double countVar = Double.parseDouble(gaussModel.get(0).split(",")[1]);
		double probVar = Double.parseDouble(gaussModel.get(1).split(",")[1]);
		
		double a2 = Math.sqrt(2.0 * Math.PI * countVar);
		double b2 = Math.pow(Math.E, -1.0 * (Math.pow(path.size() * 1.0 - countAvg, 2) / (2.0 * countVar)));
		double c2 = -1.0 * (Math.pow(path.size() * 1.0 - countAvg, 2) / (2.0 * countVar));

		
		double a = Math.sqrt(2.0 * Math.PI * probVar);
		double b = Math.pow(Math.E, -1.0 * (Math.pow(getProbOfSelect(graph, nodes, path, smoothRatio) - probAvg, 2) / (2.0 * probVar)));
		double c = -1.0 * (Math.pow(getProbOfSelect(graph, nodes, path, smoothRatio) - probAvg, 2) / (2.0 * probVar));
		double d = (1.0 / Math.sqrt(2.0 * Math.PI * probVar)) * Math.pow(Math.E, -1.0 * (Math.pow(getProbOfSelect(graph, nodes, path, smoothRatio) - probAvg, 2) / (2.0 * probVar)));
		double e = getProbOfSelect(graph, nodes, path, smoothRatio);
		result = result * (1.0 / Math.sqrt(2.0 * Math.PI * countVar)) * Math.pow(Math.E, -1.0 * (Math.pow(Math.log10(path.size() * 1.0) - countAvg, 2) / (2.0 * countVar)));
		result = result * (1.0 / Math.sqrt(2.0 * Math.PI * probVar)) * Math.pow(Math.E, -1.0 * (Math.pow(Math.log10(getProbOfSelect(graph, nodes, path, smoothRatio)) - probAvg, 2) / (2.0 * probVar)));
		
		return result;
	}
}
