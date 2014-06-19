package config;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Hashtable;

import edu.uci.ics.jung.graph.DelegateTree;
import edu.uci.ics.jung.graph.DirectedGraph;
import edu.uci.ics.jung.graph.DirectedSparseMultigraph;
import edu.uci.ics.jung.graph.UndirectedGraph;
import entity.Attribute;
import entity.Edge;
import entity.Node;

public class GraphML {
	public GraphML() {
		
	}
	
	public UndirectedGraph<Node, Edge> getGraph(ResultSet rs,
			Hashtable<String, Node> nodes, UndirectedGraph<Node, Edge> graph,
			Hashtable<Integer, String> localDB) {
		//DirectedGraph<Node, Edge> graph = new DirectedSparseMultigraph<Node, Edge>();
		//nodes.clear();
		try {
			
			//Hashtable<Integer, String> localDB = new Hashtable<Integer, String>();
			rs.beforeFirst();
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			String keyFirst = "";
			String keyLast = "";
			String stat = "";
			Edge edge;
			while (rs.next()) {
				stat = "";
				for (int i = 0; i < columnCount; i++) {
					stat = stat + "'" + rs.getString(i + 1) + "',";
				}
				if(!localDB.containsKey(stat.hashCode())){
					localDB.put(stat.hashCode(), "1");
					// i'm not sure it is right so far
					for (int i = 0; i < columnCount; i++) {
						keyFirst = rsmd.getColumnName(i + 1) + "*" + rs.getString(i + 1);
						if (!nodes.containsKey(keyFirst)) {
							nodes.put(keyFirst, new Node(rsmd.getColumnName(i + 1),
									rs.getString(i + 1), 1));

						}
						if (!graph.containsVertex(nodes.get(keyFirst))) {
							graph.addVertex(nodes.get(keyFirst));
						} else {
							nodes.get(keyFirst).addUsedTime();
						}
						for (int j = i + 1; j < columnCount; j++) {
							keyLast = rsmd.getColumnName(j + 1) + "*" 
									+ rs.getString(j + 1);
							if (!nodes.containsKey(keyLast)) {
								nodes.put(
										keyLast,
										new Node(rsmd.getColumnName(j + 1), rs
												.getString(j + 1), 0));
							}
							if (!graph.containsVertex(nodes.get(keyLast))) {
								graph.addVertex(nodes.get(keyLast));
							} 
							/**
							else {
								nodes.get(keyLast).addUsedTime();
							}
							System.out.println(nodes.get(keyLast).getAttribute()+" "+nodes.get(keyLast).getUsedTime());
							if(nodes.get(keyLast).getUsedTime()>1504){
								System.out.println("oops");
							}
							**/
							edge = graph.findEdge(nodes.get(keyFirst), nodes.get(keyLast));
							if(edge == null) {
								graph.addEdge(new Edge(), nodes.get(keyFirst),
										nodes.get(keyLast));
								//graph.addEdge(new Edge(), nodes.get(keyLast),
								//		nodes.get(keyFirst));
							} else {
								edge.setWeight(edge.getWeight() + 1);
								//edge = graph.findEdge(nodes.get(keyLast), nodes.get(keyFirst));
								//edge.setWeight(edge.getWeight() + 1);
							}
							
							/**
							edge = graph.findEdge(nodes.get(keyLast), nodes.get(keyFirst));
							if(edge == null) {
								graph.addEdge(new Edge(), nodes.get(keyLast),
										nodes.get(keyFirst));
							} else {
								edge.setWeight(edge.getWeight() + 1);
							}
							**/
							
						}
						
					}
					//System.out.println(localDB.size());
				}
				
			}
			rs.last();
			//System.out.println(localDB.size());
			//calculate prob
			Enumeration<String> e = nodes.keys();
			while (e.hasMoreElements()) {
				String it = (String) e.nextElement();
				Node n = nodes.get(it);
				n.setProb((n.getUsedTime()*1.0)/(localDB.size()*1.0));
				//System.out.println((n.getUsedTime()*1.0)/(rs.getRow()*1.0));
				/**
				Collection<Edge> edges = graph.getOutEdges(n);
				for(Edge edge4i : edges) {
					edge4i.setProb((edge4i.getWeight()*1.0)/(n.getUsedTime()*1.0));
				}
				**/
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return graph;
	}
	
	public DelegateTree<Node, Edge> createSearchTree(ArrayList<Attribute> attributes, Hashtable<String, ArrayList<String>> conditions) {
		DelegateTree<Node, Edge> tree = new DelegateTree<Node, Edge>();
		tree.setRoot(new Node("Root","this is root"));
		addChildForTree(tree, tree.getRoot(), attributes, conditions);
		return tree;
	}
	
	@SuppressWarnings("unchecked")
	public void addChildForTree(DelegateTree<Node, Edge> tree, 
			Node parent, ArrayList<Attribute> attributes, 
			Hashtable<String, ArrayList<String>> conditions) {
		
		ArrayList<String> values = new ArrayList<String>();
		ArrayList<Attribute> newAttr;
		String attrName = "";
		for (int i = 0; i < attributes.size(); i++) {
			attrName = attributes.get(i).getName();
			values = conditions.get(attrName);
			//System.out.println("i="+i+", size="+attributes.size()+", newSize="+newAttr.size());
			newAttr = (ArrayList<Attribute>) attributes.clone();
			newAttr.remove(i);
			//System.out.println("i="+i+", size="+attributes.size()+", newSize="+newAttr.size());
			for (int j = 0; j < values.size(); j++) {
				Node child = new Node(attrName, values.get(j));
				tree.addChild(new Edge(), parent, child);
				addChildForTree(tree, child, newAttr, conditions);
			}
			
		}
		
	}
	
	public double getProbOfSelect(UndirectedGraph<Node, Edge> graph,
			Hashtable<String, Node> nodes, Hashtable<String, String> path) {
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
				current = nodes.get(key + "*" + path.get(key));
				prob = current.getProb();
				//System.out.println(key+"*"+path.get(key)+"*"+prob);
				flag = false;
			} else {
				next = nodes.get(key + "*" + path.get(key));
				//System.out.println(graph.findEdge(current, next).getProb());
				edge = graph.findEdge(current, next);
				if (edge != null){
					edgeProb = (edge.getWeight()*1.0)/(current.getUsedTime()*1.0);
				} else {
					edgeProb = 0.0;
				}
				
				prob = prob * edgeProb;
				//pX = pX * next.getProb();
				//prob = prob * graph.findEdge(current, next).getProb();
				//System.out.println(next.getAttribute()+"*"+next.getValue()+"*"+current.getAttribute()+"*"+current.getValue()+"*"+graph.findEdge(current, next).getProb());
				//current = next;
				//prob = prob * next.getProb();
			}
		}
		//prob = prob / pX;
		return prob;
	}
	
	
	
	
}
