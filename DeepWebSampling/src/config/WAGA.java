package config;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Hashtable;

import edu.uci.ics.jung.graph.UndirectedGraph;
import edu.uci.ics.jung.graph.UndirectedSparseMultigraph;
import entity.Edge;
import entity.Node;

public class WAGA {

	UndirectedGraph<Node, Edge> graph;
	public WAGA() {

	}

	public UndirectedGraph<Node, Edge> createGraph(ResultSet rs,
			Hashtable<String, Node> nodes, int k) {
		graph = new UndirectedSparseMultigraph<Node, Edge>();
		try {
			rs.beforeFirst();
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			String keyFirst = "";
			String keyLast = "";

			while (rs.next() && rs.getRow() <= k) {
				// i'm not sure it is right so far
				for (int i = 0; i < columnCount; i++) {
					keyFirst = rsmd.getColumnName(i + 1) + rs.getString(i + 1);
					if (!nodes.containsKey(keyFirst)) {
						nodes.put(keyFirst, new Node(rsmd.getColumnName(i + 1),
								rs.getString(i + 1)));

					}
					if (!graph.containsVertex(nodes.get(keyFirst))) {
						graph.addVertex(nodes.get(keyFirst));
					}
					for (int j = i + 1; j < columnCount; j++) {
						keyLast = rsmd.getColumnName(j + 1)
								+ rs.getString(j + 1);
						if (!nodes.containsKey(keyLast)) {
							nodes.put(
									keyLast,
									new Node(rsmd.getColumnName(j + 1), rs
											.getString(j + 1)));
						}
						if (!graph.containsVertex(nodes.get(keyLast))) {
							graph.addVertex(nodes.get(keyLast));
						}

						graph.addEdge(new Edge(), nodes.get(keyFirst),
								nodes.get(keyLast));
						
						/**
						 * if(graph.containsEdge(graph.findEdge(nodes.get(
						 * keyFirst), nodes.get(keyLast)))) { Edge tmpE =
						 * graph.findEdge(nodes.get(keyFirst),
						 * nodes.get(keyLast));
						 * tmpE.setWeight(tmpE.getWeight()+1);
						 * //System.out.println
						 * (graph.findEdge(nodes.get(keyFirst),
						 * nodes.get(keyLast)).getWeight()); } else {
						 * graph.addEdge(new Edge(), nodes.get(keyFirst),
						 * nodes.get(keyLast)); }
						 **/

					}
				}
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return graph;
	}

}
