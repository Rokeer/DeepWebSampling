package config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

import edu.uci.ics.jung.graph.UndirectedGraph;
import entity.Attribute;
import entity.Edge;
import entity.Node;

public class Util {

	public Util() {

	}

	public String calculateRLC(
			Hashtable<String, Hashtable<String, Integer>> preProInfo,
			double allCount, double s, double k, double currentCount) {

		Set<String> set = preProInfo.keySet();
		Iterator<String> itr = set.iterator();
		String maxAttribute = "";
		String tmpAttribute = "";
		double maxRLC = -10000.0;
		double tmpRLC = -10000.0;
		boolean flag = true;
		//System.out.println("allcount: " + allCount);
		while (itr.hasNext()) {
			tmpAttribute = itr.next();

			// double a = calculateR(preProInfo.get(tmpAttribute), allCount, s,
			// k, currentCount);
			// double b = calculateC(preProInfo.get(tmpAttribute), allCount, s,
			// k, currentCount);
			// System.out.println("a = " + a + ", b = " + b);
			// tmpRLC = a/b;
			tmpRLC = calculateR(preProInfo.get(tmpAttribute), allCount, s, k,
					currentCount)
					/ calculateC(preProInfo.get(tmpAttribute), allCount, s, k,
							currentCount);
			if (flag) {
				maxRLC = tmpRLC;
				maxAttribute = tmpAttribute;
				flag = false;
			}

			if (tmpRLC > maxRLC) {
				maxRLC = tmpRLC;
				maxAttribute = tmpAttribute;
			}
		}
		if (maxAttribute.equals("")) {
			System.out.println("wtf");
		}
		// preProInfo.remove(maxAttribute);
		return maxAttribute;
	}

	public double calculateR(Hashtable<String, Integer> values,
			double allCount, double s, double k, double currentCount) {
		double result = 0.0;

		String value = "";
		Set<String> set = values.keySet();
		Iterator<String> itr = set.iterator();
		while (itr.hasNext()) {
			value = itr.next();
			result = result
					+ ((Math.pow((1 - values.get(value) / allCount), s) - Math
							.pow((1 - currentCount / allCount), s)) * (values
							.get(value) / k - 1));
		}

		return result;
	}

	public double calculateC(Hashtable<String, Integer> values,
			double allCount, double s, double k, double currentCount) {
		double result = 0.0;
		result = (1 - Math.pow((1 - currentCount / allCount), s))
				* (values.size() - 1);
		return result;
	}

	public double calculateS(UndirectedGraph<Node, Edge> graph, Node n,
			double a, double b) {
		double d = 0.0;
		//Collection<Edge> edgeSet = graph.getOutEdges(n);
		for (Edge e : graph.getOutEdges(n)) {
			d = d + (double) e.getWeight();
		}
		//double d = (double) graph.degree(n);
		double t = (double) n.getUsedTime();
		double h = calculateH(graph, n, b, d);
		double s = d * Math.pow(a, t) * h;
		return s;
	}

	public double calculateH(UndirectedGraph<Node, Edge> graph, Node n,
			double b, double d) {
		double h = 0.0;
		double p = 0.0;

		for (Node node : graph.getNeighbors(n)) {
			p = ((double) graph.findEdge(n, node).getWeight()) / d;
			h = h + ((p * Math.log(p) / Math.log(b)));
		}

		h = h * -(1.0);
		return h;
	}

	public double calculateKL(ArrayList<Attribute> attributes,
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
}
