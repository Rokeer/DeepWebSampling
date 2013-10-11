package config;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

import edu.uci.ics.jung.graph.UndirectedGraph;
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
		System.out.println("allcount: " + allCount);
		while (itr.hasNext()) {
			tmpAttribute = itr.next();
			
			// double a = calculateR(preProInfo.get(tmpAttribute), allCount, s, k, currentCount);
			// double b = calculateC(preProInfo.get(tmpAttribute), allCount, s, k, currentCount);
			// System.out.println("a = " + a + ", b = " + b);
			// tmpRLC = a/b;
			tmpRLC = calculateR(preProInfo.get(tmpAttribute), allCount, s, k, currentCount) / calculateC(preProInfo.get(tmpAttribute), allCount, s, k, currentCount);
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
		if (maxAttribute.equals("")){
			System.out.println("wtf");
		}
		//preProInfo.remove(maxAttribute);
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
			result = result + ((Math.pow((1 - values.get(value)/allCount), s) - Math.pow((1 - currentCount/allCount), s)) * (values.get(value)/k - 1));
		}
		
		
		return result;
	}

	public double calculateC(Hashtable<String, Integer> values,
			double allCount, double s, double k, double currentCount) {
		double result = 0.0;
		result = (1 - Math.pow((1 - currentCount/allCount), s)) * (values.size() - 1);
		return result;
	}

	public double calculateS(UndirectedGraph<Node, Edge> graph, Node n,
			double a, double b) {

		double d = (double) graph.degree(n);
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
			p = ((double) graph.findEdgeSet(n, node).size()) / d;
			h = h + ((p * Math.log(p) / Math.log(b)));
		}

		h = h * -(1.0);
		return h;
	}

}
