package entity;

public class Edge {
	private int weight;
	private double prob;
	public Edge()
	{
		weight = 1;
	}
	public int getWeight() {
		return weight;
	}
	public void setWeight(int weight) {
		this.weight = weight;
	}
	
	public double getProb() {
		return prob;
	}

	public void setProb(double prob) {
		this.prob = prob;
	}
}
