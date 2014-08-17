package entity;

public class Node {
	private final String attribute;
	private final String value;
	private int count;
	private double prob;
	private int queryTime;
	
	public Node(String attribute, String value)
	{
		this.attribute = attribute;
		this.value = value;
		this.count = 0;
		this.prob = 0.0;
		this.queryTime = 0;
	}
	
	public Node(String attribute, String value, int count)
	{
		this.attribute = attribute;
		this.value = value;
		this.count = count;
		this.prob = 0.0;
		this.queryTime = 0;
	}
	
	public void addCount() {
		this.count = this.count + 1;
	}
	
	public void addQueryTime() {
		this.queryTime = this.queryTime + 1;
	}
	
	
	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public String getAttribute() {
		return attribute;
	}

	public String getValue() {
		return value;
	}


	public double getProb() {
		return prob;
	}


	public void setProb(double prob) {
		this.prob = prob;
	}

	public int getQueryTime() {
		return queryTime;
	}

	public void setQueryTime(int queryTime) {
		this.queryTime = queryTime;
	}
	
	
	
}
