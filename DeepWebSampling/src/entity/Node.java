package entity;

public class Node {
	private final String attribute;
	private final String value;
	private int usedTime;
	private double prob;
	private int queryTime;
	
	public Node(String attribute, String value)
	{
		this.attribute = attribute;
		this.value = value;
		this.usedTime = 0;
		this.prob = 0.0;
		this.queryTime = 0;
	}
	
	public Node(String attribute, String value, int usedTime)
	{
		this.attribute = attribute;
		this.value = value;
		this.usedTime = usedTime;
		this.prob = 0.0;
		this.queryTime = 0;
	}
	
	public void addUsedTime() {
		this.usedTime = this.usedTime + 1;
	}
	
	public void addQueryTime() {
		this.queryTime = this.queryTime + 1;
	}
	
	
	public int getUsedTime() {
		return usedTime;
	}

	public void setUsedTime(int usedTime) {
		this.usedTime = usedTime;
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
