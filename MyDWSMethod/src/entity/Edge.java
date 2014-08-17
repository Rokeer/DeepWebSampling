package entity;

public class Edge {
	private int weight;
	private boolean needSmooth;
	public Edge()
	{
		weight = 0;
		needSmooth = true;
	}
	public int getWeight() {
		return weight;
	}
	public void setWeight(int weight) {
		this.weight = weight;
	}
	public boolean getNeedSmooth() {
		return needSmooth;
	}
	public void setNeedSmooth(boolean needSmooth) {
		this.needSmooth = needSmooth;
	}
	
}
