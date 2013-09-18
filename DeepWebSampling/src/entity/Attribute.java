package entity;

public class Attribute {
	private String name = "";
	private int hasNodes = 0;
	public Attribute (String name) {
		this.name = name;
		hasNodes = 0;
	}
	
	public Attribute (String name, int hasNodes) {
		this.name = name;
		this.hasNodes = hasNodes;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getHasNodes() {
		return hasNodes;
	}
	public void setHasNodes(int hasNodes) {
		this.hasNodes = hasNodes;
	}
	
	
	
}
