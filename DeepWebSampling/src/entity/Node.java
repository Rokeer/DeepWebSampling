package entity;

public class Node {
	private final String attribute;
	private final String value;
	private int usedTime;
	
	public Node(String attribute, String value)
	{
		this.attribute = attribute;
		this.value = value;
		this.usedTime = 0;
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
	
	
	
}
