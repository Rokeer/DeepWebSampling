package tree;

public class NodeData {
	private String attribute;
	//private String value;
	private int count;
	private boolean isDone = false;

	public NodeData() {

	}

	public NodeData(String attribute, int count) {
		super();
		this.attribute = attribute;
		//this.value = value;
		this.count = count;
	}
	
	public String getAttribute() {
		return attribute;
	}

	public void setAttribute(String attribute) {
		this.attribute = attribute;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
	
	public boolean getIsDone() {
		return isDone;
	}

	public void setIsDone(boolean isDone) {
		this.isDone = isDone;
	}

	public void output() {
		System.out.println(attribute + " " + count);
	}
	
}
