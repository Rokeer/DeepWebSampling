package config;

import java.util.Comparator;

import entity.Attribute;

public class MyAttributeCompare implements Comparator<Attribute> {
	public int compare(Attribute arg0, Attribute arg1) {
		Attribute attribute0 = arg0;
		Attribute attribute1 = arg1;

		//smallest to largest
		int flag = attribute0.getHasNodes() - attribute1.getHasNodes();
		
		//largest to smallest
		//int flag = attribute1.getHasNodes() - attribute0.getHasNodes();
		
		if (flag == 0) {
			return attribute0.getName().compareTo(attribute1.getName());
		} else {
			return flag;
		}
	}
}
