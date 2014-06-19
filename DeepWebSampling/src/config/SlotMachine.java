package config;

import java.util.Collection;
import java.util.Random;

import tree.EdgeData;

public class SlotMachine {

	private Random random = new Random();
	public SlotMachine () {
		
	}
	
	public void test () {
		System.out.println(random.nextInt()%5);
	}
	
	public boolean toDoOrNotToDo (double probability) {
		if ((probability - Math.abs(random.nextDouble())) >= 0) {
			return true;
		} else {
			return false;
		}
	}
	
	public EdgeData chanceSelect(Collection<EdgeData> keyChanceMap, int sum) {
		if(keyChanceMap == null || keyChanceMap.size() == 0)  
            return null;  
		// ��1��ʼ  
        int rand = new Random().nextInt(sum) + 1;  
        
        for(EdgeData edge : keyChanceMap) {
        	rand -= edge.getCount();
        	// ѡ��  
            if(rand <= 0) {  
                 return edge;  
            }
		}
		return null;
	}
	
}
