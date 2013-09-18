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
	
	/** 
     * 概率选择 
     * @param keyChanceMap key为唯一标识，value为该标识的概率，是去掉%的数字 
     * @return 被选中的key。未选中返回null 
     */  
	
	public EdgeData chanceSelect(Collection<EdgeData> keyChanceMap, int sum) {
		if(keyChanceMap == null || keyChanceMap.size() == 0)  
            return null;  
		// 从1开始  
        int rand = new Random().nextInt(sum) + 1;  
        
        for(EdgeData edge : keyChanceMap) {
        	rand -= edge.getCount();
        	// 选中  
            if(rand <= 0) {  
                 return edge;  
            }
		}
		return null;
	}
}
