package config;

import java.util.HashMap;
import java.util.Random;

public class Util {
	
	public static String ConstuctSelect(HashMap<String, String> path) {
		String result = "";
		for (String key : path.keySet()) {
			result = result + key + " = '" + path.get(key) + "' AND ";
		}
		result = result.substring(0, result.length() - 5);
		return result;
	}
	public static boolean ToDoOrNotToDo (double probability) {
		Random random = new Random();
		if ((probability - Math.abs(random.nextDouble())) >= 0) {
			return true;
		} else {
			return false;
		}
	}
}
