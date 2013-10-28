package importdata;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;

import dao.DAO;
import entity.Attribute;

public class TestDatabase {

	// reduce duplicate items in database
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		DAO dao = new DAO("uscensus", "usdata", "", "attrinfo");
		//ResultSet rs = dao.getCount("*", "*");
		Hashtable<Integer, String> ht = new Hashtable<Integer, String>();
		
		ResultSet rs = dao.getAttributes();
		ArrayList<Attribute> attributes = new ArrayList<Attribute>();
		try {
			while (rs.next()) {
				attributes.add(new Attribute(rs.getString(1)));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		//dao.testDatabase(attributes);
		rs = dao.getCount("*", "*");
		int allCount = 0;
		String value = "";
		int count = 0;
		int v = 0;
		try {
			rs.next();
			allCount = rs.getInt(1) + 10000;
		System.out.println(allCount);
		for(int i = 0; i < allCount; i = i + 10000) {
			count = 0;
			rs = dao.testDatabase2(i);
			System.out.println("Select Done!");
			while(rs.next()) {
				
				value = "";
				for (int j = 1; j < attributes.size(); j++) {
		            value = value + rs.getString(attributes.get(j).getName()) + "*";
		        }
				v = value.hashCode();
				//System.out.println(v);
				if(ht.containsKey(v)){
					//System.out.println("delete something lol");
					rs.deleteRow();
					count = count + 1;
				} else {
					ht.put(v, "1");
				}
			}
			System.out.println("delete " + count + " itmes");
			allCount = allCount - count;
			i = i - count;
			dao.close();
		}
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
