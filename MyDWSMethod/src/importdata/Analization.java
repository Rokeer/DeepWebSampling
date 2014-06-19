package importdata;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import dao.DAO;
import entity.Attribute;

public class Analization {

	// the last step, save attributes' information into info database, so that we can access these 
	// information easily
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		DAO dao = new DAO("uscensus", "usdatanoid", "", "attrinfo");
		ResultSet rs = dao.getAttributes();
		ArrayList<Attribute> attributes = new ArrayList<Attribute>();
		try {
			while (rs.next()) {
				attributes.add(new Attribute(rs.getString(1)));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		for (int i = 0; i < attributes.size(); i++) {
			System.out.println(i);
			rs = dao.getValues(attributes.get(i).getName());
			String values = "";
			//ArrayList<String> values = new ArrayList<String>();
			try {
				while (rs.next()) {
					values = values + rs.getString(1) + "\\;";
					//values.add(rs.getString(1));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			dao.save2info(attributes.get(i).getName(), values.substring(0, (values.length() - 2)));
		}
		System.out.println("Well Done!");
	}

}
