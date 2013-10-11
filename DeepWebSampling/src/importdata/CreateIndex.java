package importdata;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import dao.DAO;
import entity.Attribute;

public class CreateIndex {

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
		dao.createIndex(attributes);
		System.out.println("Well Done!");
	}

}
