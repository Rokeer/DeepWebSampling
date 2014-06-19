package dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import config.Conn;
import entity.Attribute;

public class DAO {
	Connection connection;
	String table = "usdata";
	String database = "uscensus";
	String sampleTable = "sampleDB";
	String infoTable = "attrinfo";
	Statement t;
	ResultSet rs;

	public DAO() {
		try {
			connection = new Conn().getConnection();
			this.t = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
				     ResultSet.CONCUR_READ_ONLY);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public DAO(String database, String table, String sampleTable, String infoTable) {
		try {
			this.database = database;
			this.table = table;
			this.sampleTable = sampleTable;
			this.infoTable = infoTable;
			connection = new Conn().getConnection();
			this.t = connection.createStatement();
			//t.execute("select * from " + database + "." + table);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public Boolean insertItem(ArrayList<String> sql) {
		// Statement t;
		StringBuffer sSQL = new StringBuffer();
		sSQL.append("INSERT INTO uscensus.usdata VALUES ");
		try {
			// t = connection.createStatement();
			for (int i = 0; i < sql.size(); i++) {
				sSQL.append("(");
				sSQL.append(sql.get(i));
				sSQL.append("),");
				//t.addBatch(sql.get(i));
			}
			//t.executeBatch();
			sSQL.deleteCharAt(sSQL.length()-1);
			//System.out.println(sSQL.toString());
			t.execute(sSQL.toString());
			
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public ResultSet getValues(String attribute) {
		ResultSet rs = null;
		Statement t;

		try {
			t = connection.createStatement();
			rs = t.executeQuery("select distinct " + attribute + " from " + database + "." + table);
			//rs = t.executeQuery("select distinc* from " + database + "." + infoTable);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rs;
	}
	
	// get all attributes' names
	public ResultSet getAttributes() {
		ResultSet rs = null;
		Statement t;

		try {
			t = connection.createStatement();
			rs = t.executeQuery("show columns from " + database + "." + table);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rs;
	}
	
	public void createIndex(ArrayList<Attribute> attributes) {
		String sql = "";
		try {
		for (int i = 0; i < attributes.size(); i++) {
			System.out.println(i);
			sql = "CREATE INDEX test"+attributes.get(i).getName()+" ON " + database + "." + table + "("+attributes.get(i).getName()+")";
			//sql = "DROP INDEX test"+attributes.get(i).getName()+" ON " + database + "." + table + "("+attributes.get(i).getName()+")";
			t.execute(sql);
		}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public ResultSet getCount(String attribute, String value) {
		try {
			//t = connection.createStatement();
			if (attribute.equals("*")) {
				rs = t.executeQuery("select count(*) from " + database + "." + table);
			} else {
				rs = t.executeQuery("select count(*) from " + database + "." + table + " where " + attribute + " = '" + value + "'");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rs;
	}
	
	public ResultSet selectIItems(int i) {
		try {
			t = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
			        ResultSet.CONCUR_UPDATABLE);
			//String stat = "select * from " + database
			//		+ "." + table + " order by caseid LIMIT " + i + ",10000";
			String stat = "select * from " + database
					+ "." + table + " where caseid>=(select caseid from " + database
					+ "." + table + " order by caseid limit " + i + ",1) limit 10000";
			//String stat = "select id,title from collect where id>=(select id from collect order by id limit 90000,1) limit 10";
			System.out.println(stat);
			rs = t.executeQuery(stat);
			//tmpRS.last();
			//System.out.println(tmpRS.getRow());
			//t.execute("insert into " + database + ".attrinfo values ('" + attribute + "', '" + values +"')");
			//t.executeQuery("select count(*) from " + database + "." + sampleTable);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rs;
	}
	
	public boolean save2info(String attribute, String values) {
		Statement t;
		try {
			
			t = connection.createStatement();
			t.execute("insert into " + database + "." + infoTable + " values ('" + attribute + "', '" + values +"')");
			//t.executeQuery("select count(*) from " + database + "." + sampleTable);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}
	
	public void close() {
		try {
			t.close();
			rs.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
