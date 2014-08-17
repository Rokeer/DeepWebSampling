package dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import config.Conn;
import config.Util;
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
	
	public ResultSet getInfo() {
		//ResultSet rs = null;
		//Statement t;

		try {
			//t = connection.createStatement();
			rs = t.executeQuery("select * from " + database + "." + infoTable);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rs;
	}
	
	public void createSampleDB(ArrayList<Attribute> al) {
		Statement t;
		String stat = "create table " + database + "." + sampleTable + " (";
		try {
			t = connection.createStatement();
			t.execute("drop table IF EXISTS " + database + "." + sampleTable);
			for (int i = 0; i < al.size(); i++) {
				stat = stat + al.get(i).getName() + " char(45), ";
			}
			stat = stat.substring(0, stat.length() - 2) + ")";
			//stat = stat.substring(0, stat.length() - 2);
			//stat = stat + ", UNIQUE KEY test1 (";
			/**
			for (int m = 0; m < al.size() / 16; m++) {
				stat = stat + ", UNIQUE KEY test" + m + " (";
				for (int i = 0; i < 16; i++) {
					stat = stat + al.get(i + 16 * m).getName() + ", ";
				}
				stat = stat.substring(0, stat.length() - 2)  + ")";
			}
			stat = stat + ")";
			**/
			//stat = stat.substring(0, stat.length() - 2) + "))";
			t.execute(stat);

		} catch (Exception e) {
			e.printStackTrace();
		}
		
		stat = "create table " + database + ".tmpTable (";
		try {
			t = connection.createStatement();
			t.execute("drop table IF EXISTS " + database + ".tmpTable");
			for (int i = 0; i < al.size(); i++) {
				stat = stat + al.get(i).getName() + " char(45), ";
			}
			stat = stat.substring(0, stat.length() - 2) + ")";
			t.execute(stat);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public ResultSet doSelect(HashMap<String, String> path, int k) {
		String sql = "select * from " + database + "." + table + " where ";
		//String sql = "select * from " + database + "." + sampleTable + " where ";
		try {
			sql = sql + Util.ConstuctSelect(path) + " LIMIT " + (k + 1);
			//System.out.println("Statment: " + sql);
			rs = t.executeQuery(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return rs;
	}
	
	public int save2SampleDBRandom(ResultSet rs, double probability) {
		//Statement t;
		int result = 0;
		String stat = "";
		//boolean flag = true;
		try {
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			rs.beforeFirst();
			
			while (rs.next()) {
				stat = "";
				t = connection.createStatement();
				for (int i = 0; i < columnCount; i++) {
					stat = stat + "'" + rs.getString(i + 1) + "',";
				}
				// System.out.println(stat);
				if (probability == 1.0) {
					t.execute("insert into " + database + "." + sampleTable
							+ " values ("
							+ stat.substring(0, stat.length() - 1) + ")");
					result = result + 1;
					//flag = false;
				} else if (Util.ToDoOrNotToDo(probability)) {
					t.execute("insert into " + database + "." + sampleTable
							+ " values ("
							+ stat.substring(0, stat.length() - 1) + ")");
					result = result + 1;
					//flag = false;
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
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
