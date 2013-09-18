package dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Random;

import config.Conn;
import config.SlotMachine;
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
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Boolean insertItem(ArrayList<String> sql) {
		// Statement t;
		StringBuffer sSQL = new StringBuffer();
		sSQL.append("INSERT INTO uscensus.alltestusdata VALUES ");
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
	
	public ResultSet getAHCount(String attribute, String value) {
		try {
			//t = connection.createStatement();
			if (attribute.equals("*")) {
				rs = t.executeQuery("select count(*) from " + database + "." + sampleTable);
			} else {
				rs = t.executeQuery("select count(*) from " + database + "." + sampleTable + " where " + attribute + " = '" + value + "'");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rs;
	}
	
	public ResultSet getPathCount (String attribute, String value, Hashtable<String, String> path) {
		String sql = "select count(*) from " + database + "." + table + " where ";
		try {
			//t = connection.createStatement();
			sql = sql + attribute + " = '" + value + "' AND ";
			//path.put(attribute, value);
			for (String key : path.keySet()) {
				sql = sql + key + " = '" + path.get(key) + "' AND ";
			}

			sql = sql.substring(0, sql.length() - 5);

			//System.out.println("Statment: " + sql);
			rs = t.executeQuery(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rs;
	}
	
	public ResultSet getAHPathCount (String attribute, String value, Hashtable<String, String> path) {
		String sql = "select count(*) from " + database + "." + sampleTable + " where ";
		try {
			//t = connection.createStatement();
			sql = sql + attribute + " = '" + value + "' AND ";
			//path.put(attribute, value);
			for (String key : path.keySet()) {
				sql = sql + key + " = '" + path.get(key) + "' AND ";
			}

			sql = sql.substring(0, sql.length() - 5);

			//System.out.println("Statment: " + sql);
			rs = t.executeQuery(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rs;
	}

	public ResultSet randomSelect(ArrayList<Attribute> attributes,
			Hashtable<String, ArrayList<String>> conditions,
			Hashtable<String, String> path, int k) {
		//ResultSet rs = null;
		// Statement t;
		Random random = new Random();
		int attCount = 0;
		int valueCount = 0;
		String attribute = "";
		String value = "";
		String sql = "select * from " + database + "." + table + " where ";
		try {
			// t = connection.createStatement();
			do {
				attCount = Math.abs(random.nextInt() % attributes.size());
			} while (path.get(attributes.get(attCount).getName()) != null);
			attribute = attributes.get(attCount).getName();
			valueCount = Math.abs(random.nextInt()
					% conditions.get(attribute).size());
			value = conditions.get(attribute).get(valueCount);
			path.put(attribute, value);

			for (String key : path.keySet()) {
				sql = sql + key + " = '" + path.get(key) + "' AND ";
			}

			sql = sql.substring(0, sql.length() - 5) + " LIMIT " + (k + 1);

			System.out.println("Statment: " + sql);
			rs = t.executeQuery(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return rs;
	}

	public ResultSet bruteForceSelect(ArrayList<Attribute> attributes,
			Hashtable<String, ArrayList<String>> conditions,
			Hashtable<String, String> path) {
		//ResultSet rs = null;
		// Statement t;
		Random random = new Random();
		int valueCount = 0;
		String attribute = "";
		String value = "";
		String sql = "select * from " + database + "." + table + " where ";
		try {
			// t = connection.createStatement();
			for (int i = 0; i < attributes.size(); i++) {
				attribute = attributes.get(i).getName();
				valueCount = Math.abs(random.nextInt()
						% conditions.get(attribute).size());
				value = conditions.get(attribute).get(valueCount);
				path.put(attribute, value);
			}
			for (String key : path.keySet()) {
				sql = sql + key + " = '" + path.get(key) + "' AND ";
			}

			sql = sql.substring(0, sql.length() - 5);

			System.out.println("Statment: " + sql);
			rs = t.executeQuery(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return rs;
	}

	public ResultSet hiddenDBSelect(ArrayList<Attribute> attributes,
			Hashtable<String, ArrayList<String>> conditions,
			Hashtable<String, String> path, int k) {
		//ResultSet rs = null;
		// Statement t;
		Random random = new Random();

		String attribute = attributes.get(path.size()).getName();
		int valueCount = Math.abs(random.nextInt()
				% conditions.get(attribute).size());
		String value = conditions.get(attribute).get(valueCount);
		String sql = "select * from " + database + "." + table + " where ";
		try {
			// t = connection.createStatement();
			path.put(attribute, value);

			for (String key : path.keySet()) {
				sql = sql + key + " = '" + path.get(key) + "' AND ";
			}

			sql = sql.substring(0, sql.length() - 5) + " LIMIT " + (k + 1);

			System.out.println("Statment: " + sql);
			rs = t.executeQuery(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return rs;
	}
	
	public ResultSet weightedAttributeGraphSelect(Hashtable<String, String> path, int k) {
		//ResultSet rs = null;
		// Statement t;
		String sql = "select * from " + database + "." + table + " where ";
		try {
			// t = connection.createStatement();

			for (String key : path.keySet()) {
				sql = sql + key + " = '" + path.get(key) + "' AND ";
			}

			sql = sql.substring(0, sql.length() - 5) + " LIMIT " + (k + 1);

			System.out.println("Statment: " + sql);
			rs = t.executeQuery(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return rs;
	}
	
	public ResultSet alertHybridSelect(Hashtable<String, String> path, int k) {
		String sql = "select * from " + database + "." + table + " where ";
		try {
			// t = connection.createStatement();

			for (String key : path.keySet()) {
				sql = sql + key + " = '" + path.get(key) + "' AND ";
			}

			sql = sql.substring(0, sql.length() - 5) + " LIMIT " + (k + 1);

			System.out.println("Statment: " + sql);
			rs = t.executeQuery(sql);
			return rs;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public ResultSet alertHybridCheck(Hashtable<String, String> path, int cs) {
		String sql = "select * from " + database + "." + sampleTable + " where ";
		try {
			// t = connection.createStatement();

			for (String key : path.keySet()) {
				sql = sql + key + " = '" + path.get(key) + "' AND ";
			}

			sql = sql.substring(0, sql.length() - 5) + " LIMIT " + (cs + 1);

			//System.out.println("Statment: " + sql);
			rs = t.executeQuery(sql);
			return rs;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public void countDecisionTreeSelect(Hashtable<String, String> path) {
		String sql = "select * from " + database + "." + table + " where ";
		try {
			// t = connection.createStatement();

			for (String key : path.keySet()) {
				sql = sql + key + " = '" + path.get(key) + "' AND ";
			}

			sql = sql.substring(0, sql.length() - 5);

			//System.out.println("Statment: " + sql);
			rs = t.executeQuery(sql);
			rs.last();
			Random random = new Random();
			int times = Math.abs(random.nextInt() % rs.getRow()) + 1;
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			rs.beforeFirst();
			
			for(int i = 0; i < times; i++) {
				rs.next();
			}
			
			String stat = "";
			//t = connection.createStatement();
			for (int i = 0; i < columnCount; i++) {
				stat = stat + "'" + rs.getString(i + 1) + "',";
			}
			t.execute("insert into " + database + "." + sampleTable + " values (" + stat.substring(0, stat.length() - 1) + ")");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
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
			t.execute(stat);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public boolean save2SampleDB(ResultSet rs, int size, double probability) {
		Statement t;
		String stat = "";
		SlotMachine sm = new SlotMachine();
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
				} else if (sm.toDoOrNotToDo(probability)) {
					t.execute("insert into " + database + "." + sampleTable
							+ " values ("
							+ stat.substring(0, stat.length() - 1) + ")");
				}

			}

			t = connection.createStatement();
			ResultSet tmpRS = t.executeQuery("select count(*) from " + database
					+ "." + sampleTable);
			tmpRS.next();
			System.out.println("SampleDB count: " + tmpRS.getInt(1));
			if (tmpRS.getInt(1) >= size) {
				return false;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}
	
	public void save2SampleDBAOver(ResultSet rs, double probability, int rowCount) {
		//Statement t;
		String stat = "";
		SlotMachine sm = new SlotMachine();
		boolean flag = true;
		try {
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			rs.beforeFirst();
			
			while (flag == true && rs.next()) {
				stat = "";
				//t = connection.createStatement();
				for (int i = 0; i < columnCount; i++) {
					stat = stat + "'" + rs.getString(i + 1) + "',";
				}
				// System.out.println(stat);
				if (probability == 1.0) {
					t.execute("insert into " + database + "." + sampleTable
							+ " values ("
							+ stat.substring(0, stat.length() - 1) + ")");
					flag = false;
				} else if (sm.toDoOrNotToDo(probability)) {
					t.execute("insert into " + database + "." + sampleTable
							+ " values ("
							+ stat.substring(0, stat.length() - 1) + ")");
					flag = false;
				}

			}
			if (flag) {
				Random random = new Random();
				int times = Math.abs(random.nextInt() % rowCount) + 1;
				rs.beforeFirst();
				
				for(int i = 0; i < times; i++) {
					rs.next();
				}
				
				stat = "";
				//t = connection.createStatement();
				for (int i = 0; i < columnCount; i++) {
					stat = stat + "'" + rs.getString(i + 1) + "',";
				}
				t.execute("insert into " + database + "." + sampleTable + " values (" + stat.substring(0, stat.length() - 1) + ")");
				
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public boolean save2info(String attribute, String values) {
		Statement t;
		try {
			
			t = connection.createStatement();
			t.execute("insert into " + database + ".attrinfo values ('" + attribute + "', '" + values +"')");
			//t.executeQuery("select count(*) from " + database + "." + sampleTable);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

}
