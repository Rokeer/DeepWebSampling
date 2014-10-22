package importdata;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;

import config.Util;
import dao.DAO;
import entity.Attribute;

public class Measure {

	
	public static double getMeasure (String name) {
		ArrayList<String> sampleDatabases = new ArrayList<String>();
		/**
		sampleDatabases.add("wagsdb");
		sampleDatabases.add("wagsdb1");
		sampleDatabases.add("wagsdb2");
		sampleDatabases.add("wagsdb3");
		sampleDatabases.add("cdtsdb");
		sampleDatabases.add("ahsdb");
		sampleDatabases.add("ahsdb1");
		sampleDatabases.add("hdbsdb");
		sampleDatabases.add("hdbsdb1");
		sampleDatabases.add("hdbsdb2");
		sampleDatabases.add("hdbsdb3");
		sampleDatabases.add("hdbrosdb");
		sampleDatabases.add("hdbrosdb1");
		sampleDatabases.add("rsdb");
		sampleDatabases.add("rsdb1");
		**/
		sampleDatabases.add(name);
		DAO dao = new DAO("uscensus", "usdatanoid", "", "attrinfo");
		ArrayList<Attribute> attributes = new ArrayList<Attribute>();
		Hashtable<String, ArrayList<String>> conditions = new Hashtable<String, ArrayList<String>>();
		ResultSet rs = dao.getInfo();
		Hashtable<String, Double> oriTable = new Hashtable<String, Double>();
		try {
			while (rs.next()) {
				ArrayList<String> values = new ArrayList<String>();
				String[] sValues = rs.getString(2).split(";");
				int count = 0;
				for (int i = 0; i < sValues.length; i++) {
					values.add(sValues[i]);
					count = count + 1;
				}
				attributes.add(new Attribute(rs.getString(1), count));
				conditions.put(rs.getString(1), values);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		rs = dao.getCount("*", "*");
		double allCount = 0.0;
		try {
			rs.next();
			allCount = rs.getDouble(1);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		// create oriTable
		ArrayList<String> tmpList = new ArrayList<String>();
		String tmpAttribute = "";
		String tmpValue = "";
		for (int i = 0; i < attributes.size(); i++) {
			tmpAttribute = attributes.get(i).getName();
			
			tmpList = conditions.get(tmpAttribute);
			for (int j = 0; j < tmpList.size(); j++) {
				tmpValue = tmpList.get(j);
				rs = dao.getCount(tmpAttribute, tmpValue);
				
				try {
					while (rs.next()) {
						oriTable.put(tmpAttribute + "*" + tmpValue, rs.getDouble(1)/allCount);
						System.out.println("count: " + i + " " + tmpAttribute + " = " + tmpValue + " p = " + rs.getDouble(1));
						
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
			}
		}
		
		
		double sampleCount = 0.0;
		double result = 0.0;
		// calculate KL for each sample database
		for (int m = 0; m < sampleDatabases.size(); m++) {
			Hashtable<String, Double> samTable = new Hashtable<String, Double>();
			rs = dao.getCountFromSample("*", "*", sampleDatabases.get(m));
			try {
				rs.next();
				sampleCount = rs.getDouble(1);
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
			//create table for sample database
			for (int i = 0; i < attributes.size(); i++) {
				tmpAttribute = attributes.get(i).getName();
				
				tmpList = conditions.get(tmpAttribute);
				for (int j = 0; j < tmpList.size(); j++) {
					tmpValue = tmpList.get(j);
					rs = dao.getCountFromSample(tmpAttribute, tmpValue, sampleDatabases.get(m));
					
					try {
						while (rs.next()) {
							samTable.put(tmpAttribute + "*" + tmpValue, rs.getDouble(1)/sampleCount);
							//System.out.println("count: " + i + " " + tmpAttribute + " = " + tmpValue + " p = " + rs.getDouble(1));
							
						}
					} catch (SQLException e) {
						e.printStackTrace();
					}
					
				}
			}
			
			// calculate KL
			result = Util.calculateKL(attributes, conditions, oriTable, samTable);
			System.out.println(sampleDatabases.get(m) + ": " + result);
			
		}
		return result;
	}

	
	

}
