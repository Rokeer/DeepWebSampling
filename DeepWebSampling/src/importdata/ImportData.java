package importdata;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import dao.DAO;

public class ImportData {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			FileReader read = new FileReader(
					"C:/Users/UIC/Desktop/Study/Research/FYP-Lian/data.txt");
			@SuppressWarnings("resource")
			BufferedReader br = new BufferedReader(read);
			String row;
			DAO dao = new DAO();
			int count = 0;

			String sql = "";
			String[] attributes;
			ArrayList<String> sqls = new ArrayList<String>();
			
			long a = System.currentTimeMillis();
			long b,c;
			while ((row = br.readLine()) != null && count < 1000) {
			//while ((row = br.readLine()) != null) {
				// System.out.println(row);
				b = System.currentTimeMillis();
				attributes = row.split(",");
				sql = "";
				//sql = "INSERT INTO uscensus.alltestusdata VALUES (";
				for (int i = 1; i < attributes.length; i++) {
					sql = sql + "'" + attributes[i] + "',";
				}
				//sql = sql.substring(0, sql.length() - 1) + ")";
				sql = sql.substring(0, sql.length() - 1);
				count = count + 1;
				//System.out.println(count);
				// System.out.println(sql);
				sqls.add(sql);
				if (sqls.size() == 1000) {
					dao.insertItem(sqls);
					sqls.clear();
					c = System.currentTimeMillis();
					System.out.println("Running time for this 1000 record: " + (c - b));
				}
				
			}
			c = System.currentTimeMillis();
		    System.out.println("Running time for all record: " + (c - a));

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
