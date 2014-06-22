package hive.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJdbcClient {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	/**
	 * @param args
	 * @throws SQLException
	 */
	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		// replace "hive" here with the name of the user the queries should run
		// as
		Connection con = DriverManager.getConnection(
				"jdbc:hive2://localhost:10000/default", "cloudera", "cloudera");
		Statement stmt = con.createStatement();
		String tableName = "synonym";
		


		// select * query
		String sql = "select * from " + tableName;
		System.out.println("Running: " + sql);
		ResultSet res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(String.valueOf(res.getString(1)) + "\t"
					+ res.getString(2));
		}

		
	}
	
	
	public String[] searchString(String searchKeyword)
	{
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		// replace "hive" here with the name of the user the queries should run
		// as
		Connection con;
		try {
			con = DriverManager.getConnection(
					"jdbc:hive2://localhost:10000/default", "cloudera", "cloudera");
		
		Statement stmt = con.createStatement();
		String tableName = "synonym";
		


		// select * query
		String sql = "select * from " + tableName +" where name =\""+searchKeyword+"\"";
		System.out.println("Running: " + sql);
		ResultSet res = stmt.executeQuery(sql);
		if (res.next()) {
			System.out.println(String.valueOf(res.getString(1)) + "\t"
					+ res.getString(2));
			
			return new String[]{res.getString(2)};
			
			
		}
		} 
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		}
		return null;

	}
	
	
}