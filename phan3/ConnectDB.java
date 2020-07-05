package phan3;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;

//import com.mysql.jdbc.PreparedStatement;

public class ConnectDB {
	PreparedStatement statement = null;
	Connection connection = null;
	void connectDatabase(String jdbcURL,String username ,String password) throws SQLException {

		connection = DriverManager.getConnection(jdbcURL, username, password);
		System.out.println("Ket noi thanh cong.");
	}
	public static void main(String[] args) throws SQLException {
		String jdbcURL = "jdbc:mysql://localhost:3306/datawarehouse";
		String username = "root";
		String password = "000000";
		ConnectDB cn= new ConnectDB();
		cn.connectDatabase(jdbcURL, username, password);
		
	}
//	 public static void main(String[] args) throws SQLException, ClassNotFoundException {
//		    Class.forName("com.mysql.cj.jdbc.Driver");
//		    Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/datawarehouse", "root", "000000");
//		    String sql = "CREATE TABLE sinhvien (" +
//		        "  id int(11) NOT NULL AUTO_INCREMENT," +
//		        "  name varchar(45) DEFAULT NULL," +
//		        "  address varchar(255) DEFAULT NULL," +
//		        "  PRIMARY KEY (id)" +
//		        ")";
//		    PreparedStatement pstmt = con.prepareStatement(sql);
//		    pstmt.execute();
//		    con.close();
//		    System.out.println("Created!");
//		  }

}
