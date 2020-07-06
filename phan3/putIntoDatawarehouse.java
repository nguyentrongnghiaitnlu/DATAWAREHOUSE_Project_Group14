package phan3;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import hung_project.ConnectDB;
import hung_project.LoadtoDW;

public class putIntoDatawarehouse {
	static String start = null;
	static String stop = null;

	private static void LoadStagingtoDataWarehouse(String jdbcURL, String username, String password) throws SQLException {
		// In ra thoi gian
		DateTimeFormatter DTF = DateTimeFormatter.ofPattern("HH:mm:ss yyyy/MM/dd");
		LocalDateTime now = LocalDateTime.now();
		start = DTF.format(now);
		System.out.println(start);
		// Ket noi voi Database
				ConnectDB conDB = new ConnectDB();
				conDB.connectDatabase(jdbcURL, username, password);
				// Tao query
				Statement sta = conDB.connection.createStatement();
				String errorQ = "SET SQL_SAFE_UPDATES = 0";
				sta.executeUpdate(errorQ);
				System.out.println("Ngat ket noi");
				// Chen Data den Datawarehouse
		
				String insertStore = "INSERT DATAWAREHOUSE.DATA SELECT * FROM SINHVIEN.STAGING ";
				sta.executeUpdate(insertStore);
				System.out.println("Lay data tu Staging den Datawarehouse");
				String DBsinhvien = "Su dung DB sinhvien";
				sta.executeUpdate(DBsinhvien);
				System.out.println("select DB sinhvien");
				// Tao mot Temp
				String createTemp = "CREATE TABLE TEMP AS SELECT * FROM "
						+ "(SELECT * FROM SINHVIEN UNION ALL SELECT * FROM STAGING)"
						+ " tbl GROUP BY STT HAVING COUNT(*) = 1 ORDER BY STT";
				sta.executeUpdate(createTemp);
				String check = "SELECT COUNT(*)  FROM TEMP";
				System.out.println("Tao bang TEMP");

				System.out.println("Kiem tra 2 DataBase bang nhau?");
				ResultSet rSet = sta.executeQuery(check);
				int count = 0;
				while (rSet.next()) {
					count = rSet.getInt(1);
				}
				System.out.println(count);
				// Truong hop 2 DataBase khong bang nhau
				if (count > 0) {
					System.out.println("2 DataBase khong bang nhau");
					String stt = null;
					String error = "SELECT STT FROM TEMP";
					rSet = sta.executeQuery(error);
					while (rSet.next()) {
						stt = rSet.getString("STT");
						System.out.println(stt);
						System.out.println("Hien thi dong bi loi");
					}
					String deleteStore = "DELETE FROM  DATAWAREHOUSE.DATA";
					sta.executeUpdate(deleteStore);
					System.out.println("Xoa thang cong datawarehouse");
					String deleteTemp = "truncate table temp";
					sta.executeUpdate(deleteTemp);
					System.out.println("Xoa thanh cong temp");
					// Truong hop 2 DataBase giong nhau
				} else {
					System.out.println("Database giong nhau");
					System.out.println("Chen Data vao DataWarehouse");
					String insertDW = "INSERT DATAWAREHOUSE.DATAWAREHOUSE SECLECT * FROM DATAWAREHOUSE.DATA";
					sta.executeUpdate(insertDW);
					String updateLog = "UPDATE datacontrol.log SET  log.statusend ='' WHERE log.idlog LIKE '1'";
					sta.executeUpdate(updateLog);
					System.out.println("Cap nhat trang thai");
				}
				String updateLogTimeStart = "UPDATE datacontrol.log SET  log.timestart ='" + start
						+ "' WHERE log.idlog like '1'";
				sta.executeUpdate(updateLogTimeStart);
				LocalDateTime LDT = LocalDateTime.now();
				stop = DTF.format(LDT);
				String updateLogTimeEnd = "UPDATE datacontrol.log SET  log.timeend ='" + stop + "' WHERE log.idlog LIKE '1'";
				sta.executeUpdate(updateLogTimeEnd);
				System.out.println("Cap nhat lai thoi gian");
				System.out.println(stop);
	
	
	}
	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		String jdbcURL = "jdbc:mysql://localhost:3306/datawarehouse";
		String username = "root";
		String password = "000000";
		putIntoDatawarehouse ltd = new putIntoDatawarehouse();
		ltd.LoadStagingtoDataWarehouse(jdbcURL, username, password);
	
}}
