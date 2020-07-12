package dataBase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import model.ConnectDatabase;
import model.Student;

public class MoveStagingtoDatabase {


	static ConnectDatabase cdb;
	PreparedStatement pst;
	
	ResultSet rs;


	public MoveStagingtoDatabase() {
		cdb= new ConnectDatabase();
	}
	 public static List<Data> loadData() throws SQLException {
	        List<Data> dscb;
	        Data cb;
	        Connection connection = cdb.connectDBStaging(); 
	        PreparedStatement pst1 = null;
	        ResultSet rs = null;
	        String sql;
	        try {
	            dscb = new ArrayList<>();
	            sql = "select * from stagingdb.staging";
	            pst1 = connection.prepareStatement(sql);
	            rs = pst1.executeQuery();
	            while (rs.next()) {
	                cb = new Data();
	                dscb.add(getData(rs, cb));
	            }
	            return dscb;
	        } catch (SQLException e) {
	            e.printStackTrace();
	            return null;
	        } finally {
	            if (pst1 != null) pst1.close();
	            if (rs != null) rs.close();
	        }
	    }
	  public static Data getData(ResultSet rs, Data data) throws SQLException {
	        data.setOrdinalNumber(rs.getString("num"));
	        data.setiD(rs.getString("id"));
	        data.setFirstName(rs.getString("first_name"));
	        data.setLastName(rs.getString("last_name"));
	        data.setDayBorn(rs.getString("dob"));
	        data.setiDClass(rs.getString("id_class"));
	        data.setClassName(rs.getString("class_name"));
	        data.setPhoneNumber(rs.getString("number_phone"));
	        data.setEmail(rs.getString("email"));
	        data.setAddress(rs.getString("address"));
	        data.setNote(rs.getString("note"));
	        return data;
	    }
	  public void insertData() {
			String sql = "insert into datawarehousedb.datawarehouse(num, id, first_name, last_name, dob, id_class, class_name, number_phone, email,address, note) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)";
			try {
				for (Data data1 : loadData()) {
					pst = cdb.connectDBWarehouse().prepareStatement(sql);
					pst.setString(1, data1.getOrdinalNumber());
					pst.setString(2, data1.getiD());
					pst.setString(3, data1.getFirstName());
					pst.setString(4, data1.getLastName());
					pst.setString(5, data1.getDayBorn());
					pst.setString(6, data1.getiDClass());
					pst.setString(7, data1.getClassName());
					pst.setString(8, data1.getPhoneNumber());
					pst.setString(9, data1.getEmail());
					pst.setString(10, data1.getAddress());
					pst.setString(11, data1.getNote());
					pst.executeUpdate();
				}
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	public void createTable() throws SQLException {
		String result = "";
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("create table if not exists datawarehousedb.datawarehouse(");
		stringBuilder.append(");");
		System.out.println(stringBuilder.toString());
		result = stringBuilder.toString();

		Connection connection = cdb.connectDBStaging();
		PreparedStatement preparedStatement = connection.prepareStatement(result);
		int rs = preparedStatement.executeUpdate();
	}
	public static void main(String[] args) {
		MoveStagingtoDatabase move= new MoveStagingtoDatabase();
		move.insertData();
	}

}
