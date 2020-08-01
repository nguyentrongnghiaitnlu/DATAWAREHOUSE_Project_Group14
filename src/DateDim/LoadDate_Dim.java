package DateDim;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.mysql.jdbc.DatabaseMetaData;

import model.ConnectDatabase;
import model.Student;

public class LoadDate_Dim {
	 ConnectDatabase cdb;
	PreparedStatement pst;
	ResultSet rs;
//	private static final int date_sk = 0;
//	private static final int full_date = 1;
//	private static final int day_since_1980 = 2;
//	private static final int month_since_1980 = 3;
//	private static final int day_of_week = 4;
//	private static final int calendar_month = 5;
//	private static final int calendar_year = 6;
//	private static final int calendar_year_month = 7;
//	private static final int day_of_month = 8;
//	private static final int day_of_year = 9;
//	private static final int week_of_year_sunday = 10;
//	private static final int year_week_sunday = 11;
//	private static final int week_sunday_start = 12;
//	private static final int week_of_year_monday = 13;
//	private static final int year_week_monday = 14;
//	private static final int week_monday_star = 15;
//	private static final int holiday = 16;
//	private static final int day_type = 17;

	public LoadDate_Dim() {
		cdb = new ConnectDatabase();
	}
	
	public  void BuiltDataDim()throws SQLException {
		
	//	Connection connection = cdb.connectDBControl();
		String col = "select * from controldb.data_config";
		String nameTable = "";
		String field = "";
		String datatype = "";
		String localdatedim = "";
		try {
		pst = cdb.connectDBControl().prepareStatement(col);
		ResultSet rs = pst.executeQuery();
		
		while(rs.next()) {
			nameTable = rs.getString("name_table_data_dim");
			field = rs.getString("field_date_dim");
			datatype = rs.getString("type_date_dim");
			localdatedim = rs.getString("local_date_dim");
		}}
		catch (SQLException e) {
            e.printStackTrace();
         
        } finally {
            if (pst != null) pst.close();
            if (rs != null) rs.close();
        }
		StringTokenizer strToField = new StringTokenizer(field, ",");
		String arrField[] = new String[18];
		int k = 0;
		System.out.println(strToField.countTokens());
		while (strToField.hasMoreElements()) {
			arrField[k] = strToField.nextToken();
			k++;
		}
		StringTokenizer strToDataType = new StringTokenizer(datatype, ",");
		String arrDataType[] = new String[18];
		int l = 0;
		System.out.println(strToDataType.countTokens());
		while (strToDataType.hasMoreElements()) {
			arrDataType[l] = strToDataType.nextToken();
			l++;
		}
		Connection conWareHouse= cdb.connectDBWarehouse();
		String sqlDest = "CREATE TABLE datawarehouse." + nameTable + "( " + arrField[0] + " " + arrDataType[0]
				+ " , " + arrField[1] + " " + arrDataType[1] + ", " + arrField[2] + " "
				+ arrDataType[3] + "," + arrField[3] + " " + arrDataType[3] + "," + arrField[4] + " " + arrDataType[4]
				+ "," + arrField[5] + " " + arrDataType[5] + "," + arrField[6] + " " + arrDataType[6] + ","
				+ arrField[7] + " " + arrDataType[7] + "," + arrField[8] + " " + arrDataType[8] + "," + arrField[9]
				+ " " + arrDataType[9] + "," + arrField[10] + " " + arrDataType[10] + "," + arrField[11] + " "
				+ arrDataType[11] + "," + arrField[12] + " " + arrDataType[12] + "," + arrField[13] + " "
				+ arrDataType[13] + "," + arrField[14] + " " + arrDataType[14] + "," + arrField[15] + " "
				+ arrDataType[15] + "," + arrField[16] + " " + arrDataType[16] + "," + arrField[17] + " "
				+ arrDataType[17] + ", PRIMARY KEY (" + arrField[0] + "))";
		System.out.println(sqlDest);
		PreparedStatement pSDataWH;
		DatabaseMetaData checkCreateTable = (DatabaseMetaData) conWareHouse.getMetaData();
		ResultSet table = checkCreateTable.getTables(null, null, nameTable, null);
		if (table.next()) {
			System.out.println("Table đã tồn tại");
		} else {
//		10. Thực hiện câu query tạo table warehouse
			pSDataWH = (PreparedStatement) conWareHouse.prepareStatement(sqlDest);
			pSDataWH.execute();

		}
		Date_Dim date_dim= new Date_Dim();
		List<String> date= date_dim.getDate();
		
		for(String i:date) {
			String[] arr=i.split(",");
		String sql = "insert into datawarehouse." +nameTable+"("+   arrField[1] + " ,"+  arrField[2] + " ,"+  arrField[3] + " ,"+  arrField[4] + " ,"
				+  arrField[5] + " ,"+  arrField[6] + " ,"+  arrField[7] + " ,"+  arrField[8] + " ,"+  arrField[9] + " ,"+  arrField[10] + " ,"+  arrField[11] + " ,"
				+  arrField[12] + " ,"+  arrField[13] + " ,"+  arrField[14] + " ,"+  arrField[15] + " ,"+  arrField[16] + " ,"+  arrField[17]  +
				")"	+ " values( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		try {
			
			pst = cdb.connectDBWarehouse().prepareStatement(sql);
			//pst.setInt(1, Integer.parseInt(arr[0]));
			pst.setString(1,arr[1]);
			pst.setString(2, arr[2]);
			pst.setString(3,arr[3]);
			pst.setString(4,arr[4]);
			pst.setString(5,arr[5]);
			pst.setString(6,arr[6]);
			pst.setString(7,arr[7]);
			pst.setString(8, arr[8]);
			pst.setString(9,arr[9]);
			pst.setString(10, arr[10]);
			pst.setString(11,arr[11]);
			pst.setString(12, arr[12]);
			pst.setString(13,arr[13]);
			pst.setString(14,arr[14]);
			pst.setString(15,arr[15]);
			pst.setString(16,arr[16]);
			pst.setString(17,arr[17]);

			pst.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	
		}
		}
	public static void main(String[] args) throws Exception {
		 LoadDate_Dim st= new LoadDate_Dim();
		 st.BuiltDataDim();
	}
	
	
		}

