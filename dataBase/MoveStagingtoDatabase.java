package dataBase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.poi.ss.examples.formula.CheckFunctionsSupported;

import constants.Status;
import dangki.dangki;
import lophoc.lophoc;
import model.ConnectDatabase;
import model.Log;
import model.Student;
import monhoc.monhoc;

public class MoveStagingtoDatabase {


	static ConnectDatabase cdb;
	static PreparedStatement pst;
	
	static ResultSet rs;


	public MoveStagingtoDatabase() {
		cdb= new ConnectDatabase();
	}
	 public static boolean checkforexistence(int id_Student) {//Data data
			String sql="SELECT count(id_student) FROM datawarehouse.student Where id_student = ?";//datawarehouse.Student
			 
				try {
					PreparedStatement pst1;
					 int a=0;
					 pst1 = cdb.connectDBStaging().prepareStatement(sql);
					 pst1.setInt(1, id_Student);//data.getiD()
					 ResultSet rs1= pst1.executeQuery();
					 if(rs1.next()) {
						a= rs1.getInt(1);
						System.out.println(a);
					 }
					 if(a>0) return true;
					 pst1.close();
					 
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
			 return false;
			 
		 }
	 
	 DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
		LocalDateTime now = LocalDateTime.now();
		String timestamp = dtf.format(now);
		public void UpdateTimeLogInsertStaging(String namefile) throws SQLException {
			 String sql= "UPDATE controldb.log SET time_stamp_insert_staging = ? WHERE file_name = ?";
			 PreparedStatement pst1 = null;
				ResultSet rs1 = null;
				String sql1;
				ConnectDatabase cdc1= new ConnectDatabase();
				try {
						pst1 = cdc1.connectDBControl().prepareStatement(sql);
						pst1.setString(1, timestamp);
						pst1.setString(2, namefile);
						pst1.executeUpdate();
						pst1.close();
					} catch (Exception e) {
						e.printStackTrace();
					}finally {
			            if (pst1 != null) pst1.close();
			            if (rs1 != null) rs1.close();
			        }
				  
		}
	 
	  public void UpdateStatus(String srt, Status sts) throws SQLException {
			String  sql = "UPDATE controldb.log SET file_status = ? WHERE file_name = ?";
				
			PreparedStatement pst1 = null;
			ResultSet rs1 = null;
			String sql1;
			ConnectDatabase cdc1= new ConnectDatabase();
			try {
					pst1 = cdc1.connectDBControl().prepareStatement(sql);
					pst1.setString(1, sts.name());
					pst1.setString(2, srt);
					pst1.executeUpdate();
					pst1.close();
				} catch (Exception e) {
					e.printStackTrace();
				}finally {
		            if (pst1 != null) pst1.close();
		            if (rs1 != null) rs1.close();
		        }
			  
		  }
		  ///load mon hoc
		  public void BuiltDataMonHoc() throws SQLException{
			  String sql= "select * from controldb.log where data_file_config_id=3 and file_status='ER'";
			List<String> bang= new ArrayList<String>();
			  try {
				pst=cdb.connectDBControl().prepareStatement(sql);
				ResultSet rs=pst.executeQuery();
				while(rs.next()){
					bang.add(rs.getString("file_name"));
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally {
	            if (pst != null) pst.close();
	            if (rs != null) rs.close();
	        }
			  
			  int MaMH;
			  String TenMH="";
			  int tinchi;
			  String Khoa="";
			  for(String st: bang) {
				  List<monhoc> listMH= new ArrayList<monhoc>();
				  String sql1= "select * from stagingdb."+st;
				  pst=cdb.connectDBStaging().prepareStatement(sql1);
				  ResultSet rs1= pst.executeQuery();
				  while(rs1.next()) {
				  MaMH=rs1.getInt("MaMH");
				  TenMH=rs1.getString("TenMH");
				  tinchi= rs1.getInt("tinchi");
				  Khoa=rs1.getString("Khoa");
				  listMH.add(new monhoc(MaMH, TenMH, tinchi, Khoa));
				  UpdateTimeLogInsertStaging(st);
				  UpdateStatus(st, Status.SU);
			  }
				  for(monhoc MH:listMH) {
					  String sql2= "insert into datawarehouse.monhocWH (MaMH, TenMH,tinchi,Khoa) values(?,?,?,?) ";
					  
					 
					  try {
						  pst= cdb.connectDBWarehouse().prepareStatement(sql2);
						
								pst.setInt(1, MH.getMaMH());
								pst.setString(2, MH.getTenMH());
								pst.setInt(3, MH.getTinchi());
								pst.setString(4, MH.getKhoa());
						pst.executeUpdate();
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}finally {
				            if (pst != null) pst.close();
				            if (rs != null) rs.close();
				        }
					  }
				  }  
		  }
		  
		  
		  // load dang ki
		  public void BuiltDataDangKi() throws SQLException{
			  String sql= "select * from controldb.log where data_file_config_id=4 and file_status='ER'";
			List<String> bang= new ArrayList<String>();
			  try {
				pst=cdb.connectDBControl().prepareStatement(sql);
				ResultSet rs=pst.executeQuery();
				while(rs.next()){
					bang.add(rs.getString("file_name"));
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally {
	            if (pst != null) pst.close();
	            if (rs != null) rs.close();
	        }
			  
			  int MaDK;
				String MSSV="";
				String MaLH="";
				String thoiGianDK="";
			  for(String st: bang) {
				  List<dangki> listDK= new ArrayList<dangki>();
				  String sql1= "select * from stagingdb."+st;
				  pst=cdb.connectDBStaging().prepareStatement(sql1);
				  ResultSet rs1= pst.executeQuery();
				  while(rs1.next()) {
					  MaDK=rs1.getInt("MaDK");
					  MSSV=rs1.getString("MSSV");
					  MaLH=rs1.getString("MaLH");
					  thoiGianDK=rs1.getString("thoiGianDK");
				  listDK.add(new dangki(MaDK, MSSV, MaLH, thoiGianDK));
				  UpdateTimeLogInsertStaging(st);
				  UpdateStatus(st, Status.SU);
			  }
				  for(dangki dk:listDK) {
					  String sql2= "insert into datawarehouse.Dangki(MaDk,MSSV,MaLH,thoiGianDK) values(?,?,?,?) ";
					  
					 
					  try {
						  pst= cdb.connectDBWarehouse().prepareStatement(sql2);
						
								pst.setInt(1, dk.getMaDK());
								pst.setString(2,dk.getMSSV());
								pst.setString(3, dk.getMaLH());
								pst.setString(4, dk.getThoiGianDK());
						pst.executeUpdate();
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}finally {
				            if (pst != null) pst.close();
				            if (rs != null) rs.close();
				        }
					  }
				  }  
		  }
		  
		  
		  //load lop hoc
		  public void BuiltDataLopHoc() throws SQLException{
			  String sql= "select * from controldb.log where data_file_config_id=2 and file_status='ER'";
			List<String> bang= new ArrayList<String>();
			  try {
				pst=cdb.connectDBControl().prepareStatement(sql);
				ResultSet rs=pst.executeQuery();
				while(rs.next()){
					bang.add(rs.getString("file_name"));
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally {
	            if (pst != null) pst.close();
	            if (rs != null) rs.close();
	        }
			  
			  int MaLH;
				int MaMH;
				String TenMH;
				int tinchi;
				String Khoa;
			  for(String st: bang) {
				  List<lophoc> listLH= new ArrayList<lophoc>();
				  String sql1= "select * from stagingdb."+st;
				  pst=cdb.connectDBStaging().prepareStatement(sql1);
				  ResultSet rs1= pst.executeQuery();
				  while(rs1.next()) {
					  MaLH=rs1.getInt("MaLH");
					  MaMH=rs1.getInt("MaMH");
					  TenMH=rs1.getString("TenMH");
					  tinchi=rs1.getInt("tinchi");
					  Khoa=rs1.getString("Khoa");
				  listLH.add(new lophoc(MaLH, MaMH, TenMH, tinchi, Khoa));
				  UpdateTimeLogInsertStaging(st);
				  UpdateStatus(st, Status.SU);
			  }
				  for(lophoc LH:listLH) {
					  String sql2= "insert into datawarehouse.LophocWH(MaLH, MaMH, TenMH, tinchi, Khoa) values(?,?,?,?,?) ";
					  
					 
					  try {
						  pst= cdb.connectDBWarehouse().prepareStatement(sql2);
						
								pst.setInt(1, LH.getMaLH());
								pst.setInt(2, LH.getMaMH());
								pst.setString(3,LH.getTenMH());
								pst.setInt(4, LH.getTinchi());
								pst.setString(5, LH.getKhoa());
						pst.executeUpdate();
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}finally {
				            if (pst != null) pst.close();
				            if (rs != null) rs.close();
				        }
					  }
				  }  
		  }
		  
		  
		  //load staging to warehouse
		  public void BuiltDataStudent() throws SQLException{
			  String sql= "select * from controldb.log where data_file_config_id=1 and file_status='ER'";
			List<String> bang= new ArrayList<String>();
			  try {
				pst=cdb.connectDBControl().prepareStatement(sql);
				ResultSet rs=pst.executeQuery();
				while(rs.next()){
					bang.add(rs.getString("file_name"));
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally {
	            if (pst != null) pst.close();
	            if (rs != null) rs.close();
	        }
			  
			     int iD;
			   	 String lastName;
				 String firstName;
				 String dayBorn;
				 String iDClass;
				 String className;
				 int phoneNumber;
				 String email;
				 String address;
				 String note;
			  for(String boad: bang) {
				  List<Data> listStudent= new ArrayList<Data>();
				  String sql1= "select * from stagingdb."+boad;
				  pst=cdb.connectDBStaging().prepareStatement(sql1);
				  ResultSet rs1= pst.executeQuery();
				  while(rs1.next()) {
					  iD=rs1.getInt("id_student");
					  firstName=rs1.getString("first_name");
					  lastName=rs1.getString("last_name");
					  dayBorn=rs1.getString("dob");
					  iDClass=rs1.getString("id_class");
					  className=rs1.getString("class_name");
					  phoneNumber=rs1.getInt("number_phone");
					  email=rs1.getString("email");
					  address=rs1.getString("address");
					  note=rs1.getString("note");
				  listStudent.add(new Data(iD,firstName,lastName,dayBorn,iDClass,className,phoneNumber,email,address,note));
				  UpdateTimeLogInsertStaging(boad);
				  UpdateStatus(boad, Status.SU);
			  }
				  for(Data st:listStudent) {
					  String sql2= "insert into datawarehouse.student(id_student,first_name,last_name,dob,id_class,class_name,number_phone,email,address,note) values(?,?,?,?,?,?,?,?,?,?) ";
					  
					 
					  try {
						  pst= cdb.connectDBWarehouse().prepareStatement(sql2);
						  if(checkforexistence(st.getiD())) {
							  continue;
						  }else {
								pst.setInt(1, st.getiD());
								pst.setString(2,st.getFirstName());
								pst.setString(3,st.getLastName());
								pst.setString(4,st.getDayBorn());
								pst.setString(5, st.getiDClass());
								pst.setString(6,st.getClassName());
								pst.setInt(7,st.getPhoneNumber());
								pst.setString(8,st.getEmail());
								pst.setString(9,st.getAddress());
								pst.setString(10,st.getNote());
						pst.executeUpdate();
						}} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}finally {
				            if (pst != null) pst.close();
				            if (rs != null) rs.close();
				        }
					  }
				  }  
		  }
//	 	public static List<Log> getLog(Status stt) {
//	 	   PreparedStatement pst = null;
//	 	   List<Log> log1= new ArrayList<Log>();
//	 	  
//	        ResultSet rs = null;
//	        String sql;
//			sql = "SELECT * FROM controldb.log WHERE file_status = ?";
//			try {
//				pst = cdb.connectDBControl().prepareStatement(sql);
//				pst.setString(1, stt.name());
//				 rs = pst.executeQuery();
//				 while (rs.next()) {
//					 Log log= new Log();
//					log.setData_file_id(rs.getInt("data_file_id"));
//					log.setData_file_config_id(rs.getInt("data_file_config_id"));
//					log.setTime_stamp_insert_staging(rs.getString("time_stamp_insert_staging"));
//					log.setTime_stamp_download(rs.getString("time_stamp_download"));
//					log.setStaging_load_count(rs.getInt("staging_load_count"));
//					log.setServer_name(rs.getString("server_name"));
//					log.setFile_status(rs.getString("file_status"));
//					log.setFile_name(rs.getString("file_name"));
//					log1.add(log);
//				}
//				return log1;
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//			return null;
//		}
//
//	 	 public static List<Data> loadData(Log log) throws SQLException {
//		        List<Data> dscb;
//		        Data cb;
//		        Connection connection = cdb.connectDBStaging(); 
//		        PreparedStatement pst1 = null;
//		        ResultSet rs1 = null;
//		        String sql;
//		        try {
//		            dscb = new ArrayList<>();
//		            sql = "select * from stagingdb."+log.getFile_name();
//		            pst1 = connection.prepareStatement(sql);
//		            rs1 = pst1.executeQuery();
//		            while (rs1.next()) {
//		                cb = new Data();
//		                dscb.add(getData(rs1, cb));
//		            }
//		            return dscb;
////		            connection.close();
//		        } catch (SQLException e) {
//		            e.printStackTrace();
//		            return null;
//		        } finally {
//		            if (pst1 != null) pst1.close();
//		            if (rs != null) rs.close();
//		        }
//		    }
//	  public static Data getData(ResultSet rs, Data data) throws SQLException {
//	        //data.setOrdinalNumber(rs.getString("satging_id"));
//	        data.setiD(rs.getInt("id_student"));
//	        data.setFirstName(rs.getString("first_name"));
//	        data.setLastName(rs.getString("last_name"));
//	        data.setDayBorn(rs.getString("dob"));
//	        data.setiDClass(rs.getString("id_class"));
//	        data.setClassName(rs.getString("class_name"));
//	        data.setPhoneNumber(rs.getInt("number_phone"));
//	        data.setEmail(rs.getString("email"));
//	        data.setAddress(rs.getString("address"));
//	        data.setNote(rs.getString("note"));
//	        return data;
//	    }
//	  
//	  public static void insertData(List<Data> data1) {
//		String sql = "insert into datawarehouse.datawarehouse( id, 	first_name, last_name, dob, id_class, class_name, number_phone, email,address, note) "
//				+ "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)";
//		try {
//			Connection connection = cdb.connectDBWarehouse();
//
//					pst = connection.prepareStatement(sql);
//					for(Data data:data1) {
//						if(checkforexistence(data.getiD())) {	
//							continue;
//						} else {
//							
//					pst.setInt(1, data.getiD());
//					pst.setString(2, data.getFirstName());
//					pst.setString(3, data.getLastName());
//					pst.setString(4, data.getDayBorn());
//					pst.setString(5, data.getiDClass());
//					pst.setString(6, data.getClassName());
//					pst.setInt(7, data.getPhoneNumber());
//					pst.setString(8, data.getEmail());
//					pst.setString(9, data.getAddress());
//					pst.setString(10, data.getNote());
//					pst.executeUpdate();
//						}
//						
//					}
//			connection.close();
//				
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
//		}
	
//	  public  void run() throws SQLException {
//		  List<Log> listlog= getLog(Status.TR);
//		  for(Log log : listlog) {
//			  List<Data> data=loadData(log);
//			  insertData(data);
//					  log.updateLog(Status.SU);
//				  }
//	  }
	
	public static void main(String[] args) throws SQLException {
		MoveStagingtoDatabase move= new MoveStagingtoDatabase();
		move.BuiltDataStudent();
	}

}
