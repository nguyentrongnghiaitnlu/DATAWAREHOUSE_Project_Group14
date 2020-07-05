package model;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.commons.compress.archivers.dump.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;

public class ChilkatSCPDownload {
	private PreparedStatement pst = null;
	private ResultSet rs = null;
	private String sql;
	private Connection conn = null;
	DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
	LocalDateTime now = LocalDateTime.now();
	String timestamp = dtf.format(now);
	static final String NUMBER_REGEX = "^[0-9]+$";
	String server_path;
	String local_path;
	static {
		try {
			System.loadLibrary("chilkat"); // copy file chilkat.dll vao thu muc
											// project
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
	}

	// phương thức sendMail
	public static boolean sendMail(String to, String subject, String bodyMail) {
		Properties props = new Properties();
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.smtp.port", "587");
		Session session = Session.getInstance(props, new javax.mail.Authenticator() {
			@Override
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication("nghjaalvin@gmail.com", "sgkuabpfvebdvhsn");
			}
		});
		try {
			MimeMessage message = new MimeMessage(session);
			message.setHeader("Content-Type", "text/plain; charset=UTF-8");
			message.setFrom(new InternetAddress("nghjaalvin@gmail.com"));
			message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
			message.setSubject(subject, "UTF-8");
			message.setText(bodyMail, "UTF-8");
			Transport.send(message);
		} catch (MessagingException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	// 1. method chilkatSCPDownload: kiểm tra kết nối đến serverm port, tài khoản,
	// mặt khẩu có bị lỗi gì hay không?
	public boolean chilkatSCPDownLoad(String hostname, int port, String user_connect, String password_connect,
			String synMustMath, String server_path, String local_path, int mode_scp) {
		CkSsh ssh = new CkSsh();
		CkGlobal ck = new CkGlobal();
		ck.UnlockBundle("Hello Team14");
		// 1.1. Kiểm tra kết nối đến address server thành công hay chưa?
		boolean success = ssh.Connect(hostname, port);
		if (success != true) {
			//1.1.1. sendMail error hostname or port
			sendMail("17130135@st.hcmuaf.edu.vn", "DATA WAREHOUSE", "Server không tìm thấy...!!");
			return false;
		}

		ssh.put_IdleTimeoutMs(5000);
		// 1.2. Kiểm tra tài khoản và mặt khẩu kết nối đến address server thành công hay
		// chưa?
		success = ssh.AuthenticatePw(user_connect, password_connect);
		if (success != true) {
			// 1.2.1. sendmail error username or password
			sendMail("17130135@st.hcmuaf.edu.vn", "DATA WAREHOUSE", "Tài khoản hoặc mặt khẩu sai!...");
			return false;
		}
		CkScp scp = new CkScp();
		// 1.3. Kiểm tra sử dụng SSH với tài khoản và mặt khẩu kết nối đến address
		// server thành công hay chưa?
		success = scp.UseSsh(ssh);
		if (success != true) {
			//1.3.1. sendMail error using ssh
			sendMail("17130135@st.hcmuaf.edu.vn", "DATA WAREHOUSE", "Không có kết nối!...");
			return false;
		}
		scp.put_SyncMustMatch(synMustMath);
//		scp.put_SyncMustNotMatch(synMustMath); //không tải những file đã tải rồi
		success = scp.SyncTreeDownload(server_path, local_path, mode_scp, false);
		// 1.4. Kiểm tra đã tải được file hay chưa
		if (success != true) {
			//1.4.1. sendMail error  download fail
			sendMail("17130135@st.hcmuaf.edu.vn", "DATA WAREHOUSE",
					"Thư mực server hoặc địa chỉ local không tìm thấy!...");
			return false;
		}
		//1.4.2. sendMail download success
		sendMail("17130135@st.hcmuaf.edu.vn", "DATA WAREHOUSE", "Đã tải file thành công!...");
		ssh.Disconnect();
		return true;

	}

	// phương thức kiểm kiểm tra đã download được hay chưa?
	public boolean isDownLoadSCPChilkat() {
		boolean result = false;
		sql = "SELECT * FROM scp";
		try {
			// 1. select fields table SCP in data SCP using download
			pst = new GetConnection().getConnection("controldb").prepareStatement(sql);
			rs = pst.executeQuery();
			while (rs.next()) {
				int id = rs.getInt("id_scp");
				String load_library = rs.getString("load_library");
				String host_name_scp = rs.getString("host_name_scp");
				int port_scp = rs.getInt("port_scp");
				String username_scp = rs.getString("username_scp");
				String password_scp = rs.getString("password_scp");
				String sync_must_math = rs.getString("sync_must_math");
				server_path = rs.getString("server_path");
				local_path = rs.getString("local_path");
				int mode_scp = rs.getInt("mode_scp");
				
				if (chilkatSCPDownLoad(host_name_scp, port_scp, username_scp, password_scp, sync_must_math, server_path,
						local_path, mode_scp)) {

					result = true;
					return result;
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			return result;

		} finally {
			try {
				if (pst != null)
					pst.close();
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}

		return result;
	}
	//1.5. method insertDatalog success or fail?
	public boolean insertDataLog() {
		int rs = 0;
		boolean check = false;
		if (!isDownLoadSCPChilkat()) {
			return check;
		}
		sql = "INSERT INTO log (file_name,data_file_config_id,file_status,staging_load_count, timestamp_download, timestamp_insert_staging, timestamp_insert_datawarehouse)"
				+ " values (?,?,?,?,?,?,?)";
		File localPath = new File(local_path);
		File[] listFileLog = localPath.listFiles();
		for (int i = 0; i < listFileLog.length; i++) {
			try {
				pst = new GetConnection().getConnection("controldb").prepareStatement(sql);
				pst.setString(1, listFileLog[i].getName());
				if (listFileLog[i].getName().substring(listFileLog[i].getName().lastIndexOf(".")).equals(".txt")) {
					pst.setInt(2, 1);
				} else if (listFileLog[i].getName().substring(listFileLog[i].getName().lastIndexOf("."))
						.equals(".xlsx")) {
					pst.setInt(2, 2);
				} else {
					pst.setInt(2, 3);
				}
				pst.setString(3, "ER");
				pst.setInt(4, 0);
				pst.setString(5, timestamp);
				pst.setString(6, null);
				pst.setString(7, null);
				rs = pst.executeUpdate();
				check = true;

			} catch (Exception e) {
				e.printStackTrace();
				return check;
			} finally {
				try {
					if (pst != null)
						pst.close();
					if (this.rs != null)
						this.rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}

			}
		}
		return check;

	}
	//1.6. sendMailInsertLog success or fail
	public void sendMailInsertLog() {
		if (insertDataLog() == true) {
			//1.6.1 if success, sendmail with content success
			sendMail("17130135@st.hcmuaf.edu.vn", "DATA WAREHOUSE", "Ghi log thành công rồi bồ tèo...!");
			System.out.println("Send mail write log success!!!");
		} else {
			//1.6.2 if fail, sendmail with content fail
			sendMail("17130135@st.hcmuaf.edu.vn", "DATA WAREHOUSE", "Ghi log fail rồi bồ tèo...!");
			System.out.println("Send mail write log fail!!!");

		}
	}

	// -----------------------------------------------------------------------------------------------------
//	private String readLines(String value, String delim) {
//		String values = "";
//		StringTokenizer stoken = new StringTokenizer(value, delim);
//		if (stoken.countTokens() > 0) {
//			stoken.nextToken();
//		}
//		int countToken = stoken.countTokens();
//		String lines = "(";
//		for (int j = 0; j < countToken; j++) {
//			String token = stoken.nextToken();
//			if (Pattern.matches(NUMBER_REGEX, token)) {
//				lines += (j == countToken - 1) ? token.trim() + ")," : token.trim() + ",";
//			} else {
//				lines += (j == countToken - 1) ? "'" + token.trim() + "')," : "'" + token.trim() + "',";
//			}
//			values += lines;
//			lines = "";
//		}
//		return values;
//	}
//
//	public String readValuesTXT(File s_file, String delim) {
//		String values = "";
//		try {
//			BufferedReader bReader = new BufferedReader(new InputStreamReader(new FileInputStream(s_file)));
//			String line;
//			while ((line = bReader.readLine()) != null) {
//				values += readLines(line, delim);
//			}
//			bReader.close();
//			return values.substring(0, values.length() - 1);
//
//		} catch (NoSuchElementException | IOException e) {
//			e.printStackTrace();
//			return null;
//		}
//	}
//
//	// + config.import_dir +file.seperator+ log.file_name
//	public String readValuesXLSX(File s_file) {
//		String values = "";
//		String value = "";
//		try {
//			FileInputStream fileIn = new FileInputStream(s_file);
//			XSSFWorkbook workBooks = new XSSFWorkbook(fileIn);
//			XSSFSheet sheet = workBooks.getSheetAt(0);
//			Iterator<Row> rows = sheet.iterator();
//			rows.next();
//			while (rows.hasNext()) {
//				Row row = rows.next();
//				Iterator<Cell> cells = row.cellIterator();
//				while (cells.hasNext()) {
//					Cell cell = cells.next();
//					CellType cellType = cell.getCellType();
//					switch (cellType) {
//					case NUMERIC:
//						if (DateUtil.isCellDateFormatted(cell)) {
//							SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
//							value += dateFormat.format(cell.getDateCellValue()) + "|";
//						} else {
//							value += (long) cell.getNumericCellValue() + "|";
//						}
//
//						break;
//					case STRING:
//						value += cell.getStringCellValue() + "|";
//						break;
//					default:
//						break;
//					}
//				}
//				values += readLines(value.substring(0, value.length() - 1), "|");
//				value = "";
//			}
//			workBooks.close();
//			fileIn.close();
//			return values.substring(0, values.length());
//		} catch (IOException e) {
//			return null;
//		}
//	}
//
//	public boolean createTable(String table_name, String variables, String column_list) {
//		sql = "CREATE TABLE " + table_name + " (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,";
//		String[] vari = variables.split(",");
//		String[] col = column_list.split(",");
//		for (int i = 0; i < vari.length; i++) {
//			sql += col[i] + " " + vari[i] + " NOT NULL,";
//		}
//		sql = sql.substring(0, sql.length() - 1) + ")";
//		try {
//			pst = new GetConnection().getConnection("staging").prepareStatement(sql);
//			pst.executeUpdate();
//			return true;
//		} catch (SQLException e) {
//			e.printStackTrace();
//			return false;
//		} finally {
//			try {
//				if (pst != null)
//					pst.close();
//				if (rs != null)
//					rs.close();
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
//
//		}
//	}
//
//	public boolean insertValues(String column_list, String values, String target_table) {
//		sql = "INSERT INTO " + target_table + "(" + column_list + ") VALUES " + values;
//		try {
//			pst = new GetConnection().getConnection("staging").prepareStatement(sql);
//			pst.executeUpdate();
//			return true;
//		} catch (SQLException e) {
//			e.printStackTrace();
//			return false;
//		} finally {
//			try {
//				if (pst != null)
//					pst.close();
//				if (rs != null)
//					rs.close();
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
//
//		}
//	}
//
//	public String getPath() {
//		String result = "";
//		sql = "select  c.import_dir, l.file_name "
//				+ "from configuration c JOIN log l ON c.config_id = l.data_file_config_id";
//		try {
//			pst = new GetConnection().getConnection("controldb").prepareStatement(sql);
//			rs = pst.executeQuery();
//			while (rs.next()) {
//				result += rs.getString(1) + File.separator + rs.getString(2) + "\n";
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return result;
//	}
//
//	public String getColumnList() {
//		String result = "";
//		sql = "select column_list from configuration";
//		try {
//			pst = new GetConnection().getConnection("controldb").prepareStatement(sql);
//			rs = pst.executeQuery();
//			while (rs.next()) {
//				result = rs.getString(1);
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return result;
//	}
//
//	public String getVariable() {
//		String result = "";
//		sql = "select variable_list from configuration";
//		try {
//			pst = new GetConnection().getConnection("controldb").prepareStatement(sql);
//			rs = pst.executeQuery();
//			while (rs.next()) {
//				result = rs.getString(1);
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return result;
//	}
//
//	public String getImport_Dir() {
//		String result = "";
//		sql = "select import_dir from configuration";
//		try {
//			pst = new GetConnection().getConnection("controldb").prepareStatement(sql);
//			rs = pst.executeQuery();
//			while (rs.next()) {
//				result = rs.getString(1);
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return result;
//	}
//
//	public String getTagetTable() {
//		String result = "";
//		sql = "select target_table from configuration";
//		try {
//			pst = new GetConnection().getConnection("controldb").prepareStatement(sql);
//			rs = pst.executeQuery();
//			while (rs.next()) {
//				result = rs.getString(1);
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return result;
//	}
//
//	public String getFileType() {
//		String result = "";
//		sql = "select file_type from configuration";
//		try {
//			pst = new GetConnection().getConnection("controldb").prepareStatement(sql);
//			rs = pst.executeQuery();
//			while (rs.next()) {
//				result = rs.getString(1);
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return result;
//	}
//
//	public String getSuccessDir() {
//		String result = "";
//		sql = "select success_dir from configuration";
//		try {
//			pst = new GetConnection().getConnection("controldb").prepareStatement(sql);
//			rs = pst.executeQuery();
//			while (rs.next()) {
//				result = rs.getString(1);
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return result;
//	}
//
//	public String getErrorDir() {
//		String result = "";
//		sql = "select error_dir from configuration";
//		try {
//			pst = new GetConnection().getConnection("controldb").prepareStatement(sql);
//			rs = pst.executeQuery();
//			while (rs.next()) {
//				result = rs.getString(1);
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return result;
//	}
//
//	public String getDelimeter() {
//		String result = "";
//		sql = "select delimmeter from configuration";
//		try {
//			pst = new GetConnection().getConnection("controldb").prepareStatement(sql);
//			rs = pst.executeQuery();
//			while (rs.next()) {
//				result = rs.getString(1);
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return result;
//	}
//
//	public String getCofigID() {
//		String result = "";
//		sql = "select config_id from configuration";
//		try {
//			pst = new GetConnection().getConnection("controldb").prepareStatement(sql);
//			rs = pst.executeQuery();
//			while (rs.next()) {
//				result = rs.getString(1);
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return result;
//	}
//
//	public boolean writeDataToBD(String column_list, String values, String target_table) {
//
//		if (insertValues(column_list, values, target_table))
//			return true;
//		return false;
//	}
//
//	public boolean tableExist(String table_name) {
//		try {
//			DatabaseMetaData dbm = new GetConnection().getConnection("staging").getMetaData();
//			ResultSet tables = dbm.getTables(null, null, table_name, null);
//			try {
//				if (tables.next()) {
//					return true;
//				}
//			} catch (SQLException e) {
//				e.printStackTrace();
//				return false;
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//			return false;
//		}
//
//		return false;
//	}
//
//	public boolean insertLog(String file_status, int staging_load_count, String timestamp_insert_staging) {
//		sql = "Update log SET file_status = " + "'" + file_status + "'" + " ,staging_load_count = " + staging_load_count
//				+ " ,timestamp_insert_staging = " + "'" + timestamp_insert_staging + "';";
//		try {
//			pst = new GetConnection().getConnection("controldb").prepareStatement(sql);
//			pst.setString(1, file_status);
//			pst.setInt(2, staging_load_count);
//			pst.setString(3, timestamp_insert_staging);
//			// pst.setString(5, timestamp);
//			pst.executeUpdate();
//			return true;
//		} catch (SQLException e) {
//			e.printStackTrace();
//			return false;
//		} finally {
//			try {
//				if (pst != null)
//					pst.close();
//				if (rs != null)
//					rs.close();
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
//
//		}
//	}
//
//	public void ExtractToDB(ChilkatSCPDownload dp) {
//		String target_table = dp.getTagetTable();
//		if (!dp.tableExist(target_table)) {
//			String variables = dp.getVariable();
//			String column_list = dp.getColumnList();
//			dp.createTable(target_table, variables, column_list);
//		}
//		String file_type = dp.getFileType();
//		String import_dir = dp.getImport_Dir();
//		String delim = dp.getDelimeter();
//		String column_list = dp.getColumnList();
//		File imp_dir = new File(import_dir);
//		if (imp_dir.exists()) {
//			String extention = "";
//			File[] listFile = imp_dir.listFiles();
//			for (File file : listFile) {
//				if (file.getName().indexOf(file_type) != -1) {
//					String values = "";
//					if (file_type.contentEquals(".txt")) {
//						values = dp.readValuesTXT(file, delim);
//						extention = ".txt";
//					} else if (file_type.contentEquals(".xlsx")) {
//						values = dp.readValuesXLSX(file);
//						extention = ".xlsx";
//					}
//					if (values != null) {
//						String table = "data_file";
//						String file_status;
//						String config_id = dp.getCofigID();
//						// time
//						DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
//						LocalDateTime now = LocalDateTime.now();
//						String timestamp = dtf.format(now);
//						// count line
//						String stagin_load_count = "";
//						try {
//							stagin_load_count = countLines(file, extention) + "";
//						} catch (InvalidFormatException
//								| org.apache.poi.openxml4j.exceptions.InvalidFormatException e) {
//							e.printStackTrace();
//						}
//						//
//						String target_dir;
//						String file_name = file.getName().replaceAll(file_type, "");
//						if (dp.writeDataToBD(column_list, target_table, values)) {
//							file_status = "SU";
//							dp.insertLog(file_status, 0, timestamp);
//							if (moveFile(dp.getSuccessDir(), file))
//								;
//
//						} else {
//							file_status = "ERR";
//							dp.insertLog(file_status, 0, timestamp);
//							if (moveFile(dp.getErrorDir(), file))
//								;
//
//						}
//					}
//				}
//			}
//
//		} else {
//			System.out.println("Path not exists!!!");
//			return;
//		}
//	}
//
//	private boolean moveFile(String target_dir, File file) {
//		try {
//			BufferedInputStream bReader = new BufferedInputStream(new FileInputStream(file));
//			BufferedOutputStream bWriter = new BufferedOutputStream(
//					new FileOutputStream(target_dir + File.separator + file.getName()));
//			byte[] buff = new byte[1024 * 10];
//			int data = 0;
//			while ((data = bReader.read(buff)) != -1) {
//				bWriter.write(buff, 0, data);
//			}
//			bReader.close();
//			bWriter.close();
//			return true;
//		} catch (IOException e) {
//			e.printStackTrace();
//			return false;
//		} finally {
//			file.delete();
//		}
//	}
//
//	private int countLines(File file, String extention)
//			throws InvalidFormatException, org.apache.poi.openxml4j.exceptions.InvalidFormatException {
//		int result = 0;
//		XSSFWorkbook workBooks = null;
//		try {
//			if (extention.indexOf(".txt") != -1) {
//				BufferedReader bReader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
//				String line;
//				while ((line = bReader.readLine()) != null) {
//					if (!line.trim().isEmpty()) {
//						result++;
//					}
//				}
//				bReader.close();
//			} else if (extention.indexOf(".xlsx") != -1) {
//				workBooks = new XSSFWorkbook(file);
//				XSSFSheet sheet = workBooks.getSheetAt(0);
//				Iterator<Row> rows = sheet.iterator();
//				rows.next();
//				while (rows.hasNext()) {
//					rows.next();
//					result++;
//				}
//				return result;
//			}
//
//		} catch (IOException | org.apache.poi.openxml4j.exceptions.InvalidFormatException e) {
//			e.printStackTrace();
//		} finally {
//			if (workBooks != null) {
//				try {
//					workBooks.close();
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//			}
//		}
//		return result;
//	}

	public static void main(String[] args) {
		ChilkatSCPDownload chil = new ChilkatSCPDownload();
		chil.sendMailInsertLog();
//		chil.isDownLoadSCPChilkat();
	}
}
