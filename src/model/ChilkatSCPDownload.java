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
	private String sqlCheck;
	private PreparedStatement pstCheck = null;
	private ResultSet rsCheck;
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

			// 1.1.1. sendMail error hostname or port
//			sendMail("17130135@st.hcmuaf.edu.vn", "DATA WAREHOUSE", "Server không tìm thấy...!!");
			return false;
		}

		ssh.put_IdleTimeoutMs(5000);
		// 1.2. Kiểm tra tài khoản và mặt khẩu kết nối đến address server thành công hay
		// chưa?
		success = ssh.AuthenticatePw(user_connect, password_connect);
		if (success != true) {
			// 1.2.1. sendmail error username or password
//			sendMail("17130135@st.hcmuaf.edu.vn", "DATA WAREHOUSE", "Tài khoản hoặc mặt khẩu sai!...");
			return false;
		}
		CkScp scp = new CkScp();
		// 1.3. Kiểm tra sử dụng SSH với tài khoản và mặt khẩu kết nối đến address
		// server thành công hay chưa?
		success = scp.UseSsh(ssh);
		if (success != true) {
			// 1.3.1. sendMail error using ssh
//			sendMail("17130135@st.hcmuaf.edu.vn", "DATA WAREHOUSE", "Không có kết nối!...");
			return false;
		}
		scp.put_SyncMustMatch(synMustMath);
//		scp.put_SyncMustNotMatch(synMustMath);
		// không tải những file đã tải rồi
		success = scp.SyncTreeDownload(server_path, local_path, mode_scp, false);
		// 1.4. Kiểm tra đã tải được file hay chưa
		if (success != true) {
			// 1.4.1. sendMail error download fail
//			sendMail("17130135@st.hcmuaf.edu.vn", "DATA WAREHOUSE",
//					"Thư mực server hoặc địa chỉ local không tìm thấy!...");
			return false;
		}
		// 1.4.2. sendMail download success
//		sendMail("17130135@st.hcmuaf.edu.vn", "DATA WAREHOUSE", "Đã tải file thành công!...");
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
				System.out.println(host_name_scp);
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
				} else {
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

	// 1.5. method insertDatalog success or fail?
	@SuppressWarnings("unused")
	public boolean insertDataLog() {
		int rs = 0;
		boolean check = false;
		if (!isDownLoadSCPChilkat()) {
			return check;
		} else {
			sql = "INSERT INTO log (file_name,data_file_config_id,file_status,staging_load_count, timestamp_download, timestamp_insert_staging, timestamp_insert_datawarehouse)"
					+ " values (?,?,?,?,?,?,?)";
			File localPath = new File(local_path);
			File[] listFileLog = localPath.listFiles();

			lable: for (int i = 0; i < listFileLog.length; i++) {
				String sqlCheck = "select * from log";
				try {
					pstCheck = new GetConnection().getConnection("controldb").prepareStatement(sqlCheck);
					rsCheck = pstCheck.executeQuery();
					while (rsCheck.next()) {
						if (rsCheck.getString(2).equals(listFileLog[i].getName())) {
							continue lable;
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
					return check;
				} finally {
					try {
						if (pstCheck != null)
							pstCheck.close();
						if (this.rsCheck != null)
							this.rsCheck.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}

				}

				try {
					pst = new GetConnection().getConnection("controldb").prepareStatement(sql);
					pst.setString(1, listFileLog[i].getName());
//								System.out.println(listFileLog[i].getName());
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
		}

		return check;

	}

	// 1.6. sendMailInsertLog success or fail
	public void sendMailInsertLog() {
		if (insertDataLog() == true) {
			// 1.6.1 if success, sendmail with content success
//			sendMail("17130135@st.hcmuaf.edu.vn", "DATA WAREHOUSE", "Ghi log thành công rồi bồ tèo...!");
			System.out.println("Send mail write log success!!!");
		} else {
			// 1.6.2 if fail, sendmail with content fail
//			sendMail("17130135@st.hcmuaf.edu.vn", "DATA WAREHOUSE", "Ghi log fail rồi bồ tèo...!");
			System.out.println("Send mail write log fail!!!");

		}
	}

	// -----------------------------------------------------------------------------------------------------

	public static void main(String[] args) {
		ChilkatSCPDownload chil = new ChilkatSCPDownload();
		chil.sendMailInsertLog();
//		chil.isDownLoadSCPChilkat();
	}
}
