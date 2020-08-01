package dangki;

public class dangki {
		int MaDK;
		String MSSV;
		String MaLH;
		String thoiGianDK;
		public dangki(int maDK, String mSSV, String maLH, String thoiGianDK) {
			super();
			MaDK = maDK;
			MSSV = mSSV;
			MaLH = maLH;
			this.thoiGianDK = thoiGianDK;
		}
		public int getMaDK() {
			return MaDK;
		}
		public void setMaDK(int maDK) {
			MaDK = maDK;
		}
		public String getMSSV() {
			return MSSV;
		}
		public void setMSSV(String mSSV) {
			MSSV = mSSV;
		}
		public String getMaLH() {
			return MaLH;
		}
		public void setMaLH(String maLH) {
			MaLH = maLH;
		}
		public String getThoiGianDK() {
			return thoiGianDK;
		}
		public void setThoiGianDK(String thoiGianDK) {
			this.thoiGianDK = thoiGianDK;
		}
		
}
