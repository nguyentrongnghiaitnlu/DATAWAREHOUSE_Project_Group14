package lophoc;

public class lophoc {
	int MaLH;
	int MaMH;
	String TenMH;
	int tinchi;
	String Khoa;
	public lophoc(int maLH, int maMH, String tenMH, int tinchi, String khoa) {
		super();
		MaLH = maLH;
		MaMH = maMH;
		TenMH = tenMH;
		this.tinchi = tinchi;
		Khoa = khoa;
	}
	public int getMaLH() {
		return MaLH;
	}
	public void setMaLH(int maLH) {
		MaLH = maLH;
	}
	public int getMaMH() {
		return MaMH;
	}
	public void setMaMH(int maMH) {
		MaMH = maMH;
	}
	public String getTenMH() {
		return TenMH;
	}
	public void setTenMH(String tenMH) {
		TenMH = tenMH;
	}
	public int getTinchi() {
		return tinchi;
	}
	public void setTinchi(int tinchi) {
		this.tinchi = tinchi;
	}
	public String getKhoa() {
		return Khoa;
	}
	public void setKhoa(String khoa) {
		Khoa = khoa;
	}
	
}
