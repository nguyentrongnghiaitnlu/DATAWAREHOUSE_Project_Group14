package dataBase;

public class Data {
	public Data() {
		
	}
	private int iD;
	private String lastName;
	private String firstName;
	private String dayBorn;
	private String iDClass;
	private String className;
	private int phoneNumber;
	private String email;
	private String address;
	private String note;
	public Data( int iD, String firstName, String lastName, String dayBorn, String iDClass,
			String className, int phoneNumber, String email, String address, String note) {
		super();
		this.iD = iD;
		this.lastName = lastName;
		this.firstName = firstName;
		this.dayBorn = dayBorn;
		this.iDClass = iDClass;
		this.className = className;
		this.phoneNumber = phoneNumber;
		this.email = email;
		this.address = address;
		this.note = note;
	}


	public int getiD() {
		return iD;
	}

	public void setiD(int iD) {
		this.iD = iD;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getDayBorn() {
		return dayBorn;
	}

	public void setDayBorn(String dayBorn) {
		this.dayBorn = dayBorn;
	}

	public String getiDClass() {
		return iDClass;
	}

	public void setiDClass(String iDClass) {
		this.iDClass = iDClass;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public int getPhoneNumber() {
		return phoneNumber;
	}

	public void setPhoneNumber(int phoneNumber) {
		this.phoneNumber = phoneNumber;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}
	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}
	public String getNote() {
		return note;
	}

	public void setNote(String note) {
		this.note = note;
	}
	
}
