package dataBase;

public class Data {
	public Data() {
		
	}
	private String ordinalNumber;
	private String iD;
	private String lastName;
	private String firstName;
	private String dayBorn;
	private String iDClass;
	private String className;
	private String phoneNumber;
	private String email;
	private String address;
	private String note;
	public Data(String ordinalNumber, String iD, String lastName, String firstName, String dayBorn, String iDClass,
			String className, String phoneNumber, String email, String address, String note) {
		super();
		this.ordinalNumber = ordinalNumber;
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

	public String getOrdinalNumber() {
		return ordinalNumber;
	}

	public void setOrdinalNumber(String ordinalNumber) {
		this.ordinalNumber = ordinalNumber;
	}

	public String getiD() {
		return iD;
	}

	public void setiD(String iD) {
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

	public String getPhoneNumber() {
		return phoneNumber;
	}

	public void setPhoneNumber(String phoneNumber) {
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
