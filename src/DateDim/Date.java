package DateDim;

public class Date {
	private int date_sk;
	private String full_date;
	private String day_since_1980;
	private String month_since_1980;
	private String day_of_week;
	private String calendar_month;
	private String calendar_year;
	private String calendar_year_month;
	private String day_of_month;
	private String day_of_year;
	private String week_of_year_sunday;
	private String year_week_sunday;
	private String week_sunday_start;
	private String week_of_year_monday;
	private String year_week_monday;
	private String week_monday_star;
	private String holiday;
	private String day_type;
	public Date() {
		
	}
	public Date(int date_sk, String full_date, String day_since_1980, String month_since_1980, String day_of_week,
			String calendar_month, String calendar_year, String calendar_year_month, String day_of_month,
			String day_of_year, String week_of_year_sunday, String year_week_sunday, String week_sunday_start,
			String week_of_year_monday, String year_week_monday, String week_monday_star, String holiday,
			String day_type) {
		this.date_sk = date_sk;
		this.full_date = full_date;
		this.day_since_1980 = day_since_1980;
		this.month_since_1980 = month_since_1980;
		this.day_of_week = day_of_week;
		this.calendar_month = calendar_month;
		this.calendar_year = calendar_year;
		this.calendar_year_month = calendar_year_month;
		this.day_of_month = day_of_month;
		this.day_of_year = day_of_year;
		this.week_of_year_sunday = week_of_year_sunday;
		this.year_week_sunday = year_week_sunday;
		this.week_sunday_start = week_sunday_start;
		this.week_of_year_monday = week_of_year_monday;
		this.year_week_monday = year_week_monday;
		this.week_monday_star = week_monday_star;
		this.holiday = holiday;
		this.day_type = day_type;
	}
	public int getDate_sk() {
		return date_sk;
	}
	public void setDate_sk(int date_sk) {
		this.date_sk = date_sk;
	}
	public String getFull_date() {
		return full_date;
	}
	public void setFull_date(String full_date) {
		this.full_date = full_date;
	}
	public String getDay_since_1980() {
		return day_since_1980;
	}
	public void setDay_since_1980(String day_since_1980) {
		this.day_since_1980 = day_since_1980;
	}
	public String getMonth_since_1980() {
		return month_since_1980;
	}
	public void setMonth_since_1980(String month_since_1980) {
		this.month_since_1980 = month_since_1980;
	}
	public String getDay_of_week() {
		return day_of_week;
	}
	public void setDay_of_week(String day_of_week) {
		this.day_of_week = day_of_week;
	}
	public String getCalendar_month() {
		return calendar_month;
	}
	public void setCalendar_month(String calendar_month) {
		this.calendar_month = calendar_month;
	}
	public String getCalendar_year() {
		return calendar_year;
	}
	public void setCalendar_year(String calendar_year) {
		this.calendar_year = calendar_year;
	}
	public String getCalendar_year_month() {
		return calendar_year_month;
	}
	public void setCalendar_year_month(String calendar_year_month) {
		this.calendar_year_month = calendar_year_month;
	}
	public String getDay_of_month() {
		return day_of_month;
	}
	public void setDay_of_month(String day_of_month) {
		this.day_of_month = day_of_month;
	}
	public String getDay_of_year() {
		return day_of_year;
	}
	public void setDay_of_year(String day_of_year) {
		this.day_of_year = day_of_year;
	}
	public String getWeek_of_year_sunday() {
		return week_of_year_sunday;
	}
	public void setWeek_of_year_sunday(String week_of_year_sunday) {
		this.week_of_year_sunday = week_of_year_sunday;
	}
	public String getYear_week_sunday() {
		return year_week_sunday;
	}
	public void setYear_week_sunday(String year_week_sunday) {
		this.year_week_sunday = year_week_sunday;
	}
	public String getWeek_sunday_start() {
		return week_sunday_start;
	}
	public void setWeek_sunday_start(String week_sunday_start) {
		this.week_sunday_start = week_sunday_start;
	}
	public String getWeek_of_year_monday() {
		return week_of_year_monday;
	}
	public void setWeek_of_year_monday(String week_of_year_monday) {
		this.week_of_year_monday = week_of_year_monday;
	}
	public String getYear_week_monday() {
		return year_week_monday;
	}
	public void setYear_week_monday(String year_week_monday) {
		this.year_week_monday = year_week_monday;
	}
	public String getWeek_monday_star() {
		return week_monday_star;
	}
	public void setWeek_monday_star(String week_monday_star) {
		this.week_monday_star = week_monday_star;
	}
	public String getHoliday() {
		return holiday;
	}
	public void setHoliday(String holiday) {
		this.holiday = holiday;
	}
	public String getDay_type() {
		return day_type;
	}
	public void setDay_type(String day_type) {
		this.day_type = day_type;
	}
	
	
}
