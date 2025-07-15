import java.time.LocalDate;

public class WeatherData {
    private String city;
    private String date;
    private int temperature;
    private String condition;

    public String getCity() { return city; }
    public void setCity(String city) { this.city = city; }

    public String getDate() { return date; }
    public void setDate(String date) { this.date = date; }

    public int getTemperature() { return temperature; }
    public void setTemperature(int temperature) { this.temperature = temperature; }

    public String getCondition() { return condition; }
    public void setCondition(String condition) { this.condition = condition; }

    public LocalDate getLocalDate() {
        return LocalDate.parse(date);
    }
}
