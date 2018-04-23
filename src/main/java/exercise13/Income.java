package exercise13;

public class Income {

    private String date;

    private float income;

    public float getIncome() {
        return this.income;
    }

    public void setIncome(float givenIncome) {
        this.income = givenIncome;
    }

    public String getDate() {
        return this.date;
    }

    public void setDate(String givenDate) {
        this.date = givenDate;
    }

    public String toString() {
        return this.date + "_" + String.valueOf(this.income);
    }

}
