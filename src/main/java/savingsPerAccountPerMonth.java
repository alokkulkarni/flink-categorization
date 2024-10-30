import lombok.*;

@Data
@NoArgsConstructor
public class savingsPerAccountPerMonth {
    
    private String customerId;
    private String accountNumber;
    private int year;
    private int month;
    private double savings_amount;
    private String amount_type;

    public savingsPerAccountPerMonth(String customerId, String accountNumber, int year, int month, double savings_amount, String amount_type) {
        this.customerId = customerId;
        this.accountNumber = accountNumber;
        this.year = year;
        this.month = month;
        this.savings_amount = savings_amount;
        this.amount_type = amount_type;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getAccountNumber() {
        return accountNumber;
    }

    public void setAccountNumber(String accountNumber) {
        this.accountNumber = accountNumber;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public double getSavings_amount() {
        return savings_amount;
    }

    public void setSavings_amount(double savings_amount) {
        this.savings_amount = savings_amount;
    }

    public String getAmount_type() {
        return amount_type;
    }

    public void setAmount_type(String amount_type) {
        this.amount_type = amount_type;
    }
}
