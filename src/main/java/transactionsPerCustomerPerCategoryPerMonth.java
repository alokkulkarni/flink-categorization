import lombok.*;

@Data
@NoArgsConstructor
public class transactionsPerCustomerPerCategoryPerMonth {
    private String customerId;
    private String accountNumber;
    private String category;
    private int year;
    private int month;
    private int count;
    private double amount;
    private String amount_type;

    public transactionsPerCustomerPerCategoryPerMonth(String customerId, String accountNumber, String category, int year, int month, int count, double amount, String amount_type) {
        this.customerId = customerId;
        this.accountNumber = accountNumber;
        this.category = category;
        this.year = year;
        this.month = month;
        this.count = count;
        this.amount = amount;
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

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
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

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getAmount_type() {
        return amount_type;
    }

    public void setAmount_type(String amount_type) {
        this.amount_type = amount_type;
    }
}

