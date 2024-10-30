import lombok.*;

@Data
@NoArgsConstructor
public class transactionInsights {

    String customerId;
    String accountNumber;
    int year;
    int month;
    String transaction_type;
    double transaction_amount;
    int total_transactions;
    double total_savings;
    String total_savings_type;
    double total_expenses;
    double total_income;
    double total_savings_percentage;
    double total_expenses_percentage;

    public transactionInsights(String customerId, String accountNumber, int year, int month, String transaction_type, double transaction_amount, int total_transactions, double total_savings, String total_savings_type, double total_expenses, double total_income, double total_savings_percentage, double total_expenses_percentage) {
        this.customerId = customerId;
        this.accountNumber = accountNumber;
        this.year = year;
        this.month = month;
        this.transaction_type = transaction_type;
        this.transaction_amount = transaction_amount;
        this.total_transactions = total_transactions;
        this.total_savings = total_savings;
        this.total_savings_type = total_savings_type;
        this.total_expenses = total_expenses;
        this.total_income = total_income;
        this.total_savings_percentage = total_savings_percentage;
        this.total_expenses_percentage = total_expenses_percentage;
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

    public String getTransaction_type() {
        return transaction_type;
    }

    public void setTransaction_type(String transaction_type) {
        this.transaction_type = transaction_type;
    }

    public double getTransaction_amount() {
        return transaction_amount;
    }

    public void setTransaction_amount(double transaction_amount) {
        this.transaction_amount = transaction_amount;
    }

    public int getTotal_transactions() {
        return total_transactions;
    }

    public void setTotal_transactions(int total_transactions) {
        this.total_transactions = total_transactions;
    }

    public double getTotal_savings() {
        return total_savings;
    }

    public void setTotal_savings(double total_savings) {
        this.total_savings = total_savings;
    }

    public String getTotal_savings_type() {
        return total_savings_type;
    }

    public void setTotal_savings_type(String total_savings_type) {
        this.total_savings_type = total_savings_type;
    }

    public double getTotal_expenses() {
        return total_expenses;
    }

    public void setTotal_expenses(double total_expenses) {
        this.total_expenses = total_expenses;
    }

    public double getTotal_income() {
        return total_income;
    }

    public void setTotal_income(double total_income) {
        this.total_income = total_income;
    }

    public double getTotal_savings_percentage() {
        return total_savings_percentage;
    }

    public void setTotal_savings_percentage(double total_savings_percentage) {
        this.total_savings_percentage = total_savings_percentage;
    }

    public double getTotal_expenses_percentage() {
        return total_expenses_percentage;
    }

    public void setTotal_expenses_percentage(double total_expenses_percentage) {
        this.total_expenses_percentage = total_expenses_percentage;
    }
}