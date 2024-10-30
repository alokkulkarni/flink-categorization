import lombok.*;

@Data
@NoArgsConstructor
public class transactionInsightsPerCustomerPerMerchantPerMonth {

    private String customer_id;
    private int year;
    private int month;
    private String transaction_type;
    private double transaction_amount;
    private String transaction_category;
    private int total_transactions;
    private String merchant;
    private double total_spend_per_Merchant;

    public transactionInsightsPerCustomerPerMerchantPerMonth(String customer_id, int year, int month, String transaction_type, double transaction_amount, String transaction_category, int total_transactions, String merchant, double total_spend_per_Merchant) {
        this.customer_id = customer_id;
        this.year = year;
        this.month = month;
        this.transaction_type = transaction_type;
        this.transaction_amount = transaction_amount;
        this.transaction_category = transaction_category;
        this.total_transactions = total_transactions;
        this.merchant = merchant;
        this.total_spend_per_Merchant = total_spend_per_Merchant;
    }

    public String getCustomer_id() {
        return customer_id;
    }

    public void setCustomer_id(String customer_id) {
        this.customer_id = customer_id;
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

    public String getTransaction_category() {
        return transaction_category;
    }

    public void setTransaction_category(String transaction_category) {
        this.transaction_category = transaction_category;
    }

    public int getTotal_transactions() {
        return total_transactions;
    }

    public void setTotal_transactions(int total_transactions) {
        this.total_transactions = total_transactions;
    }

    public String getMerchant() {
        return merchant;
    }

    public void setMerchant(String merchant) {
        this.merchant = merchant;
    }

    public double getTotal_spend_per_Merchant() {
        return total_spend_per_Merchant;
    }

    public void setTotal_spend_per_Merchant(double total_spend_per_Merchant) {
        this.total_spend_per_Merchant = total_spend_per_Merchant;
    }
}