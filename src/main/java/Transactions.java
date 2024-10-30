import lombok.*;

import java.sql.Timestamp;

@Data
@NoArgsConstructor
public class Transactions {
    private String transaction_id;
    private Timestamp transaction_time;
    private String transaction_category;
    private String transaction_description;
    private double transaction_amount;
    private String transaction_type;
    private String transaction_currency;
    private String location;
    private String merchant;
    private String customerId;
    private String accountNumber;

    public Transactions(String transaction_id, Timestamp transaction_time, String transaction_category, String transaction_description, double transaction_amount, String transaction_type, String transaction_currency, String location, String merchant, String customerId, String accountNumber) {
        this.transaction_id = transaction_id;
        this.transaction_time = transaction_time;
        this.transaction_category = transaction_category;
        this.transaction_description = transaction_description;
        this.transaction_amount = transaction_amount;
        this.transaction_type = transaction_type;
        this.transaction_currency = transaction_currency;
        this.location = location;
        this.merchant = merchant;
        this.customerId = customerId;
        this.accountNumber = accountNumber;
    }

    public String getTransaction_id() {
        return transaction_id;
    }

    public void setTransaction_id(String transaction_id) {
        this.transaction_id = transaction_id;
    }

    public Timestamp getTransaction_time() {
        return transaction_time;
    }

    public void setTransaction_time(Timestamp transaction_time) {
        this.transaction_time = transaction_time;
    }

    public String getTransaction_category() {
        return transaction_category;
    }

    public void setTransaction_category(String transaction_category) {
        this.transaction_category = transaction_category;
    }

    public String getTransaction_description() {
        return transaction_description;
    }

    public void setTransaction_description(String transaction_description) {
        this.transaction_description = transaction_description;
    }

    public double getTransaction_amount() {
        return transaction_amount;
    }

    public void setTransaction_amount(double transaction_amount) {
        this.transaction_amount = transaction_amount;
    }

    public String getTransaction_type() {
        return transaction_type;
    }

    public void setTransaction_type(String transaction_type) {
        this.transaction_type = transaction_type;
    }

    public String getTransaction_currency() {
        return transaction_currency;
    }

    public void setTransaction_currency(String transaction_currency) {
        this.transaction_currency = transaction_currency;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getMerchant() {
        return merchant;
    }

    public void setMerchant(String merchant) {
        this.merchant = merchant;
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
}
