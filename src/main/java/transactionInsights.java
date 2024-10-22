import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
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
}