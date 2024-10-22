import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
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
}