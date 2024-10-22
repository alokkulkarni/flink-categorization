import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
public class transactionsPerCustomerPerCategoryPerMonth {
    private String customerId;
    private String accountNumber;
    private String category;
    private int year;
    private int month;
    private int count;
    private double amount;
    private String amount_type;
}

