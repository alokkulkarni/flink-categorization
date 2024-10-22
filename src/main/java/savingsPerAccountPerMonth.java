import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
public class savingsPerAccountPerMonth {
    
    private String customerId;
    private String accountNumber;
    private int year;
    private int month;
    private double savings_amount;
    private String amount_type;
}
