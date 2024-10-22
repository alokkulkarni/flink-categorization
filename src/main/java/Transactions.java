import lombok.*;

import java.sql.Timestamp;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
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
}
