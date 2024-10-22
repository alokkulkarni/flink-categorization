import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
public class totalSpendAcrossMerchants {

    private String merchant;
    private double totalSpend;
    private int count;
    private int year;
    private int month;
}
