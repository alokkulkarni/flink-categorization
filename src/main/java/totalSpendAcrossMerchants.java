import lombok.*;

@Data
@NoArgsConstructor
public class totalSpendAcrossMerchants {

    private String merchant;
    private double totalSpend;
    private int count;
    private int year;
    private int month;


    public totalSpendAcrossMerchants(String merchant, double totalSpend, int count, int year, int month) {
        this.merchant = merchant;
        this.totalSpend = totalSpend;
        this.count = count;
        this.year = year;
        this.month = month;
    }

    public String getMerchant() {
        return merchant;
    }

    public void setMerchant(String merchant) {
        this.merchant = merchant;
    }

    public double getTotalSpend() {
        return totalSpend;
    }

    public void setTotalSpend(double totalSpend) {
        this.totalSpend = totalSpend;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
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
}
