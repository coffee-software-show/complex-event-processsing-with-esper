package bootiful;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/*
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WithdrawalEvent {
	private float amount;
	private String user;
	private String location;
}

*/

public record WithdrawalEvent(double amount, String user, String location) {
}