package bootiful;

import com.espertech.esper.runtime.client.EPEventService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
class BankClient {

	private final EPEventService eventService;

	public void withdraw(String username, float amount, String location) {
		var withdrawalEvent = new WithdrawalEvent(amount, username, location);
		this.eventService.sendEventBean(withdrawalEvent, WithdrawalEvent.class.getSimpleName());
	}

}
