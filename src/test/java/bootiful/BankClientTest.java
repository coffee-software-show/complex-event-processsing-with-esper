package bootiful;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@SpringBootTest(classes = { EsperApplication.class, BankClientTest.NestedConfig.class })
class BankClientTest {

	@Configuration
	static class NestedConfig {

		final AtomicReference<FraudEvent> feRef = new AtomicReference<>();

		@EventListener
		public void onFraud(FraudEvent fe) {
			feRef.set(fe);
		}
	}

	@Test
	void commitFraud(@Autowired BankClient client, @Autowired NestedConfig nc) throws Exception {
		client.withdraw("jlong", 100, "sfo");
		client.withdraw("jlong", 200, "pdx");
		Thread.sleep(100);
		var event = nc.feRef.get();
		Assertions.assertNotNull(event, "the FraudEvent should not be null");
		for (var u : List.of(event.a(), event.b()))
			Assertions.assertEquals("jlong", u.getUser());
		Assertions.assertEquals(100, event.a().getAmount());
		Assertions.assertEquals(200, event.b().getAmount());
		Assertions.assertEquals("sfo", event.a().getLocation());
		Assertions.assertEquals("pdx", event.b().getLocation());
	}
}