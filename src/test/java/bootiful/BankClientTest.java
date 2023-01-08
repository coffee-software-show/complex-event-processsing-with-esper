package bootiful;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

// todo change this to use Spring INtegration Test Utils to just poke at the MessageChannel itself instead of having this awkward counter
@SpringBootTest
class BankClientTest {

    private final BankClient client;
    private final EsperApplication application;

    @Autowired
    BankClientTest(EsperApplication application, BankClient client) {
        this.client = client;
        this.application = application;
    }

    @Test
    void commitFraud() throws Exception {
        this.client.withdraw("jlong", 100, "sfo");
        this.client.withdraw("jlong", 200, "pdx");
        Thread.sleep(100);
        Assertions.assertEquals(1, this.application.count.get());

    }
}