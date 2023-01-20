package bootiful;

import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.runtime.client.EPDeploymentService;
import com.espertech.esper.runtime.client.EPEventService;
import com.espertech.esper.runtime.client.UpdateListener;
import com.joshlong.esper.autoconfiguration.EsperConfigurationCustomizer;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Map;

@SpringBootApplication
public class EsperApplication {

    public static void main(String[] args) {
        SpringApplication.run(EsperApplication.class, args);
    }

    @Bean
    EsperConfigurationCustomizer esperConfigurationCustomizer() {
        return configuration -> {
            configuration.getCommon().addEventType(WithdrawalEvent.class.getSimpleName(),
                    Map.of("user", String.class, "location", String.class, "amount", Float.class));
            configuration.getCommon().addEventType(CustomerCreatedEvent.class.getSimpleName(),
                    Map.of("name", String.class, "age", Integer.class));
        };
    }
}


record FraudEvent(WithdrawalEvent a, WithdrawalEvent b) {
}

record CustomerCreatedEvent(String name, int age) {
}

record WithdrawalEvent(String user, String location, float amount) {
}


@Component
class BankClient {

    private final EPEventService eventService;

    BankClient(EPEventService eventService) {
        this.eventService = eventService;
    }

    void createCustomer(String name, int age) {
        this.eventService.sendEventMap(Map.of("name", name, "age", age), CustomerCreatedEvent.class.getSimpleName());
    }

    void withdraw(String user, String location, float amount) {
        this.eventService.sendEventMap(Map.of("user", user, "location", location, "amount", amount),
                WithdrawalEvent.class.getSimpleName());
    }
}


@Component
@RequiredArgsConstructor
class Demo implements ApplicationRunner {

    private final com.espertech.esper.common.client.configuration.Configuration esperConfiguration;
    private final EPCompiler epCompiler;
    private final EPDeploymentService epDeploymentService;
    private final BankClient client;
    private final UpdateListener updateListener = (newEvents, oldEvents, statement, runtime) -> {
        for (var ne : newEvents)
            System.out.println("\tnewEvent " + ne.getUnderlying());
    };

    @Override
    public void run(ApplicationArguments args) throws Exception {


//        register("@name('all') select * from CustomerCreatedEvent", "all", this.updateListener);
//        register("@name('all-qualified') select * from CustomerCreatedEvent(age >=40) ", "all-qualified", this.updateListener);
        register("""
                create context PartitionedByUser partition by user  from WithdrawalEvent ;
                        
                @name('withdrawals-from-multiple-locations')
                context PartitionedByUser select * from pattern [
                    every  (
                        a = WithdrawalEvent ->
                        b = WithdrawalEvent ( user = a.user, location != a.location) where timer:within(5 minutes)
                    )
                ]
                    """, "withdrawals-from-multiple-locations", (newEvents, oldEvents, statement, runtime) -> {
            var aMap = (Map<String, Object>) newEvents[0].get("a");
            var bMap = (Map<String, Object>) newEvents[0].get("b");
            var aWE = new WithdrawalEvent((String) aMap.get("user"), (String) aMap.get("location"), (float) aMap.get("amount"));
            var bWE = new WithdrawalEvent((String) bMap.get("user"), (String) bMap.get("location"), (float) bMap.get("amount"));
            var fe = new FraudEvent(aWE, bWE);
            System.out.println("fraud! " + fe);
            // messageChannel.send(MessageBuilder.withPayload(fe).build());
            // applicationEventPublisher.publish(fe);
        });


        Map.of("tjernigan", 30, "jlong", 38, "zsu", 30, "zz", 30, "tkoon", 38, "dsyer", 58, "dlew", 38, "ncorlett", 52)
                .forEach(client::createCustomer);
        client.withdraw("jlong", "sfo", 100);
        client.withdraw("jlong", "cmn", 10);
    }

    private void register(String query, String statementName, UpdateListener listener) throws Exception {
        var compiled = this.epCompiler.compile(query, new CompilerArguments(this.esperConfiguration));
        var deployment = this.epDeploymentService.deploy(compiled);
        epDeploymentService.getStatement(deployment.getDeploymentId(), statementName)
                .addListener((newEvents, oldEvents, statement, runtime) -> {
                    System.out.println("statement [" + statementName + "] results ");
                    listener.update(newEvents, oldEvents, statement, runtime);
                });
    }
}
