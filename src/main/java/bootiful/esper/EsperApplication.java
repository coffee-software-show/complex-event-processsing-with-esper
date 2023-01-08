package bootiful.esper;


import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.EPRuntimeProvider;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;

@Slf4j
@SpringBootApplication
public class EsperApplication {

    public static void main(String[] args) {
        SpringApplication.run(EsperApplication.class, args);
    }

    @Bean
    MessageChannel fraud() {
        return MessageChannels.queue().get();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void run() throws Exception {
        var compiler = EPCompilerProvider.getCompiler();
        var configuration = new Configuration();
        for (var eventType : new Class[]{CustomerCreatedEvent.class, WithdrawalEvent.class})
            configuration.getCommon().addEventType(eventType);
        var args = new CompilerArguments(configuration);
        var compiledEplExpression = compiler.compile("""
                    @name('people') select name, age from CustomerCreatedEvent ;
                    @name('people-over-50') select name, age from CustomerCreatedEvent (age > 50) ;
                    @name('withdrawals') select count(*) as c , sum(amount) as s from WithdrawalEvent#length(5);
                    
                    create context PartitionedByUsername partition by user  from WithdrawalEvent ;
                   
                    @name ('withdrawals-from-multiple-locations')  context PartitionedByUsername  select * from pattern [ 
                     every  (
                      a = WithdrawalEvent -> 
                      b = WithdrawalEvent ( user = a.user, location != a.location) where timer:within(5 minutes)
                     )
                    ]
                    
                                                                                                                    
                """, args);
        var runtime = EPRuntimeProvider.getDefaultRuntime(configuration);
        var deploymentService = runtime.getDeploymentService();
        var deployment = deploymentService.deploy(compiledEplExpression);
        for (var statement : "people,people-over-50".split(","))
            deploymentService.getStatement(deployment.getDeploymentId(), statement).addListener((newData, oldData, stmt, rt) -> {
                var name = (String) newData[0].get("name");
                var age = (int) newData[0].get("age");
                log.info("statement name: " + stmt.getName() + ": " + String.format("Name: %s, Age: %d%n", name, age));
            });
        //
        deploymentService.getStatement(deployment.getDeploymentId(), "withdrawals").addListener((newEvents, oldEvents, statement, runtime1) -> {
            var sum = (float) newEvents[0].get("s");
            var count = (long) newEvents[0].get("c");
            log.info("sum: " + sum + " count: " + count);
        });
        var messageChannel = fraud();
        deploymentService.getStatement(deployment.getDeploymentId(), "withdrawals-from-multiple-locations").addListener((newEvents, oldEvents, statement, runtime12) -> {
            var a = (WithdrawalEvent) newEvents[0].get("a");
            var b = (WithdrawalEvent) newEvents[0].get("b");
            messageChannel.send(MessageBuilder.withPayload(new FraudEvent(a, b)).build());
        });
        var eventService = runtime.getEventService();
        var eventTypeName = CustomerCreatedEvent.class.getSimpleName();
        eventService.sendEventBean(new CustomerCreatedEvent("Peter", 10), eventTypeName);
        eventService.sendEventBean(new CustomerCreatedEvent("Jane", 53), eventTypeName);
        for (var i = 0; i < 6; i++)
            eventService.sendEventBean(new WithdrawalEvent(i + 42.0f, "jlong", "sfo"), WithdrawalEvent.class.getSimpleName());

        eventService.sendEventBean(new WithdrawalEvent(1000, "jlong", "sfo"), WithdrawalEvent.class.getSimpleName());
        eventService.sendEventBean(new WithdrawalEvent(1000, "jlong", "pdx"), WithdrawalEvent.class.getSimpleName());
    }

    record FraudEvent(WithdrawalEvent a, WithdrawalEvent b) {
    }

    @ServiceActivator(inputChannel = "fraud")
    public void fraud(FraudEvent fraudEvent) {
        log.error("warning! fraud detected! " + fraudEvent);
    }
}

