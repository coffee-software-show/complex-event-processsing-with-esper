package bootiful;

import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.runtime.client.EPDeploymentService;
import com.espertech.esper.runtime.client.EPEventService;
import com.joshlong.esper.autoconfiguration.EsperConfigurationCustomizer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@SpringBootApplication
public class EsperApplication {

	public static void main(String[] args) {
		SpringApplication.run(EsperApplication.class, args);
	}

	@Bean
	EsperConfigurationCustomizer esperConfigurationCustomizer() {
		return configuration -> List.of(CustomerCreatedEvent.class, WithdrawalEvent.class)
				.forEach(eventType -> configuration.getCommon().addEventType(eventType));
	}

	@Bean
	InitializingBean esperDeploymentsIntialization(
			com.espertech.esper.common.client.configuration.Configuration configuration, EPCompiler compiler,
			EPDeploymentService deploymentService, ApplicationEventPublisher publisher) {
		return () -> {

			var epl = """

					@name('people')
					select name, age from CustomerCreatedEvent ;

					@name('people-over-50')
					select name, age from CustomerCreatedEvent (age > 50) ;

					@name('withdrawals')
					select count(*) as c , sum(amount) as s from WithdrawalEvent#length(5);

					create context PartitionedByUser partition by user  from WithdrawalEvent ;

					@name('withdrawals-from-multiple-locations')
					context PartitionedByUser select * from pattern [
					 every  (
					  a = WithdrawalEvent ->
					  b = WithdrawalEvent ( user = a.user, location != a.location) where timer:within(5 minutes)
					 )
					]
					""";
			var deployment = deploymentService.deploy(compiler.compile(epl, new CompilerArguments(configuration)));
			deploymentService //
					.getStatement(deployment.getDeploymentId(), "withdrawals-from-multiple-locations")
					.addListener((newEvents, oldEvents, statement, runtime) -> {
						var a = (WithdrawalEvent) newEvents[0].get("a");
						var b = (WithdrawalEvent) newEvents[0].get("b");
						publisher.publishEvent(new FraudEvent(a, b));
					});
		};
	}

}

@Component
@RequiredArgsConstructor
class BankClient {

	private final EPEventService eventService;

	public void withdraw(String username, float amount, String location) {
		var withdrawalEvent = new WithdrawalEvent(amount, username, location);
		this.eventService.sendEventBean(withdrawalEvent, WithdrawalEvent.class.getSimpleName());
	}

}
