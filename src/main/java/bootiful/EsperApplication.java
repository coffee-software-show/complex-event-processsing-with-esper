package bootiful;

import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;
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
	EPCompiler compiler() {
		return EPCompilerProvider.getCompiler();
	}

	@Bean
	Configuration configuration() {
		var c = new Configuration();
		List.of(CustomerCreatedEvent.class, WithdrawalEvent.class)
				.forEach(eventType -> c.getCommon().addEventType(eventType));
		return c;
	}

	@Bean
	EPRuntime runtime(Configuration configuration) {
		return EPRuntimeProvider.getDefaultRuntime(configuration);
	}

	@Bean
	EPDeploymentService deploymentService(EPRuntime runtime) {
		return runtime.getDeploymentService();
	}

	@Bean
	EPDeployment deployment(Configuration configuration, EPCompiler compiler, EPDeploymentService deploymentService)
			throws Exception {
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
		var compiledEplExpression = compiler.compile(epl, new CompilerArguments(configuration));
		return deploymentService.deploy(compiledEplExpression);
	}

	@Bean
	EPEventService eventService(EPRuntime runtime) {
		return runtime.getEventService();
	}

	@Bean
	InitializingBean listenerConnectingInitializingBean(EPDeploymentService ds, EPDeployment deployment,
			ApplicationEventPublisher publisher) {
		return () -> ds //
				.getStatement(deployment.getDeploymentId(), "withdrawals-from-multiple-locations")
				.addListener((newEvents, oldEvents, statement, runtime) -> {
					var a = (WithdrawalEvent) newEvents[0].get("a");
					var b = (WithdrawalEvent) newEvents[0].get("b");
					publisher.publishEvent(new FraudEvent(a, b));
				});
	}

}
