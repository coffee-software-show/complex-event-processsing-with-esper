package bootiful;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.runtime.client.EPDeploymentService;
import com.espertech.esper.runtime.client.EPEventService;
import com.espertech.esper.runtime.client.UpdateListener;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.joshlong.esper.autoconfiguration.EsperConfigurationCustomizer;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Slf4j
@SpringBootApplication
public class EsperApplication {

	public static void main(String[] args) {
		SpringApplication.run(EsperApplication.class, args);
	}

	@Bean
	EsperConfigurationCustomizer esperConfigurationCustomizer() {
		return configuration -> List.of(CustomerCreatedEvent.class, WithdrawalEvent.class)
				.forEach(eventClass -> configuration.getCommon().addEventType(eventClass.getSimpleName(),
						Utils.typeMapFor(eventClass)));
	}

	@Bean
	InitializingBean deploy(com.espertech.esper.common.client.configuration.Configuration configuration,
			EPCompiler compiler, EPDeploymentService deploymentService, ApplicationEventPublisher publisher) {
		return () -> {
			var updateListener = (UpdateListener) (newEvents, oldEvents, statement, runtime) -> Utils.log(newEvents);
			register(configuration, compiler, deploymentService,
					" @name('people') select name, age from CustomerCreatedEvent", "people", updateListener);
			register(configuration, compiler, deploymentService,
					" @name('people-over-40')  select name, age from CustomerCreatedEvent (age >= 40)  ",
					"people-over-40", updateListener);
			register(configuration, compiler, deploymentService,
					" @name('withdrawals') select count(*) as c , sum(amount) as s from WithdrawalEvent#length(5) ",
					"withdrawals", updateListener);
			register(configuration, compiler, deploymentService, """

					    create context PartitionedByUser partition by user  from WithdrawalEvent ;

					    @name('withdrawals-from-multiple-locations')
					    context PartitionedByUser select * from pattern [
					     every  (
					      a = WithdrawalEvent ->
					      b = WithdrawalEvent ( user = a.user, location != a.location) where timer:within(5 minutes)
					     )
					    ]

					""", "withdrawals-from-multiple-locations", (newEvents, oldEvents, statement, runtime) -> {
				var a = withdrawalEventFromMap((Map<String, Object>) newEvents[0].get("a"));
				var b = withdrawalEventFromMap((Map<String, Object>) newEvents[0].get("b"));
				Utils.log(newEvents);
				var fe = new FraudEvent(a, b);
				publisher.publishEvent(fe);
			});
		};
	}

	@Bean
	ApplicationListener<ApplicationReadyEvent> ready(BankClient bank) {
		return event -> {
			Map.of("Zhouyue", 30, "Zhen", 30, "Josh", 38, "Dave", 50, "Stéphane", 40, "Leon", 30, "Reon", 39)
					.forEach(bank::createCustomer);
			bank.withdraw("jlong", 100.0, "sfo");
			bank.withdraw("jlong", 10.0, "cmn");
		};
	}

	private static WithdrawalEvent withdrawalEventFromMap(Map<String, Object> mapEventBean) {
		return new WithdrawalEvent((Double) mapEventBean.get("amount"), (String) mapEventBean.get("user"),
				(String) mapEventBean.get("location"));
	}

	private static void register(Configuration configuration, EPCompiler compiler,
			EPDeploymentService deploymentService, String epl, String name, UpdateListener updateListener)
			throws Exception {
		var deployment = deploymentService.deploy(compiler.compile(epl, new CompilerArguments(configuration)));
		var epStatement = deploymentService.getStatement(deployment.getDeploymentId(), name);
		epStatement.addListener((newEvents, oldEvents, statement, runtime) -> {
			System.out.println("running " + name);
			updateListener.update(newEvents, oldEvents, statement, runtime);
		});
	}

}

@Component
@RequiredArgsConstructor
class BankClient {

	private final EPEventService eventService;

	private final ObjectMapper objectMapper;

	public void createCustomer(String name, int age) {
		var record = Utils.mapFromRecord(this.objectMapper, new CustomerCreatedEvent(name, age));
		this.eventService.sendEventMap(record, CustomerCreatedEvent.class.getSimpleName());
	}

	public void withdraw(String username, double amount, String location) {
		var withdrawalEvent = Utils.mapFromRecord(this.objectMapper, new WithdrawalEvent(amount, username, location));
		this.eventService.sendEventMap(withdrawalEvent, WithdrawalEvent.class.getSimpleName());
	}

}

abstract class Utils {

	static Map<String, Object> typeMapFor(Class<? extends Record> recordClass) {
		var map = new HashMap<String, Object>();
		Arrays.stream(recordClass.getRecordComponents()).forEach(rc -> map.put(rc.getName(), rc.getType()));
		return map;
	}

	static void log(EventBean[] events) {
		Stream.of(events).forEach(eb -> System.out.println(eb.getUnderlying()));
	}

	@SneakyThrows
	static Map<String, Object> mapFromRecord(ObjectMapper objectMapper, Object o) {
		var json = objectMapper.writeValueAsString(o);
		return objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {
		});
	}

}