package bootiful.esper;


import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.EPRuntimeProvider;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

@SpringBootApplication
public class EsperApplication {

    public static void main(String[] args) {
        SpringApplication.run(EsperApplication.class, args);
    }


    @EventListener(ApplicationReadyEvent.class)
    public void run() throws Exception {
        var compiler = EPCompilerProvider.getCompiler();
        var configuration = new Configuration();
        configuration.getCommon().addEventType(PersonEvent.class);
        var args = new CompilerArguments(configuration);
        var epCompiled = compiler.compile("@name('my-statement') select name, age from PersonEvent", args);
        var runtime = EPRuntimeProvider.getDefaultRuntime(configuration);
        var deployment = runtime.getDeploymentService().deploy(epCompiled);
        var statement = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "my-statement");
        statement.addListener((newData, oldData, stmt, rt) -> {
            String name = (String) newData[0].get("name");
            int age = (int) newData[0].get("age");
            System.out.println(String.format("Name: %s, Age: %d", name, age));
        });
        runtime.getEventService().sendEventBean(new PersonEvent("Peter", 10),
                PersonEvent .class.getSimpleName());


   /*     var runtime = EPRuntimeProvider.getDefaultRuntime();
        runtime.getEventService().sendEventBean(new PersonEvent("Bob",24),
                "PersonEvent");
*/

    }
}

