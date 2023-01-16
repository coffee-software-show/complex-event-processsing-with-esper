package bootiful;

/*

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CustomerCreatedEvent {

	private String name;

	private int age;

}
*/

public record CustomerCreatedEvent(String name, int age) {
}