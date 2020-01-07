package springbootelasticsearch;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class Person {

    private String id;
    private String name;
    private Integer age;

}
