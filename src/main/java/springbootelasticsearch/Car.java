package springbootelasticsearch;


import lombok.*;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class Car {

    private String id;
    private String brand;
    private String model;
    private String year;
    private Integer price;
    private String category;
    private String description;
    private List<String> features = new ArrayList();
}
