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
public class Word {

    private String id;
    private String creationTime;
    private Info info;
    private List<Meaning> meaningList = new ArrayList<>();
    private List<Long> ownerIdList = new ArrayList<>();

}
