package springbootelasticsearch;


import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Service
@Log4j2
public class WordService {

    private RestHighLevelClient esRestClient;

    private ObjectMapper objectMapper;

    private static final String INDEX = "word";
    private static final String TYPE = "_doc";
    private static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Autowired
    public WordService(RestHighLevelClient esRestClient, ObjectMapper objectMapper) {
        this.esRestClient = esRestClient;
        this.objectMapper = objectMapper;
    }

    public boolean createIndex() {
        boolean result = false;

        try {
            CreateIndexRequest request = new CreateIndexRequest(INDEX);
            request.settings(Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1));

            Map<String, Object> mapping = new HashMap<>();
            Map<String, Object> properties = new HashMap<>();

            Map<String, Object> text = new HashMap<>();text.put("type", "text");
            Map<String, Object> long_ = new HashMap<>();long_.put("type", "long");


            Map<String, Object> creationTime = new HashMap<>();
            creationTime.put("type", "date");
            creationTime.put("format", "yyyy-MM-dd HH:mm:ss");

            Map<String, Object> info = new HashMap<>();
            Map<String, Object> infoProperties = new HashMap<>();
            infoProperties.put("createdBy", text);
            info.put("properties", infoProperties);

            Map<String, Object> ownerIdProperties = new HashMap<>();
            ownerIdProperties.put("type", "long");
            Map<String, Object> ownerIdList = new HashMap<>();
            ownerIdList.put("properties", long_);

            Map<String, Object> meaningList = new HashMap<>();
            Map<String, Object> meaningProperties = new HashMap<>();
            meaningProperties.put("description", text);
            meaningProperties.put("meaningType", text);
            meaningList.put("properties", meaningProperties);

            properties.put("id", text);
            properties.put("creationTime", creationTime);
            properties.put("info", info);
            properties.put("ownerIdList", long_);
            properties.put("meaningList", meaningList);

            mapping.put("properties", properties);
            request.mapping("mapping", mapping);


            CreateIndexResponse response = esRestClient.indices().create(request, RequestOptions.DEFAULT);
            result = response.isAcknowledged();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    public void saveWord() {

        try {
            Word word = new Word();
            word.setCreationTime(dateFormatter.format(new Date()));
            word.setId("w1");

            Info info = new Info();
            info.setCreatedBy("gcintir");

            word.setInfo(info);

            Meaning meaning = new Meaning();
            meaning.setDescription("m1");
            meaning.setMeaningType("verb");

            word.getMeaningList().add(meaning);

            word.getOwnerIdList().add(1l);
            word.getOwnerIdList().add(2l);

            IndexRequest indexRequest = new IndexRequest(INDEX, TYPE).id(word.getId()).source(convertWordToMap(word), XContentType.JSON);
            IndexResponse indexResponse = esRestClient.index(indexRequest, RequestOptions.DEFAULT);
            String resp = indexResponse.getResult().name();
            log.info("index:{} id:{}", indexResponse.getIndex(), indexResponse.getId());
            if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {

            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    private Map<String, Object> convertWordToMap(Word word) {
        return objectMapper.convertValue(word, Map.class);
    }

    private Word convertMapToWord(Map<String, Object> map){
        return objectMapper.convertValue(map, Word.class);
    }
}
