package springbootelasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;


@Service
@Log4j2
public class PersonService {

    private RestHighLevelClient esRestClient;

    private ObjectMapper objectMapper;

    private static final String INDEX = "person";
    private static final String TYPE = "_doc";

    @Autowired
    public PersonService(RestHighLevelClient esRestClient, ObjectMapper objectMapper) {
        this.esRestClient = esRestClient;
        this.objectMapper = objectMapper;
    }

    public boolean createIndex(String indexName) {
        boolean result = false;

        try {
            CreateIndexRequest request = new CreateIndexRequest(indexName);
            CreateIndexResponse response = esRestClient.indices().create(request, RequestOptions.DEFAULT);
            result = response.isAcknowledged();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    public boolean createIndex(String indexName, int numOfShards, int numOfReplicas) {
        boolean result = false;

        try {
            CreateIndexRequest request = new CreateIndexRequest(indexName);
            request.settings(Settings.builder()
                    .put("index.number_of_shards", numOfShards)
                    .put("index.number_of_replicas", numOfReplicas)
            );

            CreateIndexResponse response = esRestClient.indices().create(request, RequestOptions.DEFAULT);
            result = response.isAcknowledged();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    public boolean deleteIndex(String indexName) {
        boolean result = false;

        try {
            DeleteIndexRequest request = new DeleteIndexRequest(indexName);
            AcknowledgedResponse response = esRestClient.indices().delete(request, RequestOptions.DEFAULT);
            result = response.isAcknowledged();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }


    public String savePerson(Person person) {
        String resp = null;
        try {
            IndexRequest indexRequest = new IndexRequest(INDEX, TYPE).id(person.getId()).source(convertPersonToMap(person), XContentType.JSON);
            IndexResponse indexResponse = esRestClient.index(indexRequest, RequestOptions.DEFAULT);
            resp = indexResponse.getResult().name();
            log.info("index:{} id:{}", indexResponse.getIndex(), indexResponse.getId());
            if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resp;
    }

    public Person findPersonById(String id) {
        Person person = null;
        try {
            GetRequest request = new GetRequest(INDEX, TYPE, id);
            GetResponse response = esRestClient.get(request, RequestOptions.DEFAULT);
            log.info("person with id:{} {}", id, response.isExists());
            if (response.isExists()) {
                person = convertMapToPerson(response.getSource());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return person;
    }

    public boolean deletePersonById(String id) {
        boolean resp = false;
        try {
            DeleteRequest deleteRequest = new DeleteRequest(INDEX, TYPE, id);
            DeleteResponse response = esRestClient.delete(deleteRequest,RequestOptions.DEFAULT);
            log.info("deleted with id:{} {}", id, response.getResult().name());
            resp = response.getResult() == DocWriteResponse.Result.DELETED ? true : false;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return resp;
    }

    public boolean updatePerson(Person person) {
        boolean resp = false;
        try {
            UpdateRequest updateRequest = new UpdateRequest(INDEX, TYPE, person.getId());
            updateRequest.doc(convertPersonToMap(person));
            UpdateResponse updateResponse = esRestClient.update(updateRequest, RequestOptions.DEFAULT);
            log.info("updated with id:{} {}", person.getId(), updateResponse.getResult().name());
            resp = updateResponse.getResult() == DocWriteResponse.Result.UPDATED ? true : false;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resp;
    }


    private Map<String, Object> convertPersonToMap(Person person) {
        return objectMapper.convertValue(person, Map.class);
    }

    private Person convertMapToPerson(Map<String, Object> map){
        return objectMapper.convertValue(map, Person.class);
    }


}
