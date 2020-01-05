package springbootelasticsearch;

import lombok.extern.log4j.Log4j2;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
@Log4j2
public class ElasticSearchService {

    private RestHighLevelClient esRestClient;

    @Autowired
    public ElasticSearchService(RestHighLevelClient esRestClient) {
        this.esRestClient = esRestClient;
    }

    public boolean createIndex(String indexName) {
        boolean created = false;

        CreateIndexRequest request = new CreateIndexRequest(indexName);
        try {
            CreateIndexResponse response = esRestClient.indices().create(request, RequestOptions.DEFAULT);
            created = response.isAcknowledged();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return created;
    }

}
