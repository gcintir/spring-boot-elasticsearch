package springbootelasticsearch;

import lombok.extern.log4j.Log4j2;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
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
        boolean result = false;

        try {
            CreateIndexRequest request = new CreateIndexRequest(indexName);
            CreateIndexResponse response = esRestClient.indices().create(request, RequestOptions.DEFAULT);
            result = response.isAcknowledged();
        } catch (IOException e) {
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
        } catch (IOException e) {
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
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }




}
