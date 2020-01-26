package springbootelasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
@Log4j2
public class CarService {

    private RestHighLevelClient esRestClient;

    private ObjectMapper objectMapper;

    private static final String INDEX = "car";
    private static final String TYPE = "_doc";

    private static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * car has following properties
     * - brand (string)
     * - model (string)
     * - year (date)
     * - price (integer)
     * - category (string num)
     * - description (string)
     * - features: [] (string array)
     *
     */

    /**
     * {
     *   "car" : {
     *     "mappings" : {
     *       "properties" : {
     *         "brand" : {
     *           "type" : "text"
     *         },
     *         "category" : {
     *           "type" : "keyword"
     *         },
     *         "description" : {
     *           "type" : "text"
     *         },
     *         "features" : {
     *           "type" : "text"
     *         },
     *         "model" : {
     *           "type" : "text"
     *         },
     *         "price" : {
     *           "type" : "integer"
     *         },
     *         "year" : {
     *           "type" : "date",
     *           "format" : "yyyy-MM-dd HH:mm:ss"
     *         }
     *       }
     *     }
     *   }
     * }
     *
     */

    @Autowired
    public CarService(RestHighLevelClient esRestClient, ObjectMapper objectMapper) {
        this.esRestClient = esRestClient;
        this.objectMapper = objectMapper;
    }

    public boolean createIndexWithExplicitMapping() {
        boolean result = false;
        try {

            Map<String, Object> text_ = new HashMap<>();text_.put("type", "text");
            Map<String, Object> keyword_ = new HashMap<>();keyword_.put("type", "keyword");
            Map<String, Object> long_ = new HashMap<>();long_.put("type", "long");
            Map<String, Object> int_ = new HashMap<>();int_.put("type", "integer");
            Map<String, Object> date_ = new HashMap<>();date_.put("type", "date");date_.put("format", "yyyy-MM-dd HH:mm:ss");

            CreateIndexRequest request = new CreateIndexRequest(INDEX);
            request.settings(Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1));

            Map<String, Object> mapping = new HashMap<>();
            Map<String, Object> properties = new HashMap<>();

            properties.put("brand", text_);
            properties.put("model", text_);
            properties.put("year", date_);
            properties.put("price", int_);
            properties.put("category", keyword_);
            properties.put("description", text_);
            properties.put("features", text_);

            mapping.put("properties", properties);

            request.mapping("mapping", mapping);

            CreateIndexResponse response = esRestClient.indices().create(request, RequestOptions.DEFAULT);
            result = response.isAcknowledged();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return result;
    }

    public void saveBulkData () {
        try {
            Car car1 = new Car();
            car1.setBrand("opel"); car1.setModel("astra");
            car1.setCategory("normal"); car1.setPrice(65000);
            car1.setDescription("fuel, automatic gear, clean");
            car1.getFeatures().add("automatic");
            car1.getFeatures().add("fuel");
            car1.setYear(dateFormatter.format(new Date()));

            Car car2 = new Car();
            car2.setBrand("mercedes"); car2.setModel("C180");
            car2.setCategory("sport"); car2.setPrice(120000);
            car2.setDescription("fuel, automatic gear, clean, sport");
            car2.getFeatures().add("automatic");
            car2.getFeatures().add("fuel");
            car2.setYear(dateFormatter.format(new Date()));

            Car car3 = new Car();
            car3.setBrand("range rover"); car3.setModel("vogue");
            car3.setCategory("jeep"); car3.setPrice(300000);
            car3.setDescription("diesel, automatic gear, clean, jeep");
            car3.getFeatures().add("automatic");
            car3.getFeatures().add("diesel");
            car3.setYear(dateFormatter.format(new Date()));

            Car car4 = new Car();
            car4.setBrand("skoda"); car4.setModel("superb");
            car4.setCategory("normal"); car4.setPrice(150000);
            car4.setDescription("diesel, manual gear");
            car4.getFeatures().add("manual");
            car4.getFeatures().add("diesel");
            car4.setYear(dateFormatter.format(new Date()));

            IndexRequest indexRequest = new IndexRequest(INDEX, TYPE).source(convertCarToMap(car4), XContentType.JSON);
            IndexResponse indexResponse = esRestClient.index(indexRequest, RequestOptions.DEFAULT);
            String resp = indexResponse.getResult().name();
            log.info("index:{} id:{} result:{}", indexResponse.getIndex(), indexResponse.getId(), indexResponse.getResult());
            if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                car1.setId(indexResponse.getId());
            }


        }catch (Exception ex) {
            ex.printStackTrace();
        }
    }


    public void searchData1() {
        try {
            SearchRequest searchRequest = new SearchRequest(INDEX);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = esRestClient.search(searchRequest, RequestOptions.DEFAULT);
            displaySearchOperationResults(searchResponse);
            displaySearchHits(searchResponse.getHits());

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void searchNormalCarsWithPagination() {
        try {
            SearchRequest searchRequest = new SearchRequest(INDEX);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

            searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
            searchSourceBuilder.query(QueryBuilders.termQuery("category", "normal"));
            searchSourceBuilder.from(0);
            searchSourceBuilder.size(1);


            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = esRestClient.search(searchRequest, RequestOptions.DEFAULT);
            displaySearchOperationResults(searchResponse);
            displaySearchHits(searchResponse.getHits());

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void getCarsHaving_10_000_price_By_Pagination() {
        try {

            int total = 0;
            int from = 0;
            int page_size = 300;
            SearchRequest searchRequest = new SearchRequest(INDEX);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
            searchSourceBuilder.query(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("price", 10_000)));
            searchSourceBuilder.from(from);
            searchSourceBuilder.size(page_size);

            searchRequest.source(searchSourceBuilder);
            SearchResponse searchResponse = esRestClient.search(searchRequest, RequestOptions.DEFAULT);

            log.info("from:{}", from);
            SearchHit[] searchHits = searchResponse.getHits().getHits();
            log.info("number of hits:{}", searchHits.length);

            while(searchHits != null && searchHits.length == page_size) {
                from += searchHits.length;
                total += searchHits.length;
                searchSourceBuilder.from(from);
                log.info("from:{}", from);
                searchResponse = esRestClient.search(searchRequest, RequestOptions.DEFAULT);
                searchHits = searchResponse.getHits().getHits();
                log.info("number of hits:{}", searchHits.length);
                Boolean terminatedEarly = searchResponse.isTerminatedEarly();
                log.info("terminated:{} status:{}", terminatedEarly, searchResponse.status());

            }
            total += searchHits.length;

            Boolean terminatedEarly = searchResponse.isTerminatedEarly();
            log.info("terminated:{} status:{} total:{}", terminatedEarly, searchResponse.status(), total);

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void searchDieselCars() {
        try {
            SearchRequest searchRequest = new SearchRequest(INDEX);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

            searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
            searchSourceBuilder.query(QueryBuilders.matchQuery("description", "Diesel"));
            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = esRestClient.search(searchRequest, RequestOptions.DEFAULT);
            displaySearchOperationResults(searchResponse);
            displaySearchHits(searchResponse.getHits());

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void searchDieselCarsByTerm() {
        try {
            SearchRequest searchRequest = new SearchRequest(INDEX);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

            searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
            searchSourceBuilder.query(QueryBuilders.termQuery("description", "diesel"));
            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = esRestClient.search(searchRequest, RequestOptions.DEFAULT);
            displaySearchOperationResults(searchResponse);
            displaySearchHits(searchResponse.getHits());

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void searchDieselCarsByBool() {
        try {
            SearchRequest searchRequest = new SearchRequest(INDEX);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

            searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
            searchSourceBuilder.query(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("description", "diesel")));
            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = esRestClient.search(searchRequest, RequestOptions.DEFAULT);
            displaySearchOperationResults(searchResponse);
            displaySearchHits(searchResponse.getHits());

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void searchDieselCarsByBool2() {
        try {
            SearchRequest searchRequest = new SearchRequest(INDEX);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

            searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
            searchSourceBuilder.query(QueryBuilders.boolQuery().filter(QueryBuilders.matchQuery("description", "Diesel")));
            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = esRestClient.search(searchRequest, RequestOptions.DEFAULT);
            displaySearchOperationResults(searchResponse);
            displaySearchHits(searchResponse.getHits());

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void searchJeepOrSportCars() {
        try {
            SearchRequest searchRequest = new SearchRequest(INDEX);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

            searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
            //searchSourceBuilder.query(QueryBuilders.queryStringQuery("category=jeep OR category=sport"));
            searchSourceBuilder.query(QueryBuilders.termsQuery("category", "jeep", "sport"));
            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = esRestClient.search(searchRequest, RequestOptions.DEFAULT);
            displaySearchOperationResults(searchResponse);
            displaySearchHits(searchResponse.getHits());

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void searchCarsPriceBetween_50000_150000() {
        try {
            SearchRequest searchRequest = new SearchRequest(INDEX);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

            searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
            searchSourceBuilder.query(QueryBuilders.rangeQuery("price").gte(50_000).lte(150_000));
            searchSourceBuilder.sort(new FieldSortBuilder("price").order(SortOrder.ASC));

            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = esRestClient.search(searchRequest, RequestOptions.DEFAULT);
            displaySearchOperationResults(searchResponse);
            displaySearchHits(searchResponse.getHits());

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void getBrands() {
        try {
            SearchRequest searchRequest = new SearchRequest(INDEX);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

            searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());

            String[] includeFields = new String[] {"brand"};
            searchSourceBuilder.fetchSource(includeFields, null);


            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = esRestClient.search(searchRequest, RequestOptions.DEFAULT);
            displaySearchOperationResults(searchResponse);
            displaySearchHits(searchResponse.getHits());

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void getAggregationByCategory() {
        try {
            SearchRequest searchRequest = new SearchRequest(INDEX);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

            searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

            TermsAggregationBuilder aggr = AggregationBuilders.terms("by_category").field("category");

            searchSourceBuilder.aggregation(aggr);

            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = esRestClient.search(searchRequest, RequestOptions.DEFAULT);
            displaySearchOperationResults(searchResponse);
            displaySearchHits(searchResponse.getHits());

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }


    public void getCountByCountApi() {
        try {
            CountRequest countRequest = new CountRequest();
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("price", 10_000)));
            countRequest.source(searchSourceBuilder);
            CountResponse countResponse = esRestClient.count(countRequest, RequestOptions.DEFAULT);
            long count = countResponse.getCount();
            log.info("count:{}", count);

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }


    public void searchWithScrollApi() {
        try {
            SearchRequest searchRequest = new SearchRequest(INDEX);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("price", 10_000)));
            searchSourceBuilder.size(10);

            searchRequest.source(searchSourceBuilder);
            searchRequest.scroll(TimeValue.timeValueSeconds(60));

            SearchResponse searchResponse = esRestClient.search(searchRequest, RequestOptions.DEFAULT);
            String scrollId = searchResponse.getScrollId();
            log.info("scrollId:{}", scrollId);

            SearchHit[] searchHits = searchResponse.getHits().getHits();
            log.info("number of hits:{}", searchHits.length);
            for (SearchHit hit : searchHits) {
                log.info("sourceAsString:{}", hit.getSourceAsString());
            }

            while(searchHits != null && searchHits.length > 0) {
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(TimeValue.timeValueSeconds(60));
                searchResponse = esRestClient.scroll(scrollRequest, RequestOptions.DEFAULT);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();

                log.info("scrollId:{}", scrollId);
                searchHits = searchResponse.getHits().getHits();
                log.info("number of hits:{}", searchHits.length);
                for (SearchHit hit : searchHits) {
                    log.info("sourceAsString:{}", hit.getSourceAsString());
                }

            }

            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            ClearScrollResponse clearScrollResponse = esRestClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
            boolean succeeded = clearScrollResponse.isSucceeded();
            log.info("clear scrool {}", succeeded);

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void displayAggregations(SearchResponse searchResponse) {

        Aggregations aggregations = searchResponse.getAggregations();
        Terms byCategoryAggregation = aggregations.get("by_category");
        Terms.Bucket elasticBucket = byCategoryAggregation.getBucketByKey("Elastic");
        long count = elasticBucket.getDocCount();
        log.info("count:{}", count);

    }


    private void displaySearchHits(SearchHits hits) {
        TotalHits totalHits = hits.getTotalHits();

        long numHits = totalHits.value;
        TotalHits.Relation relation = totalHits.relation;
        float maxScore = hits.getMaxScore();

        log.info("numHits:{} relation:{} maxScore:{}", numHits, relation, maxScore);

        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {

            String index = hit.getIndex();
            String id = hit.getId();
            float score = hit.getScore();

            String sourceAsString = hit.getSourceAsString();

            //Car car = convertMapToCar(hit.getSourceAsMap());
            //log.info(car);
            log.info("index:{} id:{} score:{} sourceAsString:{}", index, id, score, sourceAsString);

        }

    }

    private void displaySearchOperationResults(SearchResponse searchResponse) {
        RestStatus status = searchResponse.status();
        TimeValue took = searchResponse.getTook();
        Boolean terminatedEarly = searchResponse.isTerminatedEarly();
        boolean timedOut = searchResponse.isTimedOut();

        int totalShards = searchResponse.getTotalShards();
        int successfulShards = searchResponse.getSuccessfulShards();
        int failedShards = searchResponse.getFailedShards();
        for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
            log.error("failure:{}", failure);
        }

        log.info("status:{} elapsedTime:{} terminatedEarly:{} timedOut:{} totalShards:{} successfulShards:{} failedShards:{}",
                status, took.getMillis(), terminatedEarly, timedOut,
                totalShards, successfulShards, failedShards);

    }

    private Map<String, Object> convertCarToMap(Car car) {
        return objectMapper.convertValue(car, Map.class);
    }

    private Car convertMapToCar(Map<String, Object> map){
        return objectMapper.convertValue(map, Car.class);
    }
}
