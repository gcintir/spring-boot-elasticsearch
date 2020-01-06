package springbootelasticsearch;

import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Log4j2
public class SpringBootElasticsearchApplication implements ApplicationRunner {

	@Autowired
	private ElasticSearchService elasticSearchService;

	public static void main(String[] args) {
		SpringApplication.run(SpringBootElasticsearchApplication.class, args);
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		boolean response = elasticSearchService.createIndex("test_index2");
		log.info("test_index created: {}", response);
		response = elasticSearchService.deleteIndex("test_index");
		log.info("test_index deleted: {}", response);
	}
}
