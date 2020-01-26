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
	private PersonService elasticSearchService;

	@Autowired
	private WordService wordService;

	@Autowired
	private CarService carService;

	public static void main(String[] args) {
		SpringApplication.run(SpringBootElasticsearchApplication.class, args);
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		//boolean response = elasticSearchService.createIndex("person");
		//log.info("test_index created: {}", response);
		//response = elasticSearchService.deleteIndex("test_index");
		//log.info("test_index deleted: {}", response);
		//savePerson();
		//saveWord();
		testCarService();
	}

	void testCarService() {
		//carService.createIndexWithExplicitMapping();
		//carService.saveBulkData();
		//carService.searchData1();
		//carService.searchNormalCarsWithPagination();
		//carService.searchDieselCarsByBool2();
		//carService.searchJeepOrSportCars();
		//carService.searchCarsPriceBetween_50000_150000();
		//carService.getBrands();
		//carService.getAggregationByCategory();
		//carService.getCountByCountApi();
		carService.getCarsHaving_10_000_price_By_Pagination();
	}

	void createWordIndex() {
		log.info("word index created: {}", wordService.createIndex());
	}

	void saveWord() {
		wordService.saveWord();
	}

	 void savePerson() {

		Person p = new Person();
		p.setId("p3");
		p.setAge(25);
		p.setName("admin3");
		log.info("person saved resp: " + elasticSearchService.savePerson(p));

		p.setAge(35);
		log.info("person updated: " + elasticSearchService.updatePerson(p));

		log.info("retrieved person {}", elasticSearchService.findPersonById(p.getId()));

		log.info("deleted person with id:{}", elasticSearchService.deletePersonById("p3"));

	}
}
