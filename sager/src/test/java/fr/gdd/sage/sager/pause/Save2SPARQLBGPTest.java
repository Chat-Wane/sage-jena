package fr.gdd.sage.sager.pause;

import fr.gdd.sage.databases.inmemory.InMemoryInstanceOfTDB2ForRandom;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
public class Save2SPARQLBGPTest {

    private static final Logger log = LoggerFactory.getLogger(Save2SPARQLBGPTest.class);
    private static final InMemoryInstanceOfTDB2ForRandom dataset = new InMemoryInstanceOfTDB2ForRandom();

    @Test
    public void create_a_simple_query_and_pause_at_each_result () {
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c}";

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            queryAsString = Save2SPARQLTest.executeQuery(queryAsString, dataset.getDataset());
            sum += 1;
        }
        sum -= 1; // last call does not retrieve results
        assertEquals(3, sum);
    }

    @Test
    public void create_a_bgp_query_and_pause_at_each_result () {
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> <http://nantes> .
                ?p <http://own> ?a .
               }""";
        log.debug(queryAsString);

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            queryAsString = Save2SPARQLTest.executeQuery(queryAsString, dataset.getDataset());
            sum += 1;
        }
        sum -= 1; // last call does not retrieve results
        assertEquals(3, sum);
    }

    @Test
    public void create_a_bgp_query_and_pause_at_each_result_but_different_order () {
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://own> ?a .
                ?p <http://address> <http://nantes> .
               }""";

        log.debug(queryAsString);

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            queryAsString = Save2SPARQLTest.executeQuery(queryAsString, dataset.getDataset());
            sum += 1;
        }
        sum -= 1; // last call does not retrieve results
        assertEquals(3, sum);
    }

    @Test
    public void bgp_with_3_tps_that_preempt () {
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://own> ?a .
                ?p <http://address> <http://nantes> .
                ?a <http://species> ?s
               }""";

        log.debug(queryAsString);

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            queryAsString = Save2SPARQLTest.executeQuery(queryAsString, dataset.getDataset());
            sum += 1;
        }
        sum -= 1; // last call does not retrieve results
        assertEquals(3, sum);
    }

}
