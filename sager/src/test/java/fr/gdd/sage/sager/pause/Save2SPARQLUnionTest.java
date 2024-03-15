package fr.gdd.sage.sager.pause;

import fr.gdd.sage.databases.inmemory.InMemoryInstanceOfTDB2ForRandom;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
public class Save2SPARQLUnionTest {

    private static final Logger log = LoggerFactory.getLogger(Save2SPARQLUnionTest.class);
    private static final InMemoryInstanceOfTDB2ForRandom dataset = new InMemoryInstanceOfTDB2ForRandom();

    @Test
    public void create_an_simple_union_that_does_not_come_from_preemption() {
        String queryAsString = """
               SELECT * WHERE {
                {?p <http://own> ?a } UNION { ?p <http://address> <http://nantes> }
               }""";

        log.debug(queryAsString);

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            queryAsString = Save2SPARQLTest.executeQuery(queryAsString, dataset.getDataset());
            sum += 1;
        }
        sum -= 1; // last call does not retrieve results
        assertEquals(5, sum); // Alice * 3 + Alice + Carol
    }

    @Test
    public void create_an_simple_union_with_bgp_inside() {
        String queryAsString = """
               SELECT * WHERE {
                {?p <http://own> ?a . ?a <http://species> ?s } UNION { ?p <http://address> <http://nantes> }
               }""";

        log.debug(queryAsString);

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            queryAsString = Save2SPARQLTest.executeQuery(queryAsString, dataset.getDataset());
            sum += 1;
        }
        sum -= 1; // last call does not retrieve results
        assertEquals(5, sum); // Alice * 3 + Alice + Carol
    }
}
