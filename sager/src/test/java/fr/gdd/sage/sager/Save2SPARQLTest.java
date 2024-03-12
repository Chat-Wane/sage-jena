package fr.gdd.sage.sager;

import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.databases.inmemory.InMemoryInstanceOfTDB2ForRandom;
import fr.gdd.sage.sager.optimizers.BGP2Triples;
import fr.gdd.sage.sager.pause.Save2SPARQL;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
class Save2SPARQLTest {

    private static final Logger log = LoggerFactory.getLogger(Save2SPARQLTest.class);
    private static final InMemoryInstanceOfTDB2ForRandom dataset = new InMemoryInstanceOfTDB2ForRandom();

    @Disabled
    @Test
    public void create_a_simple_query_and_pause_at_each_result () {
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c}";

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            queryAsString = executeQuery(queryAsString, dataset.getDataset());
            sum += 1;
        }
        sum -= 1; // last call does not retrieve results
        assertEquals(3, sum);
    }

    @Disabled
    @Test
    public void create_a_bgp_query_and_pause_at_each_result () {
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> <http://nantes> .
                ?p  <http://own>  ?a .
               }""";
        log.debug(queryAsString);

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            queryAsString = executeQuery(queryAsString, dataset.getDataset());
            sum += 1;
        }
        sum -= 1; // last call does not retrieve results
        assertEquals(3, sum);
    }

    @Disabled
    @Test
    public void create_a_bgp_query_and_pause_at_each_result_but_different_order () {
        String queryAsString = """
               SELECT * WHERE {
                ?p  <http://own>  ?a .
                ?p <http://address> <http://nantes> .
               }""";

        log.debug(queryAsString);

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            queryAsString = executeQuery(queryAsString, dataset.getDataset());
            sum += 1;
        }
        sum -= 1; // last call does not retrieve results
        assertEquals(3, sum);
    }



    /* ************************************************************** */

    public static String executeQuery(String queryAsString, Dataset dataset) {
        ARQ.enableOptimizer(false);

        Op query = Algebra.compile(QueryFactory.create(queryAsString));
        query = ReturningOpVisitorRouter.visit(new BGP2Triples(), query);

        ExecutionContext ec = new ExecutionContext(dataset.asDatasetGraph());
        SagerOpExecutor executor = new SagerOpExecutor(ec);

        QueryIterator iterator = executor.optimizeThenExecute(query);
        if (!iterator.hasNext()) {
            return null;
        }
        log.debug("{}", iterator.next());

        Save2SPARQL saver = ec.getContext().get(SagerConstants.SAVER);
        Op saved = saver.save(null);
        String savedAsString = OpAsQuery.asQuery(saved).toString();
        log.debug(savedAsString);
        return savedAsString;
    }

}