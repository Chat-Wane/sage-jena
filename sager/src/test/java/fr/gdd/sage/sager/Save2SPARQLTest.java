package fr.gdd.sage.sager;

import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.databases.inmemory.InMemoryInstanceOfTDB2ForRandom;
import fr.gdd.sage.databases.inmemory.InMemoryInstanceOfTDB2WithSimpleData;
import fr.gdd.sage.sager.optimizers.BGP2Triples;
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

class Save2SPARQLTest {

    private static final Logger log = LoggerFactory.getLogger(Save2SPARQLTest.class);
    private static final InMemoryInstanceOfTDB2WithSimpleData data = new InMemoryInstanceOfTDB2WithSimpleData();
    private static final InMemoryInstanceOfTDB2ForRandom data2 = new InMemoryInstanceOfTDB2ForRandom();

    @Disabled
    @Test
    public void create_a_simple_query_and_pause_at_each_result () {
        String queryAsString = "SELECT * WHERE {?s <http://named> ?o}";

        while (Objects.nonNull(queryAsString)) {
            queryAsString = executeQuery(queryAsString, data.getDataset());
        }
    }

    @Disabled
    @Test
    public void create_a_bgp_query_and_pause_at_each_result () {
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> <http://nantes> .
                ?p  <http://own>  ?a .
               }""";

        while (Objects.nonNull(queryAsString)) {
            queryAsString = executeQuery(queryAsString, data2.getDataset());
        }
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