package fr.gdd.sage.sager;

import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.databases.inmemory.InMemoryInstanceOfTDB2WithSimpleData;
import fr.gdd.sage.sager.optimizers.BGP2Triples;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertTrue;

class Save2SPARQLTest {

    private static final Logger log = LoggerFactory.getLogger(Save2SPARQLTest.class);
    private static final InMemoryInstanceOfTDB2WithSimpleData data = new InMemoryInstanceOfTDB2WithSimpleData();


    @Disabled
    @Test
    public void create_a_simple_query_and_pause_at_first_result () {
        String queryAsString = "SELECT * WHERE {?s <http://named> ?o}";

        while (Objects.nonNull(queryAsString)) {
            queryAsString = executeQuery(queryAsString);
        }
    }

    public static String executeQuery(String queryAsString) {
        ARQ.enableOptimizer(false);

        Op query = Algebra.compile(QueryFactory.create(queryAsString));
        query = ReturningOpVisitorRouter.visit(new BGP2Triples(), query);

        ExecutionContext ec = new ExecutionContext(data.getDataset().asDatasetGraph());
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