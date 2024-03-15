package fr.gdd.sage.sager.pause;

import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.databases.inmemory.InMemoryInstanceOfTDB2ForRandom;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.SagerOpExecutor;
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

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
class Save2SPARQLTest {

    private static final Logger log = LoggerFactory.getLogger(Save2SPARQLTest.class);

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