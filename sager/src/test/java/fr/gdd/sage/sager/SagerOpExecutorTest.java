package fr.gdd.sage.sager;

import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.databases.inmemory.InMemoryInstanceOfTDB2ForRandom;
import fr.gdd.sage.databases.inmemory.InMemoryInstanceOfTDB2WithSimpleData;
import fr.gdd.sage.sager.optimizers.BGP2Triples;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

class SagerOpExecutorTest {

    private static final Logger log = LoggerFactory.getLogger(SagerOpExecutorTest.class);

    @Test
    public void create_a_simple_query_and_execute_it () {
        InMemoryInstanceOfTDB2WithSimpleData data = new InMemoryInstanceOfTDB2WithSimpleData();
        ExecutionContext ec = new ExecutionContext(data.getDataset().asDatasetGraph());
        SagerOpExecutor executor = new SagerOpExecutor(ec);

        ARQ.enableOptimizer(false);
        Op query = Algebra.compile(QueryFactory.create("SELECT * WHERE {?s <http://named> ?o}"));
        query = ReturningOpVisitorRouter.visit(new BGP2Triples(), query);
        QueryIterator iterator = executor.optimizeThenExecute(query);

        int sum = 0;
        while (iterator.hasNext()) {
            Binding binding = iterator.next();
            log.debug("{}", binding.toString());
            sum += 1;
        }
        assertEquals(2, sum);
    }

    @Test
    public void create_a_bgp_and_execute_it () {
        InMemoryInstanceOfTDB2ForRandom data = new InMemoryInstanceOfTDB2ForRandom();
        ExecutionContext ec = new ExecutionContext(data.getDataset().asDatasetGraph());
        SagerOpExecutor executor = new SagerOpExecutor(ec);

        ARQ.enableOptimizer(false);
        Op query = Algebra.compile(QueryFactory.create("""
               SELECT * WHERE {
                ?p <http://address> <http://nantes> .
                ?p  <http://own>  ?a .
               }"""));
        query = ReturningOpVisitorRouter.visit(new BGP2Triples(), query);
        QueryIterator iterator = executor.optimizeThenExecute(query);

        int sum = 0;
        while (iterator.hasNext()) {
            Binding binding = iterator.next();
            log.debug("{}", binding.toString());
            sum += 1;
        }
        assertEquals(3, sum); // Alice, Alice, and Alice.
    }


    @Disabled
    @Test
    public void create_a_subquery_to_see_what_it_looks_like () {
        String queryAsString = """
            SELECT * WHERE {
                ?s <http://named> ?o . {
                    SELECT * WHERE {?o <http://owns> ?a} ORDER BY ?o OFFSET 1 LIMIT 1
                }}
            """;
        // Sub-queries are handled with JOIN of the inner operators of the query
        // always slice outside, then order, the bgp
        Op query = Algebra.compile(QueryFactory.create(queryAsString));
    }
}