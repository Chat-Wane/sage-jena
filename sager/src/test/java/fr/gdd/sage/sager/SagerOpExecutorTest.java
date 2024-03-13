package fr.gdd.sage.sager;

import fr.gdd.sage.databases.inmemory.InMemoryInstanceOfTDB2ForRandom;
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

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
class SagerOpExecutorTest {

    private static final Logger log = LoggerFactory.getLogger(SagerOpExecutorTest.class);
    private static final InMemoryInstanceOfTDB2ForRandom dataset = new InMemoryInstanceOfTDB2ForRandom();

    @Test
    public void create_a_simple_query_and_execute_it () {
        ExecutionContext ec = new ExecutionContext(dataset.getDataset().asDatasetGraph());
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c}";
        int nbResults = executeWithSager(queryAsString, ec);
        assertEquals(3, nbResults); // Bob, Alice, and Carol.
    }

    @Test
    public void create_a_bgp_and_execute_it () {
        ExecutionContext ec = new ExecutionContext(dataset.getDataset().asDatasetGraph());
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> <http://nantes> .
                ?p  <http://own>  ?a .
               }""";
        int nbResults = executeWithSager(queryAsString, ec);
        assertEquals(3, nbResults); // Alice, Alice, and Alice.
    }

    @Test
    public void create_a_bind_and_execute () {
        ExecutionContext ec = new ExecutionContext(dataset.getDataset().asDatasetGraph());
        String queryAsString = """
               SELECT * WHERE {
                BIND (<http://Alice> AS ?p)
                ?p  <http://own>  ?a .
               }""";
        int nbResults = executeWithSager(queryAsString, ec);
        assertEquals(3, nbResults); // Alice, Alice, and Alice.
    }

    @Test
    public void execute_a_simple_union () {
        ExecutionContext ec = new ExecutionContext(dataset.getDataset().asDatasetGraph());
        String queryAsString = """
               SELECT * WHERE {
                {?p  <http://own>  ?a}
                UNION
                {?p  <http://address> ?a}
               }""";
        int nbResults = executeWithSager(queryAsString, ec);
        assertEquals(6, nbResults); // 3 triples + 3 triples
    }

    @Test
    public void execute_a_union_inside_a_triple_pattern () {
        ExecutionContext ec = new ExecutionContext(dataset.getDataset().asDatasetGraph());
        String queryAsString = """
               SELECT * WHERE {
                ?p  <http://own>  ?a .
                {?a <http://species> ?s} UNION {?a <http://species> ?s}
               }""";
        int nbResults = executeWithSager(queryAsString, ec);
        assertEquals(6, nbResults); // (cat + dog + snake)*2
    }

    @Test
    public void meow () {
        ExecutionContext ec = new ExecutionContext(dataset.getDataset().asDatasetGraph());
        String queryAsString = """
                SELECT * WHERE { { SELECT  *
                                          WHERE
                                            { ?p  <http://own>  ?a }
                                          OFFSET  1
                                        }
                                        ?p  <http://address>  <http://nantes>
                                      }""";
        int nbResults = executeWithSager(queryAsString, ec);
        assertEquals(2, nbResults); // (cat + dog + snake)*2
    }

    /* ****************************************************************** */

    public static int executeWithSager(String queryAsString, ExecutionContext ec) {
        ARQ.enableOptimizer(false);
        SagerOpExecutor executor = new SagerOpExecutor(ec);

        Op query = Algebra.compile(QueryFactory.create(queryAsString));
        QueryIterator iterator = executor.optimizeThenExecute(query);

        int sum = 0;
        while (iterator.hasNext()) {
            Binding binding = iterator.next();
            log.debug("{}", binding.toString());
            sum += 1;
        }

        return sum;
    }

    /* ****************************************************************** */

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
        log.debug("{}", query);
    }
}