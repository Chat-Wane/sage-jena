package fr.gdd.sage.sager;

import fr.gdd.sage.databases.inmemory.InMemoryInstanceOfTDB2ForRandom;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
public class SagerOpExecutorUnionTest {


    private static final Logger log = LoggerFactory.getLogger(SagerOpExecutorUnionTest.class);
    private static final InMemoryInstanceOfTDB2ForRandom dataset = new InMemoryInstanceOfTDB2ForRandom();


    @Test
    public void execute_a_simple_union () {
        ExecutionContext ec = new ExecutionContext(dataset.getDataset().asDatasetGraph());
        String queryAsString = """
               SELECT * WHERE {
                {?p  <http://own>  ?a}
                UNION
                {?p  <http://address> ?a}
               }""";
        int nbResults = SagerOpExecutorTest.executeWithSager(queryAsString, ec);
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
        int nbResults = SagerOpExecutorTest.executeWithSager(queryAsString, ec);
        assertEquals(6, nbResults); // (cat + dog + snake)*2
    }

}
