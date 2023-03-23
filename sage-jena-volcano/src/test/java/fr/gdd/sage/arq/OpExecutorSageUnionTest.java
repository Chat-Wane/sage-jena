package fr.gdd.sage.arq;

import fr.gdd.sage.InMemoryInstanceOfTDB2;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static fr.gdd.sage.arq.OpExecutorSageBGPTest.run_to_the_limit;
import static org.junit.jupiter.api.Assertions.*;

class OpExecutorSageUnionTest {

    static Dataset dataset = null;

    @BeforeAll
    public static void initializeDB() {
        dataset = new InMemoryInstanceOfTDB2().getDataset();

        // set up the chain of execution to use Sage when called on this dataset
        QC.setFactory(dataset.getContext(), new OpExecutorSage.OpExecutorSageFactory(ARQ.getContext()));
        QueryEngineRegistry.addFactory(QueryEngineSage.factory);
    }

    @AfterAll
    public static void closeDB() {
        dataset.abort();
        TDBInternal.expel(dataset.asDatasetGraph());
    }


    @Test
    public void simple_union_over_bgps() {
        Op op = SSE.parseOp("(union " +
                "(bgp (?s ?p <http://db.uwaterloo.ca/~galuc/wsdbm/Country1>)) " +
                "(bgp (<http://db.uwaterloo.ca/~galuc/wsdbm/City0> <http://www.geonames.org/ontology#parentCountry> ?o))" +
                ")");

        SageOutput<?> output = run_to_the_limit(dataset, op, new SageInput<>());
        assertEquals(3, output.size());
    }

    @Disabled
    @Test
    public void preempt_at_2nd_tuple_after_the_left_hand_side_of_union() {
        Op op = SSE.parseOp("(union " +
                "(bgp (?s ?p <http://db.uwaterloo.ca/~galuc/wsdbm/Country1>)) " +
                "(bgp (<http://db.uwaterloo.ca/~galuc/wsdbm/City0> <http://www.geonames.org/ontology#parentCountry> ?o))" +
                ")");

        SageOutput<?> output = run_to_the_limit(dataset, op, new SageInput<>().setLimit(2));
        assertEquals(2, output.size());
        output = run_to_the_limit(dataset, op, new SageInput<>());
        assertEquals(1, output.size());
    }
}