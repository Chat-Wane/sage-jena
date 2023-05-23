package fr.gdd.sage.arq;

import fr.gdd.sage.InMemoryInstanceOfTDB2;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.optimize.TransformFilterPlacement;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

class SageOptimizerTest {

    static Logger log = LoggerFactory.getLogger(SageOptimizerTest.class);

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
    public void simple_reordering_with_2_triple_patterns() {
        // triple pattern #1: card 10
        // triple pattern #2: card 2
        // so they should be inverted
        Op op = SSE.parseOp("(bgp (?s <http://www.geonames.org/ontology#parentCountry> ?o) (?s ?p <http://db.uwaterloo.ca/~galuc/wsdbm/Country2>))");
        assertEquals("http://www.geonames.org/ontology#parentCountry", ((OpBGP) op).getPattern().get(0).getPredicate().toString());
        assertEquals("http://db.uwaterloo.ca/~galuc/wsdbm/Country2", ((OpBGP) op).getPattern().get(1).getObject().toString());

        SageOptimizer o = new SageOptimizer(dataset);
        Op newOp = o.transform((OpBGP) op);

        assertTrue(newOp instanceof OpBGP);
        assertEquals("http://db.uwaterloo.ca/~galuc/wsdbm/Country2", ((OpBGP) newOp).getPattern().get(0).getObject().toString());
        assertEquals("http://www.geonames.org/ontology#parentCountry", ((OpBGP) newOp).getPattern().get(1).getPredicate().toString());
    }

    @Test
    public void reordering_with_3_triple_patterns_but_one_is_cartesian_product() {
        // |tp1| = 2
        // |tp2| = 10
        // |tp3| = 1
        // it should be tp3.tp2.tp1 since variables are set in tp2 by tp1.
        Op op = SSE.parseOp("(bgp (?x ?y <http://db.uwaterloo.ca/~galuc/wsdbm/Country2>) (?s <http://www.geonames.org/ontology#parentCountry> ?o) (?s ?p <http://db.uwaterloo.ca/~galuc/wsdbm/Country0>))");
        SageOptimizer o = new SageOptimizer(dataset);
        Op newOp = o.transform((OpBGP) op);

        assertEquals("http://db.uwaterloo.ca/~galuc/wsdbm/Country0", ((OpBGP) newOp).getPattern().get(0).getObject().toString());
        assertEquals("http://www.geonames.org/ontology#parentCountry", ((OpBGP) newOp).getPattern().get(1).getPredicate().toString());
        assertEquals("http://db.uwaterloo.ca/~galuc/wsdbm/Country2", ((OpBGP) newOp).getPattern().get(2).getObject().toString());
    }

}