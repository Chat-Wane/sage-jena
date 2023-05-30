package fr.gdd.sage.arq;

import fr.gdd.sage.databases.inmemory.InMemoryInstanceOfTDB2;
import fr.gdd.sage.databases.persistent.Watdiv10M;
import org.apache.jena.dboe.trans.bplustree.ProgressJenaIterator;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    public void reordering_in_optional_takes_into_account_outside_variables() {
        // |tp1| = 2 |tp2| = 1  |tp3| = 10
        // We have `tp1 OPT (tp2. tp3)` but tp3 has a variable in tp1, so it should be chosen in priority despite its
        // higher cardinality.
        String query_asString = "SELECT * WHERE {?x ?y <http://db.uwaterloo.ca/~galuc/wsdbm/Country2> " +
                "OPTIONAL {?s ?p <http://db.uwaterloo.ca/~galuc/wsdbm/Country0>. " +
                "?x <http://www.geonames.org/ontology#parentCountry> ?o}}";
        Query query = QueryFactory.create(query_asString);
        Op op = Algebra.compile(query);
        SageOptimizer o = new SageOptimizer(dataset);

        Op newOp = Transformer.transform(o, op);
        newOp.visit(new OpVisitorBase() {
            @Override
            public void visit(OpProject opProject) {
                opProject.getSubOp().visit(this);
            }
            @Override
            public void visit(OpLeftJoin opLeftJoin) {
                opLeftJoin.getRight().visit(this); // we only want to check if the right side is properly ordered
            }
            @Override
            public void visit(OpBGP opBGP) {
                assertEquals("http://www.geonames.org/ontology#parentCountry", opBGP.getPattern().get(0).getPredicate().toString());
                assertEquals("http://db.uwaterloo.ca/~galuc/wsdbm/Country0", opBGP.getPattern().get(1).getObject().toString());
            }
        });
    }

    @Disabled
    @Test
    public void try_to_get_watdiv_orders_with_optimizer() throws IOException {
        // unfortunately, it depends on cardinalities that are not accurate enough to reliably
        // get the same results as sage's optimizer on HDT.
        new Watdiv10M(Optional.of("../target/"));
        Dataset dataset = TDB2Factory.connectDataset("../target/watdiv10M");
        SageOptimizer o = new SageOptimizer(dataset);

        ProgressJenaIterator.NB_WALKS = 20000;

        File filePath = new File("../sage-jena-benchmarks/queries/watdiv_with_sage_plan");
        File[] listingAllFiles = filePath.listFiles();
        for (File f : listingAllFiles) {
            String query_asString = Files.readString(f.toPath());
            Query query = QueryFactory.create(query_asString);
            Op op = Algebra.compile(query);

            Op newOp = Transformer.transform(o, op);
            // assertEquals(op, newOp); // may fail because of RNG
            // When differences occurs, they are on close cardinality estimates,
            // this most often lead to inverted triple patterns, sometimes inverted star patterns.
            if (!op.equals(newOp)) {
                log.debug("Old Sage plan: {}", op);
                log.debug("New Sage plan. {}", newOp);
            }
        }
    }
}