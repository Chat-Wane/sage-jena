package fr.gdd.sage;

import fr.gdd.sage.arq.SageConstants;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ResultSet;
import org.apache.jena.reasoner.rulesys.impl.BindingVectorMultiSet;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingRoot;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.tdb2.solver.QueryEngineTDB;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpExecutorRandomBGPTest {

    static Dataset dataset;

    @BeforeAll
    public static void initializeDB() {
        dataset = new InMemoryInstanceOfTDB2ForRandom().getDataset();
        QueryEngineRandom.register();
    }

    @AfterAll
    public static void closeDB() {
        dataset.abort();
        TDBInternal.expel(dataset.asDatasetGraph());
    }

    @Test
    public void get_a_random_from_a_triple_pattern() {
        Op op = SSE.parseOp("(bgp (?s <http://address> ?o))");
        Set<Binding> allBindings = generateResults(op);

        Context c = dataset.getContext().copy().set(SageConstants.limit, 1);
        QueryEngineFactory factory = QueryEngineRegistry.findFactory(op, dataset.asDatasetGraph(), c);
        Plan plan = factory.create(op, dataset.asDatasetGraph(), BindingRoot.create(), c);

        QueryIterator iterator = plan.iterator();
        long sum = 0;
        while (iterator.hasNext()) {
            assertTrue(allBindings.contains(iterator.next()));
            sum += 1;
        }
        assertEquals(1, sum);
    }

    @Disabled
    @Test
    public void get_1000_randoms_from_a_triple_pattern() {
        Op op = SSE.parseOp("(bgp (?s <http://address> ?o))");
        Set<Binding> allBindings = generateResults(op);

        final long LIMIT = 1000;
        Context c = dataset.getContext().copy().set(SageConstants.limit, LIMIT);
        QueryEngineFactory factory = QueryEngineRegistry.findFactory(op, dataset.asDatasetGraph(), c);
        Plan plan = factory.create(op, dataset.asDatasetGraph(), BindingRoot.create(), c);

        QueryIterator iterator = plan.iterator();
        long sum = 0;
        while (iterator.hasNext()) {
            assertTrue(allBindings.contains(iterator.next()));
            sum += 1;
        }
        assertEquals(LIMIT, sum);
        // (TODO) assert that every binding of the result has a random binding, (with a high probability)
    }

    /**
     * Generates all bindings of an operation in order to check if random results belong to it.
     * @param op The operation to execute.
     * @return A set of bindings.
     */
    static Set<Binding> generateResults(Op op){
        Plan planTDB = QueryEngineTDB.getFactory().create(op, dataset.asDatasetGraph(), BindingRoot.create(), dataset.getContext());
        HashSet<Binding> bindings = new HashSet<>();
        QueryIterator iteratorTDB = planTDB.iterator();
        while (iteratorTDB.hasNext()) {
            bindings.add(iteratorTDB.next());
        }
        return bindings;
    }

}