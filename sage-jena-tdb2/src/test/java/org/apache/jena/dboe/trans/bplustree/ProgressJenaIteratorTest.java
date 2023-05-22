package org.apache.jena.dboe.trans.bplustree;

import fr.gdd.sage.InMemoryInstanceOfTDB2;
import fr.gdd.sage.generics.Pair;
import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.interfaces.SPOC;
import fr.gdd.sage.io.SageOutput;
import fr.gdd.sage.jena.JenaBackend;
import fr.gdd.sage.jena.SerializableRecord;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class ProgressJenaIteratorTest {

    static Dataset dataset = null;
    static JenaBackend backend = null;

    static NodeId predicate = null;
    static NodeId any = null;

    @BeforeAll
    public static void initializeDB() {
        dataset = new InMemoryInstanceOfTDB2().getDataset();

        backend = new JenaBackend(dataset);
        predicate = backend.getId("<http://www.geonames.org/ontology#parentCountry>");
        any = backend.any();
    }

    @AfterAll
    public static void closeDB() {
        dataset.abort();
        TDBInternal.expel(dataset.asDatasetGraph());
    }

    @Test
    public void simple_progression_test_of_two_loops() {
        SageOutput output = run_loops(backend, new HashMap<>(), 1);

    }

    /**
     * Function that ease the creation of two loops, it stops at the `stopAt`^th next();
     */
    private static SageOutput run_loops(JenaBackend backend, HashMap<Integer, Serializable> savedState, int stopAt) {
        NodeId city_2 = backend.getId("<http://db.uwaterloo.ca/~galuc/wsdbm/City102>");
        BackendIterator<NodeId, Serializable> it = backend.search(city_2, any, any);
        SageOutput<Serializable> output = new SageOutput<>();
        if (savedState.containsKey(0)) {
            it.skip(savedState.get(0));
        }

        int shouldStop = 0;
        while (it.hasNext()) {
            it.next();

            BackendIterator<?, Serializable> it_2 = backend.search(any, it.getId(SPOC.PREDICATE), any);
            if (savedState.containsKey(1)) {
                it_2.skip(savedState.get(1));
            }
            while (it_2.hasNext()) {
                it_2.next();

                shouldStop += 1;
                if (shouldStop >= stopAt) {
                    output.save(new Pair<>(0, it.previous()), new Pair<>(1, it_2.current()));
                    break;
                }
            }
            shouldStop += 1;
            if (shouldStop >= stopAt) {
                output.save(new Pair<>(0, it.current()));
                break;
            }
        }

        return output;
    }

}