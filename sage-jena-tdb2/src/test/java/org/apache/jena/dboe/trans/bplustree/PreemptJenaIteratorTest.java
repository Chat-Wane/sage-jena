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

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Testing the pausing/resuming capabilities of {@link PreemptJenaIterator}.
 * The results should always be identical to that of normal iterators.
 **/
class PreemptJenaIteratorTest {

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
    public void read_the_whole_using_predicate() {
        BackendIterator<?, ?> it = backend.search(any, predicate, any);
        int nbResults = 0;
        while (it.hasNext()) {
            it.next();
            nbResults += 1;
        }
        assertEquals(dataset.getDefaultModel().size(), nbResults);
    }

    @Test
    public void read_only_city_2() {
        NodeId city_2 = backend.getId("<http://db.uwaterloo.ca/~galuc/wsdbm/City102>");
        BackendIterator<?,?> it = backend.search(city_2, any, any);
        int nbResults = 0;
        while (it.hasNext()) {
            it.next();
            nbResults += 1;
        }
        assertEquals(1, nbResults );
        // only one triple so we are sure of its values
        assertEquals("http://db.uwaterloo.ca/~galuc/wsdbm/Country17", it.getValue(SPOC.OBJECT));
        assertEquals("http://www.geonames.org/ontology#parentCountry", it.getValue(SPOC.PREDICATE));
        assertEquals("http://db.uwaterloo.ca/~galuc/wsdbm/City102", it.getValue(SPOC.SUBJECT));
    }

    @Test
    public void read_first_then_pause_resume() {
        ArrayList<String> subjects = new ArrayList<>();
        ArrayList<String> predicates = new ArrayList<>();
        ArrayList<String> objects = new ArrayList<>();
        fillWithSolutions(subjects, predicates, objects, any, predicate, any);

        BackendIterator<?, SerializableRecord> it = backend.search(any, predicate, any);
        assert(it.hasNext());
        it.next();
        assertTriple(subjects, predicates, objects, 0, it);

        assertNotNull(it.current());
        BackendIterator<?, SerializableRecord> it2 = backend.search(any, predicate, any);
        it2.skip(it.current());
        assert(it2.hasNext());
        it2.next();
        assertTriple(subjects,predicates,objects, 1, it2);

        BackendIterator<?, SerializableRecord> it3 = backend.search(any, predicate, any);
        it3.skip(it2.current());
        assert(it3.hasNext());
        it3.next();
        assertTriple(subjects,predicates,objects, 2, it3);
    }


    @Test
    public void read_half_pause_resume_then_the_rest() {
        ArrayList<String> subjects = new ArrayList<>();
        ArrayList<String> predicates = new ArrayList<>();
        ArrayList<String> objects = new ArrayList<>();
        fillWithSolutions(subjects, predicates, objects, any, predicate, any);

        BackendIterator<?, SerializableRecord> it = backend.search(any, predicate, any);
        int nbResults = 0;
        int stoppingIndex = 5;
        while (nbResults < stoppingIndex && it.hasNext()) { // carefully call hasNext after stopping
            it.next();
            nbResults += 1;
        }

        assertEquals(subjects.get(stoppingIndex - 1), it.getValue(SPOC.SUBJECT));
        assertEquals(predicates.get(stoppingIndex - 1), it.getValue(SPOC.PREDICATE));
        assertEquals(objects.get(stoppingIndex - 1), it.getValue(SPOC.OBJECT));

        BackendIterator<?, SerializableRecord> it2 = backend.search(any, predicate, any);
        it2.skip(it.current());

        int nbResultsFinish = 0;
        while (it2.hasNext()) {
            it2.next();
            assertEquals(subjects.get(stoppingIndex + nbResultsFinish), it2.getValue(SPOC.SUBJECT));
            assertEquals(predicates.get(stoppingIndex + nbResultsFinish), it2.getValue(SPOC.PREDICATE));
            assertEquals(objects.get(stoppingIndex + nbResultsFinish), it2.getValue(SPOC.OBJECT));
            nbResultsFinish += 1;
        }

        assertEquals(dataset.getDefaultModel().size(), nbResultsFinish + nbResults);
    }


    @Test
    public void nested_scans_with_stop_resume_at_first() {
        NodeId city_2 = backend.getId("<http://db.uwaterloo.ca/~galuc/wsdbm/City102>");
        BackendIterator<NodeId, SerializableRecord> it = backend.search(city_2, any, any);

        SageOutput<SerializableRecord> output = new SageOutput<>();

        // #1 first part of the query, we stop at first result
        int nb_results = 0;
        while (it.hasNext()) {
            it.next();
            BackendIterator<?, SerializableRecord> it_2 = backend.search(any, it.getId(SPOC.PREDICATE), any);
            while (it_2.hasNext()) {
                it_2.next();
                nb_results += 1;
                if (nb_results >= 1) { // limit 1
                    assertNull(it.previous());
                    output.save(new Pair<>(0, it.previous()), new Pair<>(1, it_2.current()));
                    break;
                }
            }
        }

        assertEquals(1, nb_results);

        // #2 second part of the query, we restart from the first result, then we run till the end.
        BackendIterator<NodeId, SerializableRecord> it_resume = backend.search(city_2, any, any);
        it_resume.skip(output.getState().get(0));
        while (it_resume.hasNext()) {
            it_resume.next();
            BackendIterator<?, SerializableRecord> it_2_resume = backend.search(any, it.getId(SPOC.PREDICATE), any);
            if (output.getState().containsKey(1)) {
                SerializableRecord to = output.getState().remove(1);
                it_2_resume.skip(to);
            }
            while (it_2_resume.hasNext()) {
                it_2_resume.next();
                nb_results += 1;
            }
        }

        assertEquals(10, nb_results);
    }



    /**
     * Convenience function to assert the current values of the iterator compared to the truth.
     */
    static void assertTriple(ArrayList<String> s, ArrayList<String> p, ArrayList<String> o, int index, BackendIterator<?,?> it) {
        assertEquals(s.get(index), it.getValue(SPOC.SUBJECT));
        assertEquals(p.get(index), it.getValue(SPOC.PREDICATE));
        assertEquals(o.get(index), it.getValue(SPOC.OBJECT));
    }

    /**
     * Fills the arrays with solutions corresponding to the pattern.
     */
    static void fillWithSolutions(ArrayList<String> subjects, ArrayList<String> predicates, ArrayList<String> objects,
                                  NodeId s, NodeId p, NodeId o) {
        BackendIterator<?, ?> baseline_it = backend.search(s, p, o);
        while (baseline_it.hasNext()) {
            baseline_it.next();
            subjects.add(baseline_it.getValue(SPOC.SUBJECT));
            predicates.add(baseline_it.getValue(SPOC.PREDICATE));
            objects.add(baseline_it.getValue(SPOC.OBJECT));
        }
    }

}