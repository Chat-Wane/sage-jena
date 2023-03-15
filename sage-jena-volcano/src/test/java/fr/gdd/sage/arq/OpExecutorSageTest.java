package fr.gdd.sage.arq;

import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import fr.gdd.sage.jena.JenaBackend;
import fr.gdd.sage.jena.SerializableRecord;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.BindingRoot;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Testing the executor by building queries by hand.
 */
class OpExecutorSageTest {

    static Dataset dataset = null;
    static JenaBackend backend = null;

    static NodeId predicate = null;
    static NodeId any = null;

    @BeforeAll
    public static void initializeDB() {
        // (TODO) import In memory database
        dataset = TDB2Factory.createDataset();
        dataset.begin(ReadWrite.WRITE);

        // Model containing the 10 first triples of Dataset Watdiv.10M
        // Careful, the order in the DB is not identical to that of the array
        List<String> statements = Arrays.asList(
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City0>   <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country6>.",
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City100> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country2>.",
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City101> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country2> .",
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City102> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country17> .",
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City103> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country3> .",
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City104> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country1> .",
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City105> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country0> .",
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City106> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country10> .",
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City107> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country23> .",
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City108> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country1>."
        );

        InputStream statementsStream = new ByteArrayInputStream(String.join("\n",statements).getBytes());
        Model model = ModelFactory.createDefaultModel();
        model.read(statementsStream, "", Lang.NT.getLabel());

        statements = Arrays.asList(
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City0>   <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country6>.",
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City100> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country2>.",
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City101> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country2> ."
        );
        statementsStream = new ByteArrayInputStream(String.join("\n",statements).getBytes());
        Model modelA = ModelFactory.createDefaultModel();
        modelA.read(statementsStream, "", Lang.NT.getLabel());

        statements = List.of(
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City102> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country17> ."
        );
        statementsStream = new ByteArrayInputStream(String.join("\n",statements).getBytes());
        Model modelB = ModelFactory.createDefaultModel();
        modelB.read(statementsStream, "", Lang.NT.getLabel());

        dataset.setDefaultModel(model);
        dataset.addNamedModel("https://graphA.org", modelA);
        dataset.addNamedModel("https://graphB.org", modelB);

        backend = new JenaBackend(dataset);
        predicate = backend.getId("<http://www.geonames.org/ontology#parentCountry>");
        any = backend.any();

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
    public void simple_select_all_triples() {
        Op op = SSE.parseOp("(bgp (?s ?p ?o))");

        SageOutput<?> output = run_to_the_limit(op, new SageInput<>());
        assertEquals(10, output.size());
    }

    @Test
    public void simple_select_all_triples_by_predicate() {
        Op op = SSE.parseOp("(bgp (?s <http://www.geonames.org/ontology#parentCountry> ?o))");

        SageOutput<?> output = run_to_the_limit(op, new SageInput<>());
        assertEquals(10, output.size());
    }

    @Test
    public void select_all_triple_but_pauses_at_first_then_resume() {
        Op op = SSE.parseOp("(bgp (?s <http://www.geonames.org/ontology#parentCountry> ?o))");

        // #A we set a limit of only one result on first execution
        SageOutput<?> output = run_to_the_limit(op, new SageInput<>().setLimit(1));

        // #B Then we don't set a limit to get the other 9 results
        // thanks to `output.getState()`, the iterator is able to skip where the previous paused its execution
        SageOutput<?> rest = run_to_the_limit(op, new SageInput().setState(output.getState()));
        assertEquals(9, rest.size());
    }

    @Test
    public void simple_bgp_then_pause_at_first_then_resume() {
        Op op = SSE.parseOp("(bgp (<http://db.uwaterloo.ca/~galuc/wsdbm/City102> ?p <http://db.uwaterloo.ca/~galuc/wsdbm/Country17>)" +
                " (?s <http://www.geonames.org/ontology#parentCountry> ?o))");

        SageOutput<?> out = run_to_the_limit(op, new SageInput<>().setLimit(1));
        SageOutput<?> rest = run_to_the_limit(op, new SageInput().setState(out.getState()));
        assertEquals(9, rest.size());
    }


    public SageOutput<SerializableRecord> run_to_the_limit(Op query, SageInput<?> input) {
        boolean limitIsSet = input.getLimit() != Long.MAX_VALUE;
        Context c = dataset.getContext().copy().set(SageConstants.input, input);
        Plan plan = QueryEngineSage.factory.create(query, dataset.asDatasetGraph(), BindingRoot.create(), c);
        QueryIterator it = plan.iterator();

        long nb_results = 0;
        while (it.hasNext()) {
            it.next();
            nb_results += 1;
        }
        SageOutput<SerializableRecord> output = c.get(SageConstants.output);
        if (limitIsSet) {
            assertEquals(input.getLimit(), nb_results);
            assertEquals(input.getLimit(), output.size());
        }
        return output;
    }

}