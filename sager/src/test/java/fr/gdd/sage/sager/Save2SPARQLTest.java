package fr.gdd.sage.sager;

import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.databases.inmemory.InMemoryInstanceOfTDB2WithSimpleData;
import fr.gdd.sage.sager.optimizers.BGP2Triples;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class Save2SPARQLTest {

    @Disabled
    @Test
    public void create_a_simple_query_and_pause_at_first_result () {
        InMemoryInstanceOfTDB2WithSimpleData data = new InMemoryInstanceOfTDB2WithSimpleData();
        ExecutionContext ec = new ExecutionContext(data.getDataset().asDatasetGraph());
        SagerOpExecutor executor = new SagerOpExecutor(ec);

        ARQ.enableOptimizer(false);
        Op query = Algebra.compile(QueryFactory.create("SELECT * WHERE {?s <http://named> ?o}"));
        query = ReturningOpVisitorRouter.visit(new BGP2Triples(), query);
        QueryIterator iterator = executor.execute(query);

        int sum = 0;
        assertTrue(iterator.hasNext());
        iterator.next();
        Save2SPARQL saver = ec.getContext().get(SagerConstants.SAVER);
        Op saved = saver.save(saver.op2it.keySet().iterator().next());
    }

}