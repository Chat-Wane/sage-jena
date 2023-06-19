package fr.gdd.sage.arq;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.sse.SSE;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class IdentifierLinkerTest {

    @Test
    public void parents_of_a_bgp() {
        Op op = SSE.parseOp("(bgp (?s <http://A> ?o)(?s <http://C> ?o))");
        IdentifierLinker l = new IdentifierLinker(op);

        assertEquals(Set.of(1), l.getParents(2));
    }

    @Test
    public void parents_of_two_bgps() {
        Op op = SSE.parseOp("(join " +
                "(bgp (?s <http://A> ?o)(?s <http://C> ?o)) " +
                "(bgp (?s <http://B> ?o)))");
        IdentifierLinker l = new IdentifierLinker(op);

        assertEquals(Set.of(), l.getParents(1));
        assertEquals(Set.of(1), l.getParents(2));
        assertEquals(Set.of(2, 1), l.getParents(3));
    }

    @Test
    public void parents_of_an_opt() {
        Op op = SSE.parseOp("(conditional " +
                "(bgp (?s ?p <http://db.uwaterloo.ca/~galuc/wsdbm/Country1>)) " +
                "(bgp (?s <http://www.geonames.org/ontology#doesNotExist> ?o))" + // never true
                ")");
        IdentifierLinker l = new IdentifierLinker(op);

        assertEquals(Set.of(), l.getParents(1)); // tp1
        assertEquals(Set.of(1), l.getParents(2)); // opt id
        assertEquals(Set.of(2, 1), l.getParents(3)); // tp2
    }

    @Test
    public void multiple_independent_optionals () {
        Query query = QueryFactory.create("SELECT * WHERE {?s ?p ?o OPTIONAL {?s <http://P1> ?o} OPTIONAL {?s <http://P2> ?o}}");
        Op op = Algebra.compile(query);
        IdentifierLinker l = new IdentifierLinker(op);

        assertEquals(Set.of(), l.getParents(1)); // tp1
        assertEquals(Set.of(1), l.getParents(2)); // opt1
        assertEquals(Set.of(1,2), l.getParents(3)); // tp2
        // the optional is independent, but still, it must be saved in case it is not.
        assertEquals(Set.of(1,2,3), l.getParents(4)); // opt2
        assertEquals(Set.of(1,2,3,4), l.getParents(5)); // tp3
    }

    @Test
    public void nested_optionals() {
        Query query = QueryFactory.create("SELECT * WHERE {?s ?p ?o OPTIONAL { {?s <http://P1> ?o} OPTIONAL {?s <http://P2> ?o}}}");
        Op op = Algebra.compile(query);
        IdentifierLinker l = new IdentifierLinker(op);

        assertEquals(Set.of(), l.getParents(1)); // tp1
        assertEquals(Set.of(1), l.getParents(2)); // opt1
        assertEquals(Set.of(1,2), l.getParents(3)); // tp2
        assertEquals(Set.of(1,2,3), l.getParents(4)); // opt2
        assertEquals(Set.of(1,2,3,4), l.getParents(5)); // tp3
    }

}