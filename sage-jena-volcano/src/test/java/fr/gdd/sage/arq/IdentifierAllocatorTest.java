package fr.gdd.sage.arq;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.OpVisitorByType;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.sse.SSE;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class IdentifierAllocatorTest {

    Logger log = LoggerFactory.getLogger(IdentifierAllocatorTest.class);

    @Test
    public void simple_test_with_only_one_bgp () {
        Op op = SSE.parseOp("(bgp (?s <http://www.geonames.org/ontology#parentCountry> ?o))");
        IdentifierAllocator a = new IdentifierAllocator();
        op.visit(a);

        op.visit(new OpVisitorBase() {
            @Override
            public void visit(OpBGP opBGP) {
                assertEquals(1, a.getId(opBGP));
            }
        });
    }

    @Test
    public void two_bgps_linked_by_a_join () {
        Op op = SSE.parseOp("(join " +
                "(bgp (?s <http://A> ?o)(?s <http://C> ?o)) " +
                "(bgp (?s <http://B> ?o)))");
        IdentifierAllocator a = new IdentifierAllocator();
        op.visit(a);

        op.visit(new OpVisitorBase() {
            @Override
            public void visit(OpBGP opBGP) {
                switch (opBGP.getPattern().get(0).getPredicate().toString()) {
                    case "http://A" -> assertEquals(1, a.getId(opBGP)); // can provide id 0 and id 1
                    case "http://B" -> assertEquals(3, a.getId(opBGP));
                    default -> assertFalse(true);
                }
            }

            @Override
            public void visit(OpJoin opJoin) {
                opJoin.getLeft().visit(this);
                opJoin.getRight().visit(this);
            }
        });
    }

    @Test
    public void identical_operator_built_independently_return_the_id () {
        Op op = SSE.parseOp("(join " +
                "(bgp (?s <http://A> ?o)(?s <http://C> ?o)) " +
                "(bgp (?s <http://B> ?o)))");
        IdentifierAllocator a = new IdentifierAllocator();
        op.visit(a);

        Op op1 = SSE.parseOp("(bgp (?s <http://A> ?o)(?s <http://C> ?o))");
        assertEquals(1, a.getId(op1));
        Op op2 = SSE.parseOp("(bgp (?s <http://B> ?o))");
        assertEquals(3, a.getId(op2));
    }


    @Test
    public void parents_of_a_bgp() {
        Op op = SSE.parseOp("(bgp (?s <http://A> ?o)(?s <http://C> ?o))");
        IdentifierAllocator a = new IdentifierAllocator();
        op.visit(a);

        assertEquals(Set.of(1), a.getParents(2));
    }

    @Test
    public void parents_of_two_bgps() {
        Op op = SSE.parseOp("(join " +
                "(bgp (?s <http://A> ?o)(?s <http://C> ?o)) " +
                "(bgp (?s <http://B> ?o)))");
        IdentifierAllocator a = new IdentifierAllocator();
        op.visit(a);

        assertEquals(Set.of(), a.getParents(1));
        assertEquals(Set.of(1), a.getParents(2));
        assertEquals(Set.of(2, 1), a.getParents(3));
    }

    @Test
    public void parents_of_an_opt() {
        Op op = SSE.parseOp("(conditional " +
                "(bgp (?s ?p <http://db.uwaterloo.ca/~galuc/wsdbm/Country1>)) " +
                "(bgp (?s <http://www.geonames.org/ontology#doesNotExist> ?o))" + // never true
                ")");
        IdentifierAllocator a = new IdentifierAllocator();
        op.visit(a);

        assertEquals(Set.of(), a.getParents(1)); // tp1
        assertEquals(Set.of(1), a.getParents(2)); // opt id
        assertEquals(Set.of(2, 1), a.getParents(3)); // tp2
    }

    @Test
    public void multiple_independent_optionals () {
        Query query = QueryFactory.create("SELECT * WHERE {?s ?p ?o OPTIONAL {?s <http://P1> ?o} OPTIONAL {?s <http://P2> ?o}}");
        Op op = Algebra.compile(query);

        IdentifierAllocator a = new IdentifierAllocator();
        op.visit(a);

        assertEquals(Set.of(), a.getParents(1)); // tp1
        assertEquals(Set.of(1), a.getParents(2)); // opt1
        assertEquals(Set.of(1,2), a.getParents(3)); // tp2
        assertEquals(Set.of(1), a.getParents(4)); // opt2
        assertEquals(Set.of(1,4), a.getParents(5)); // tp3
    }

    @Test
    public void nested_optionals() {
        Query query = QueryFactory.create("SELECT * WHERE {?s ?p ?o OPTIONAL { {?s <http://P1> ?o} OPTIONAL {?s <http://P2> ?o}}}");
        Op op = Algebra.compile(query);

        IdentifierAllocator a = new IdentifierAllocator();
        op.visit(a);

        assertEquals(Set.of(), a.getParents(1)); // tp1
        assertEquals(Set.of(1), a.getParents(2)); // opt1
        assertEquals(Set.of(1,2), a.getParents(3)); // tp2
        assertEquals(Set.of(1,2,3), a.getParents(4)); // opt2
        assertEquals(Set.of(1,2,3,4), a.getParents(5)); // tp3
    }

}