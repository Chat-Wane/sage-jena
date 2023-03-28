package fr.gdd.sage.arq;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.sse.SSE;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class IdFactoryTest {

    @Test
    public void simple_triple_gets_simple_id() {
        String triple_asString = "(<http://db.uwaterloo.ca/~galuc/wsdbm/City0> " +
                "<http://www.geonames.org/ontology#parentCountry> " +
                "<http://db.uwaterloo.ca/~galuc/wsdbm/Country6>)";
        Op op = SSE.parseOp("(bgp " + triple_asString + ")");

        IdFactory idFactory = new IdFactory();
        op.visit(idFactory);

        Integer assigned = idFactory.get(new OpTriple(SSE.parseTriple(triple_asString)));
        assertEquals(0, assigned);
    }

    @Test
    public void each_element_of_single_bgp_gets_an_id() {
        String triple1_asString = "(?s ?p <http://db.uwaterloo.ca/~galuc/wsdbm/Country6>)";
        String triple2_asString = "(<http://db.uwaterloo.ca/~galuc/wsdbm/City0> ?p ?o)";
        Op op = SSE.parseOp("(bgp " + triple1_asString + triple2_asString + ")");

        IdFactory idFactory = new IdFactory();
        op.visit(idFactory);

        Integer assigned = idFactory.get(new OpTriple(SSE.parseTriple(triple1_asString)));
        assertEquals(0, assigned);
        assigned = idFactory.get(new OpTriple(SSE.parseTriple(triple2_asString)));
        assertEquals(1, assigned);
    }

    @Test
    public void each_triple_of_union_and_union_gets_an_id() {
        String triple1_asString = "(?s ?p <http://db.uwaterloo.ca/~galuc/wsdbm/Country6>)";
        String triple2_asString = "(<http://db.uwaterloo.ca/~galuc/wsdbm/City0> ?p ?o)";
        String bgp1_asString = String.format("(bgp %s)", triple1_asString);
        String bgp2_asString = String.format("(bgp %s)", triple2_asString);
        Op op = SSE.parseOp("(union "+ bgp1_asString + bgp2_asString + ")");

        IdFactory idFactory = new IdFactory();
        op.visit(idFactory);

        Integer assigned = idFactory.get(new OpUnion(new OpBGP(SSE.parseBGP(bgp1_asString)),
                new OpBGP(SSE.parseBGP(bgp2_asString))));
        assertEquals(0, assigned);
        assigned = idFactory.get(new OpTriple(SSE.parseTriple(triple1_asString)));
        assertEquals(1, assigned);
        assigned = idFactory.get(new OpTriple(SSE.parseTriple(triple2_asString)));
        assertEquals(2, assigned);
    }

}