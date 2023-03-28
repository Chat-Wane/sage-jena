package fr.gdd.sage.arq;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.sse.SSE;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IdFactoryConsumerTest {

    @Test
    public void flattened_union_skips_one_triple_and_consumes_its_id() {
        String triple1_asString = "(?s ?p <http://db.uwaterloo.ca/~galuc/wsdbm/Country6>)";
        String triple2_asString = "(<http://db.uwaterloo.ca/~galuc/wsdbm/City0> ?p ?o)";
        String bgp1_asString = String.format("(bgp %s)", triple1_asString); // 1
        String bgp2_asString = String.format("(bgp %s)", triple2_asString); // 2
        Op op = SSE.parseOp("(union "+ bgp1_asString + bgp2_asString + ")"); // 0

        IdFactory idFactory = new IdFactory();
        op.visit(idFactory);
        IdFactoryConsumer idFactoryConsumer = new IdFactoryConsumer(idFactory);

        assertEquals(3, idFactoryConsumer.idFactory.ids.size());
        idFactoryConsumer.visit((OpUnion) op, 1);
        assertEquals(2, idFactoryConsumer.idFactory.ids.size()); // only remains triple2 and top caller union
        assertEquals(0, idFactory.get((OpUnion) op));
        assertEquals(2, idFactory.get(new OpTriple(SSE.parseTriple(triple2_asString))));
    }

    @Test
    public void two_unions_skips_two() {
        String triple1_asString = "(?s ?p <http://db.uwaterloo.ca/~galuc/wsdbm/Country6>)";
        String triple2_asString = "(<http://db.uwaterloo.ca/~galuc/wsdbm/City0> ?p ?o)";
        String triple3_asString = "(<http://db.uwaterloo.ca/~galuc/wsdbm/City10> ?p ?o)";
        String bgp1_asString = String.format("(bgp %s)", triple1_asString); // 3
        String bgp2_asString = String.format("(bgp %s)", triple2_asString); // 4
        String bgp3_asString = String.format("(bgp %s)", triple3_asString); // 1
        Op op = SSE.parseOp("(union "+ bgp3_asString + " (union "+ bgp1_asString + bgp2_asString + "))"); // 0 and 2

        IdFactory idFactory = new IdFactory();
        op.visit(idFactory);
        IdFactoryConsumer idFactoryConsumer = new IdFactoryConsumer(idFactory);

        assertEquals(5, idFactoryConsumer.idFactory.ids.size());
        idFactoryConsumer.visit((OpUnion) op, 2); // skips tp3, tp1, remove union2
        assertEquals(2, idFactoryConsumer.idFactory.ids.size());
        assertEquals(0, idFactory.get(op));
        assertEquals(4, idFactory.get(new OpTriple(SSE.parseTriple(triple2_asString))));
    }

    @Test
    public void one_union_with_bgp_comprising_union() {
        String triple1_asString = "(?s ?p <http://db.uwaterloo.ca/~galuc/wsdbm/Country6>)";
        String triple2_asString = "(<http://db.uwaterloo.ca/~galuc/wsdbm/City0> ?p ?o)";
        String triple3_asString = "(<http://db.uwaterloo.ca/~galuc/wsdbm/City10> ?p ?o)";
        String triple4_asString = "(<http://db.uwaterloo.ca/~galuc/wsdbm/City42> ?p ?o)";
        String bgp1_asString = String.format("(bgp %s)", triple1_asString); // 3
        String bgp2_asString = String.format("(bgp %s)", triple2_asString); // 4
        String bgp3_asString = String.format("(bgp %s)", triple3_asString); // 1

        Op op = SSE.parseOp(String.format(
                "(union (join (bgp %s)(union (bgp %s)(bgp %s)))(bgp %s))",
                triple1_asString, triple2_asString, triple3_asString, triple4_asString));

        IdFactory idFactory = new IdFactory();
        op.visit(idFactory);
        IdFactoryConsumer idFactoryConsumer = new IdFactoryConsumer(idFactory);

        assertEquals(4+2, idFactory.ids.size());

    }

}