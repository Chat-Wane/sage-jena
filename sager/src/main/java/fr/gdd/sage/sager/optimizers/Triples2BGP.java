package fr.gdd.sage.sager.optimizers;

import fr.gdd.jena.visitors.ReturningOpBaseVisitor;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.BasicPattern;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converse operation of {@link BGP2Triples}, just so it looks better.
 */
public class Triples2BGP extends ReturningOpBaseVisitor {

    @Override
    public Op visit(OpJoin join) {
        List<Op> ops = flattenJoin(join);
        List<Triple> triples = ops.stream().filter(o -> o instanceof OpTriple)
                .map(o -> ((OpTriple) o).getTriple()).toList();
        List<Op> rest = ops.stream().filter(o -> ! (o instanceof OpTriple)).toList();
        OpBGP bgp = new OpBGP(BasicPattern.wrap(triples));
        if (!triples.isEmpty() && !rest.isEmpty()) {
            return OpJoin.create(bgp, BGP2Triples.asJoins(rest));
        } else if (!triples.isEmpty()){
            return bgp;
        } else {
            return BGP2Triples.asJoins(rest);
        }
    }

    /* *************************************************************** */

    public static List<Op> flattenJoin(OpJoin join) {
        List<Op> result = new ArrayList<>();
        if (join.getLeft() instanceof OpJoin left) {
            result.addAll(flattenJoin(left));
        } else {
            result.add(join.getLeft());
        }
        if (join.getRight() instanceof OpJoin right) {
            result.addAll(flattenJoin(right));
        } else {
            result.add(join.getRight());
        }
        return result;
    }

}
