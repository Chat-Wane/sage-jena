package fr.gdd.sage.sager.optimizers;

import fr.gdd.jena.visitors.ReturningOpBaseVisitor;
import org.apache.jena.query.QueryExecException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpTriple;

public class BGP2Triples extends ReturningOpBaseVisitor {

    @Override
    public Op visit(OpBGP bgp) {
        return switch (bgp.getPattern().getList().size()) {
            case 0 -> throw new QueryExecException();
            case 1 -> new OpTriple(bgp.getPattern().get(0));
            default -> throw new UnsupportedOperationException("bgp > 1");
        };
    }
}
