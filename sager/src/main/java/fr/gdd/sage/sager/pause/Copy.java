package fr.gdd.sage.sager.pause;

import fr.gdd.jena.visitors.ReturningOpBaseVisitor;
import fr.gdd.sage.sager.optimizers.Offset2Skip;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.algebra.op.OpTriple;

public class Copy extends ReturningOpBaseVisitor {

    final Offset2Skip loader;

    public Copy(Offset2Skip loader) {
        this.loader = loader;
    }


    @Override
    public Op visit(OpTriple triple) {
        Long offset = loader.getOffset(triple);
        if (offset != 0) {
            return new OpSlice(triple, offset, Long.MIN_VALUE);
        }
        return triple;
    }
}
