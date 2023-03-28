package fr.gdd.sage.arq;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Quad;

public class IdFactoryConsumer extends OpVisitorBase {

    IdFactory idFactory;

    public IdFactoryConsumer(IdFactory idFactory) {
        this.idFactory = idFactory;
    }

    @Override
    public void visit(OpTriple opTriple) {
        idFactory.get(opTriple);
    }

    @Override
    public void visit(OpBGP opBGP) {
        for (Triple t : opBGP.getPattern().getList())
            visit(new OpTriple(t));
    }

    @Override
    public void visit(OpQuad opQuad) {
        idFactory.get(opQuad);
    }

    @Override
    public void visit(OpQuadPattern quadPattern) {
        for (Quad q : quadPattern.getPattern().getList())
            visit(new OpQuad(q));
    }

    @Override
    public void visit(OpUnion opUnion) {
        opUnion.getLeft().visit(this);
        opUnion.getRight().visit(this);
    }

    /**
     * opUnion gets flattened. We visit the part that is before the offset.
     */
    public Integer visit(OpUnion opUnion, Integer offset) {
        if (opUnion.getLeft() instanceof OpUnion) {
            offset = visit((OpUnion) opUnion.getLeft(), offset); // -X
            idFactory.get(opUnion.getLeft());
        } else if (offset > 0) {
            opUnion.getLeft().visit(this);
            offset -= 1;
        }

        if (opUnion.getRight() instanceof OpUnion) {
            offset = visit((OpUnion) opUnion.getRight(), offset); // -X
            idFactory.get(opUnion.getRight());
        } else if (offset > 0) {
            opUnion.getRight().visit(this);
            offset -= 1;
        }

        return offset;
    }
}
