package fr.gdd.sage.sager.pause;

import fr.gdd.jena.visitors.ReturningOpVisitor;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.algebra.op.OpUnion;

import java.util.Objects;

public class ShouldPreempt extends ReturningOpVisitor<Boolean> {

    final Save2SPARQL saver;

    public ShouldPreempt(Save2SPARQL saver) {
        this.saver = saver;
    }

    @Override
    public Boolean visit(OpExtend extend) {
        return false;
    }

    @Override
    public Boolean visit(OpTriple triple) {
        return Objects.nonNull(saver.op2it.get(triple));
    }

    @Override
    public Boolean visit(OpJoin join) {
        return ReturningOpVisitorRouter.visit(this, join.getLeft());
    }

    @Override
    public Boolean visit(OpUnion union) {
        return Objects.nonNull(saver.op2it.get(union));
    }
}
