package fr.gdd.sage.sager.pause;

import fr.gdd.jena.visitors.ReturningOpVisitor;
import org.apache.jena.sparql.algebra.op.OpExtend;

public class ShouldPreempt extends ReturningOpVisitor<Boolean> {

    final Save2SPARQL saver;

    public ShouldPreempt(Save2SPARQL saver) {
        this.saver = saver;
    }

    @Override
    public Boolean visit(OpExtend extend) {
        return super.visit(extend); // TODO
    }
}
