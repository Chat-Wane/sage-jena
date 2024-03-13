package fr.gdd.sage.sager.pause;

import fr.gdd.jena.visitors.ReturningOpVisitor;
import fr.gdd.sage.sager.BindingId2Value;
import fr.gdd.sage.sager.iterators.SagerScan;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.util.ExprUtils;

import java.util.Objects;

/**
 * Fully preempted operators. If it cannot be preempted, then it returns null.
 */
public class FullyPreempted extends ReturningOpVisitor<Op> {

    final Save2SPARQL saver;

    public FullyPreempted(Save2SPARQL saver) {
        this.saver = saver;
    }

    @Override
    public Op visit(OpTriple triple) {
        SagerScan it = (SagerScan) saver.op2it.get(triple);

        if (Objects.isNull(it)) return null;

        BindingId2Value lastBinding = it.current();
        OpSequence sequence = OpSequence.create();
        for (Var v : lastBinding) {
            Node node = lastBinding.getValue(v);
            String nodeAsString = NodeFmtLib.displayStr(node);
            Expr asExpr = ExprUtils.parse(nodeAsString);
            sequence.add(OpExtend.extend(OpTable.unit(), v, asExpr));
        }
        return sequence;
    }

    @Override
    public Op visit(OpExtend extend) {
        return extend;
    }
}
