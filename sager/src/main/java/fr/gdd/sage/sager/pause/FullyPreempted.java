package fr.gdd.sage.sager.pause;

import fr.gdd.jena.visitors.ReturningOpBaseVisitor;
import fr.gdd.sage.sager.BindingId2Value;
import fr.gdd.sage.sager.iterators.SagerScan;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.util.ExprUtils;

import java.util.Objects;

/**
 * Fully preempted operators. If it cannot be preempted, then it returns null.
 */
public class FullyPreempted extends ReturningOpBaseVisitor {

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
            sequence.add(toBind(node, v));
        }
        return sequence;
    }

    @Override
    public Op visit(OpExtend extend) {
        return extend;
    }

    @Override
    public Op visit(OpSlice slice) {
        if (slice.getSubOp() instanceof OpTriple triple) {
            return this.visit(triple);
        }
        throw new UnsupportedOperationException("TODO normal slice fully preempted."); // TODO
    }

    /* ************************************************************************** */

    public static Op toBind(Node value, Var variable) {
        return OpExtend.extend(OpTable.unit(), variable, ExprUtils.parse(NodeFmtLib.displayStr(value)));
    }
}
