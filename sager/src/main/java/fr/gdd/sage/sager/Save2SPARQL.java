package fr.gdd.sage.sager;

import fr.gdd.jena.visitors.ReturningOpBaseVisitor;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.jena.JenaBackend;
import fr.gdd.sage.sager.iterators.SagerScan;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.util.ExprUtils;
import org.apache.jena.tdb2.solver.BindingNodeId;

import java.util.Objects;

/**
 * Generate a SPARQL query from the paused state.
 */
public class Save2SPARQL extends ReturningOpBaseVisitor {

    final JenaBackend backend;
    final Op root; // origin
    Op saved; // preempted
    Op caller;
    final HashMapWithPtrs<Op, SagerScan> op2it = new HashMapWithPtrs<>(); // TODO check pointer's identity.

    public Save2SPARQL(Op root, ExecutionContext context) {
        this.root = root;
        this.backend = context.getContext().get(SagerConstants.BACKEND);
    }

    public void register(Op op, SagerScan it) {op2it.put(op, it);}
    public void unregister(Op op) {op2it.remove(op);}

    /* **************************************************************************** */

    public Op save(Op caller) {
        this.caller = caller;
        this.saved = ReturningOpVisitorRouter.visit(this, root);
        return this.saved;
    }

    @Override
    public Op visit(OpTriple triple) {
        SagerScan it = op2it.get(triple);

        // TODO remove op2it.size() > 1 which is to experiment when the last throws
        // TODO but it does not work always || OR MAYBE IT DOES ?
        if (Objects.nonNull(it) && op2it.size() > 1) {
            BindingId2Value lastBinding = it.current();
            op2it.remove(triple);
            OpSequence sequence = OpSequence.create();
            for (Var v : lastBinding) {
                Node node = lastBinding.getValue(v);
                String nodeAsString = NodeFmtLib.displayStr(node);
                Expr asExpr = ExprUtils.parse(nodeAsString);
                sequence.add(OpExtend.extend(OpTable.unit(), v, asExpr));
            }
            return sequence;
        } else if (Objects.nonNull(it)) { // should save it
            return new OpSlice(triple, it.offset(), Long.MIN_VALUE);
        }
        // else nothing
        return triple;
    }

}
