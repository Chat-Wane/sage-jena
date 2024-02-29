package fr.gdd.sage.sager;

import fr.gdd.jena.visitors.ReturningOpVisitor;
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

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Generate a SPARQL query from the paused state.
 */
public class Save2SPARQL extends ReturningOpVisitor<List<Op>> {

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
        this.saved = unionize(ReturningOpVisitorRouter.visit(this, root));
        return this.saved;
    }

    @Override
    public List<Op> visit(OpTriple triple) {
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
            return List.of(sequence);
        } else if (Objects.nonNull(it)) { // should save it
            return List.of(new OpSlice(triple, it.offset(), Long.MIN_VALUE));
        }
        // else nothing
        return List.of(triple);
    }


    /* ************************************************************ */

    public static Op unionize(List<Op> ops) {
        return switch (ops.size()) {
            case 0 -> null;
            case 1 -> ops.get(0);
            default -> {
                Op left = ops.get(0);
                for (int i = 1; i < ops.size(); ++i) {
                    Op right = ops.get(i);
                    left = OpUnion.create(left, right);
                }
                yield left;
            }
        };
    }
}
