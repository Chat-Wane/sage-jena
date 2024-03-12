package fr.gdd.sage.sager.pause;

import fr.gdd.jena.visitors.ReturningOpVisitor;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.jena.JenaBackend;
import fr.gdd.sage.sager.HashMapWithPtrs;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.SagerOpExecutor;
import fr.gdd.sage.sager.iterators.SagerScan;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Generate a SPARQL query from the paused state.
 */
public class Save2SPARQL extends ReturningOpVisitor<Op> {

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

        if (Objects.isNull(it)) {return null;}

        return new OpSlice(triple, it.offset(), Long.MIN_VALUE);
    }

    @Override
    public Op visit(OpJoin join) {
        FullyPreempted fp = new FullyPreempted(this);
        Op leftFullyPreempt = ReturningOpVisitorRouter.visit(fp, join.getLeft());
        Op right = ReturningOpVisitorRouter.visit(this, join.getRight());
        Op left = ReturningOpVisitorRouter.visit(this, join.getLeft());

        return OpUnion.create(
                distributeJoin(leftFullyPreempt, right), // preempted
                OpJoin.create(left, join.getRight()) // rest
        );
    }

    @Override
    public Op visit(OpUnion union) {
        Op left = ReturningOpVisitorRouter.visit(this, union.getLeft());
        Op right = ReturningOpVisitorRouter.visit(this, union.getRight());
        return OpUnion.create(left, right);
    }

    @Override
    public Op visit(OpExtend extend) {
        return extend;
    }

    /* ************************************************************ */

    public static Op distributeJoin(Op op, Op over) {
        List<Op> ops = SagerOpExecutor.flattenUnion(over);
        return switch (ops.size()) {
            case 0 -> op;
            case 1 -> OpJoin.create(op, over);
            default -> {
                Op left = ops.get(0);
                for (int i = 1; i < ops.size(); ++i) {
                    Op right = OpJoin.create(op, ops.get(i));
                    left = OpUnion.create(left, right);
                }
                yield left;
            }
        };
    }

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
