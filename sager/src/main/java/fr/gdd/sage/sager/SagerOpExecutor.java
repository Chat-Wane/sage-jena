package fr.gdd.sage.sager;

import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.jena.visitors.ReturningArgsOpVisitor;
import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.jena.JenaBackend;
import fr.gdd.sage.sager.iterators.SagerBind;
import fr.gdd.sage.sager.iterators.SagerRoot;
import fr.gdd.sage.sager.iterators.SagerScanFactory;
import fr.gdd.sage.sager.iterators.SagerUnion;
import fr.gdd.sage.sager.optimizers.SagerOptimizer;
import fr.gdd.sage.sager.pause.Save2SPARQL;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Execute only operators that can be preempted. Operators work
 * on `NodeId` by default instead of `Node`, for the sake of performance.
 * That's why it does not extend `OpExecutor` since the latter
 * works on `QueryIterator` that returns `Binding` that provides `Node`.
 */
public class SagerOpExecutor extends ReturningArgsOpVisitor<Iterator<BindingId2Value>, Iterator<BindingId2Value>> {

    final ExecutionContext execCxt;
    final JenaBackend backend;

    public SagerOpExecutor(ExecutionContext execCxt) {
        this.execCxt = execCxt;
        execCxt.getContext().setIfUndef(SagerConstants.BACKEND, new JenaBackend(execCxt.getDataset()));
        backend = execCxt.getContext().get(SagerConstants.BACKEND);
        execCxt.getContext().setIfUndef(SagerConstants.LOADER, new SagerOptimizer());
    }

    /**
     * @param root The query to execute in the form of a Jena `Op`.
     * @return An iterator that can produce the bindings.
     */
    public QueryIterator optimizeThenExecute(Op root) {
        SagerOptimizer optimizer = execCxt.getContext().get(SagerConstants.LOADER);
        root = optimizer.optimize(root);
        return this.execute(root);
    }

    public QueryIterator execute(Op root) {
        execCxt.getContext().set(SagerConstants.SAVER, new Save2SPARQL(root, execCxt));

        Iterator<BindingId2Value> wrapped = new SagerRoot(execCxt,
                ReturningArgsOpVisitorRouter.visit(this, root, Iter.of(new BindingId2Value())));
        // TODO make them abortable
        return QueryIterPlainWrapper.create(Iter.map(wrapped, bnid -> {
            BindingBuilder builder = BindingFactory.builder();
            for (Var var : bnid) {
                builder.add(var, bnid.getValue(var));
            }
            return builder.build();
        }), execCxt);
    };

    /* ******************************************************************* */

    @Override
    public Iterator<BindingId2Value> visit(OpTriple opTriple, Iterator<BindingId2Value> input) {
        return new SagerScanFactory(input, execCxt, opTriple);
    }

    @Override
    public Iterator<BindingId2Value> visit(OpSequence sequence, Iterator<BindingId2Value> input) {
        for (Op op : sequence.getElements()) {
            input = ReturningArgsOpVisitorRouter.visit(this, op, input);
        }
        return input;
    }

    @Override
    public Iterator<BindingId2Value> visit(OpJoin join, Iterator<BindingId2Value> input) {
        input = ReturningArgsOpVisitorRouter.visit(this, join.getLeft(), input);
        return ReturningArgsOpVisitorRouter.visit(this, join.getRight(), input);
    }

    @Override
    public Iterator<BindingId2Value> visit(OpUnion union, Iterator<BindingId2Value> input) {
        // TODO What about some parallelism here? :)
        Save2SPARQL saver = execCxt.getContext().get(SagerConstants.SAVER);
        SagerUnion iterator = new SagerUnion(this, input, union.getLeft(), union.getRight());
        saver.register(union, iterator);
        return iterator;
    }

    @Override
    public Iterator<BindingId2Value> visit(OpExtend extend, Iterator<BindingId2Value> input) {
        Iterator<BindingId2Value> newInput = ReturningArgsOpVisitorRouter.visit(this, extend.getSubOp(), input);
        return new SagerBind(newInput, extend, execCxt);
    }

    @Override
    public Iterator<BindingId2Value> visit(OpTable table, Iterator<BindingId2Value> input) {
        if (table.isJoinIdentity())
            return input;
        throw new UnsupportedOperationException("TODO: VALUES Should be considered as a Scan iterator…"); // TODO
    }

    /**
     * Preemption mostly comes from this: the ability to start over from an OFFSET efficiently.
     * When we find a pattern like SELECT * WHERE {?s ?p ?o} OFFSET X, the engine know that
     * it must skip X elements of the iterator. But the pattern must be accurate: a single
     * triple pattern.
     */
    @Override
    public Iterator<BindingId2Value> visit(OpSlice slice, Iterator<BindingId2Value> input) {
        if (slice.getSubOp() instanceof OpTriple triple) { // TODO OpQuad
            return new SagerScanFactory(input, execCxt, triple, slice.getStart());
        }
        // TODO otherwise it's a normal slice
        throw new UnsupportedOperationException("TODO Default LIMIT OFFSET not implemented yet.");
    }

    /* **************************************************************************** */

    /**
     * @param op The operator to visit.
     * @return The list of operators that are under the (nested) unions.
     */
    public static List<Op> flattenUnion(Op op) {
        return switch (op) {
            case OpUnion u -> {
                List<Op> ops = new ArrayList<>();
                ops.addAll(flattenUnion(u.getLeft()));
                ops.addAll(flattenUnion(u.getRight()));
                yield ops;
            }
            case null -> List.of();
            default -> List.of(op);
        };
    }

    /**
     * @param op The operator to visit.
     * @return The list of operators that are under the (nested) joins.
     */
    public static List<Op> flattenJoin(Op op) {
        return switch (op) {
            case OpJoin j -> {
                List<Op> ops = new ArrayList<>();
                ops.addAll(flattenJoin(j.getLeft()));
                ops.addAll(flattenJoin(j.getRight()));
                yield ops;
            }
            case OpExtend e -> {
                List<Op> ops = new ArrayList<>();
                ops.add(OpCloningUtil.clone(e, OpTable.unit()));
                ops.addAll(flattenJoin(e.getSubOp()));
                yield ops;
            }
            case null -> List.of();
            default -> List.of(op);
        };
    }

}
