package fr.gdd.sage.sager;

import fr.gdd.jena.visitors.ReturningArgsOpVisitor;
import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.sage.jena.JenaBackend;
import fr.gdd.sage.sager.iterators.SagerBind;
import fr.gdd.sage.sager.iterators.SagerRoot;
import fr.gdd.sage.sager.iterators.SagerScanFactory;
import fr.gdd.sage.sager.optimizers.SagerOptimizer;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import org.apache.jena.tdb2.solver.BindingNodeId;
import org.apache.jena.tdb2.solver.BindingTDB;

import java.util.Iterator;

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
    public Iterator<BindingId2Value> visit(OpExtend extend, Iterator<BindingId2Value> input) {
        Iterator<BindingId2Value> newInput = ReturningArgsOpVisitorRouter.visit(this, extend.getSubOp(), input);
        return new SagerBind(newInput, extend, execCxt);
    }

    @Override
    public Iterator<BindingId2Value> visit(OpTable table, Iterator<BindingId2Value> input) {
        if (table.isJoinIdentity())
            return input;
        throw new UnsupportedOperationException("TODO: Should be considered as a Scan iterator…"); // TODO
    }
}
