package fr.gdd.sage.sager;

import fr.gdd.jena.visitors.ReturningArgsOpVisitor;
import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.sage.jena.JenaBackend;
import fr.gdd.sage.sager.iterators.SagerRoot;
import fr.gdd.sage.sager.iterators.SagerScanFactory;
import fr.gdd.sage.sager.optimizers.SagerOptimizer;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import org.apache.jena.tdb2.solver.BindingNodeId;

import java.util.Iterator;

/**
 * Execute only operators that can be preempted. Operators work
 * on `NodeId` by default instead of `Node`, for the sake of performance.
 * That's why it does not extend `OpExecutor` since the latter
 * works on `QueryIterator` that returns `Binding` that provides `Node`.
 */
public class SagerOpExecutor extends ReturningArgsOpVisitor<Iterator<BindingNodeId>, Iterator<BindingNodeId>> {

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
        execCxt.getContext().set(SagerConstants.SAVER, new Save2SPARQL(root));
        Iterator<BindingNodeId> wrapped = new SagerRoot(execCxt,
                ReturningArgsOpVisitorRouter.visit(this, root, Iter.of(BindingNodeId.root)));
        // TODO make them abortable
        return QueryIterPlainWrapper.create(Iter.map(wrapped, bnid -> {
            BindingBuilder builder = BindingFactory.builder();
            for (Var var : bnid) {
                builder.add(var, backend.getNode(bnid.get(var)));
            }
            return builder.build();
        }), execCxt);
    };

    /* ******************************************************************* */

    @Override
    public Iterator<BindingNodeId> visit(OpTriple opTriple, Iterator<BindingNodeId> input) {
        return new SagerScanFactory(input, execCxt, opTriple);
    }

    // TODO when subquery, identify the pattern

}
