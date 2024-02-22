package fr.gdd.sage.sager;

import fr.gdd.jena.visitors.ReturningArgsOpVisitor;
import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.sage.jena.JenaBackend;
import fr.gdd.sage.sager.iterators.SagerRoot;
import fr.gdd.sage.sager.iterators.SagerScanFactory;
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

    ExecutionContext execCxt;
    JenaBackend backend;

    public SagerOpExecutor(ExecutionContext execCxt) {
        this.execCxt = execCxt;
        execCxt.getContext().setIfUndef(SagerConstants.BACKEND, new JenaBackend(execCxt.getDataset()));
        backend = execCxt.getContext().get(SagerConstants.BACKEND);
    }

    /**
     * @param root The query to execute in the form of a Jena `Op`.
     * @return An iterator that can produce the bindings.
     */
    public QueryIterator execute(Op root) {
        Iterator<BindingNodeId> wrapped = new SagerRoot(
                ReturningArgsOpVisitorRouter.visit(this, root, Iter.empty()));
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

    protected Iterator<BindingNodeId> execute(OpTriple opTriple, Iterator<BindingNodeId> input) {
        return new SagerScanFactory(input, execCxt, opTriple);
    }

    // TODO when subquery, identify the pattern

}
