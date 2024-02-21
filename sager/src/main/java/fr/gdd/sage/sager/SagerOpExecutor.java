package fr.gdd.sage.sager;

import fr.gdd.jena.executor.OpExecutorUnimplemented;
import fr.gdd.sage.sager.iterators.SagerRoot;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.iterator.QueryIterRoot;
import org.apache.jena.sparql.engine.main.solver.SolverLib;

/**
 * Execute only operators that can be preempted.
 */
public class SagerOpExecutor extends OpExecutorUnimplemented {

    protected SagerOpExecutor(ExecutionContext execCxt) {
        super(execCxt);
    }

    /**
     * @param root The query to execute in the form of a Jena `Op`.
     * @return An iterator that can produce the bindings.
     */
    public QueryIterator execute(Op root) {
        QueryIterator wrapped = this.exec(root, QueryIterRoot.create(execCxt));
        return new SagerRoot(wrapped);
    };

    @Override
    protected QueryIterator execute(OpTriple opTriple, QueryIterator input) {
        Node s = SolverLib.nodeTopLevel(opTriple.getTriple().getSubject());
        Node p = SolverLib.nodeTopLevel(opTriple.getTriple().getPredicate());
        Node o = SolverLib.nodeTopLevel(opTriple.getTriple().getObject());


        return super.execute(opTriple, input);
    }

    // TODO when subquery, identify the pattern

}
