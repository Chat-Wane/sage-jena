package fr.gdd.sage.sager.iterators;

import fr.gdd.sage.jena.JenaBackend;
import fr.gdd.sage.sager.SagerConstants;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIterNullIterator;
import org.apache.jena.sparql.engine.iterator.QueryIteratorWrapper;
import org.apache.jena.sparql.engine.main.solver.SolverLib;

public class SagerScanFactory extends QueryIteratorWrapper {

    OpTriple triple;
    QueryIterator instanciated;
    Binding binding;
    JenaBackend backend;

    public SagerScanFactory(QueryIterator qIter, ExecutionContext context, OpTriple triple) {
        super(qIter);
        this.triple = triple;
        instanciated = new QueryIterNullIterator(context);
        backend = context.getContext().get(SagerConstants.BACKEND);
    }

    @Override
    protected boolean hasNextBinding() {
        if (!instanciated.hasNext() && !super.hasNextBinding()) {
            return false;
        } else if (!instanciated.hasNext() && super.hasNextBinding()) {
            while (!instanciated.hasNext() && super.hasNextBinding()) {
                binding = super.nextBinding();
                Triple tPattern = Substitute.substitute(triple.getTriple(), binding);
                Node s = SolverLib.nodeTopLevel(tPattern.getSubject());
                Node p = SolverLib.nodeTopLevel(tPattern.getPredicate());
                Node o = SolverLib.nodeTopLevel(tPattern.getObject());

                backend.search(backend.getId(s), backend.getId(p), backend.getId(o));
            }
        }

        return super.hasNextBinding();
    }

    @Override
    protected Binding moveToNextBinding() {
        return instanciated.nextBinding();
    }
}
