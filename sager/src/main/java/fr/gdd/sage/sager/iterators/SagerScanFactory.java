package fr.gdd.sage.sager.iterators;

import fr.gdd.sage.jena.JenaBackend;
import fr.gdd.sage.sager.BindingId2Value;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.optimizers.SagerOptimizer;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.tuple.Tuple3;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.tdb2.store.NodeId;

import java.util.Iterator;
import java.util.Objects;

public class SagerScanFactory implements Iterator<BindingId2Value> {

    JenaBackend backend;
    ExecutionContext context;
    SagerOptimizer loader;

    OpTriple triple;
    Iterator<BindingId2Value> input;
    BindingId2Value binding;

    Iterator<BindingId2Value> instantiated;

    public SagerScanFactory(Iterator<BindingId2Value> input, ExecutionContext context, OpTriple triple) {
        this.input = input;
        this.triple = triple;
        instantiated = Iter.empty();
        backend = context.getContext().get(SagerConstants.BACKEND);
        loader = context.getContext().get(SagerConstants.LOADER);
        this.context = context;
    }

    @Override
    public boolean hasNext() {
        if (!instantiated.hasNext() && !input.hasNext()) {
            return false;
        } else while (!instantiated.hasNext() && input.hasNext()) {
            binding = input.next();
            Tuple3<NodeId> spo = substitute(triple.getTriple(), binding);

            instantiated = new SagerScan(context,
                    triple,
                    backend.search(spo.get(0), spo.get(1), spo.get(2))).skip(loader.getOffset2skip().getOffset(triple));
        }

        return instantiated.hasNext();
    }

    @Override
    public BindingId2Value next() {
        return instantiated.next().setParent(binding);
    }

    private Tuple3<NodeId> substitute(Triple triple, BindingId2Value binding) {
        return TupleFactory.create3(substitute(triple.getSubject(), binding),
                substitute(triple.getPredicate(),binding),
                substitute(triple.getObject(), binding));
    }

    private NodeId substitute(Node sOrPOrO, BindingId2Value binding) {
        if (sOrPOrO.isVariable()) {
            NodeId id = binding.getId(Var.alloc(sOrPOrO));
            return Objects.isNull(id) ? NodeId.NodeIdAny : id;
        } else {
            return backend.getId(sOrPOrO);
        }
    }
}
