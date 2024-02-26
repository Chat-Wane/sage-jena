package fr.gdd.sage.sager.iterators;

import fr.gdd.sage.jena.JenaBackend;
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
import org.apache.jena.tdb2.solver.BindingNodeId;
import org.apache.jena.tdb2.store.NodeId;

import java.util.Iterator;
import java.util.Objects;

public class SagerScanFactory implements Iterator<BindingNodeId> {

    JenaBackend backend;
    ExecutionContext context;
    SagerOptimizer loader;

    OpTriple triple;
    Iterator<BindingNodeId> input;
    BindingNodeId binding;

    Iterator<BindingNodeId> instantiated;

    public SagerScanFactory(Iterator<BindingNodeId> input, ExecutionContext context, OpTriple triple) {
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
    public BindingNodeId next() {
        BindingNodeId copy = new BindingNodeId(binding);
        BindingNodeId newInstantiated = instantiated.next();
        newInstantiated.forEach((bnid) -> {
            if (!copy.containsKey(bnid)) { // ugly, maybe there is a better way to merge
                copy.put(bnid, newInstantiated.get(bnid));
            }
        });
        return copy;
    }

    private Tuple3<NodeId> substitute(Triple triple, BindingNodeId binding) {
        return TupleFactory.create3(substitute(triple.getSubject(), binding),
                substitute(triple.getPredicate(),binding),
                substitute(triple.getObject(), binding));
    }

    private NodeId substitute(Node sOrPOrO, BindingNodeId binding) {
        if (sOrPOrO.isVariable()) {
            NodeId id = binding.get(Var.alloc(sOrPOrO));
            return Objects.isNull(id) ? NodeId.NodeIdAny : id;
        } else {
            return backend.getId(sOrPOrO);
        }
    }
}
