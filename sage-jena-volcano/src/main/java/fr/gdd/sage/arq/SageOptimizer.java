package fr.gdd.sage.arq;

import fr.gdd.sage.generics.LazyIterator;
import fr.gdd.sage.generics.Pair;
import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.jena.JenaBackend;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.dboe.trans.bplustree.PreemptJenaIterator;
import org.apache.jena.dboe.trans.bplustree.ProgressJenaIterator;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.util.VarUtils;
import org.apache.jena.tdb2.solver.PatternMatchSage;
import org.apache.jena.tdb2.store.NodeId;

import java.util.*;
import java.util.stream.Collectors;

/**
 * It reorders graph patterns using a heuristic based on estimated cardinalities.
 * The topest the smallest; candidates patterns are chosen amongst set variables, all if none.
 */
public class SageOptimizer extends TransformCopy {

    JenaBackend backend;
    Dataset dataset;

    SageOptimizer(Dataset dataset) {
        backend = new JenaBackend(dataset);
        this.dataset = dataset;
    }

    @Override
    public Op transform(OpBGP opBGP) {
        List<Pair<Triple, ProgressJenaIterator>> tripleToIt = opBGP.getPattern().getList().stream().map(triple -> {
            NodeId s = triple.getSubject().isVariable() ? backend.any() : backend.getId(triple.getSubject());
            NodeId p = triple.getPredicate().isVariable() ? backend.any() : backend.getId(triple.getPredicate());
            NodeId o = triple.getObject().isVariable() ? backend.any() : backend.getId(triple.getObject());

            BackendIterator<?, ?> it = backend.search(s, p, o);
            ProgressJenaIterator casted = (ProgressJenaIterator) ((LazyIterator<?, ?>) it).iterator;
            return new Pair<>(triple, casted);
        }).sorted((p1, p2) -> { // sort ASC by cardinality
            long c1 = p1.right.cardinality();
            long c2 = p2.right.cardinality();
            return Long.compare(c1, c2);
        }).collect(Collectors.toList());

        List<Triple> triples = new ArrayList<>();
        triples.add(tripleToIt.remove(0).left);

        Set<Var> patternVarsScope = new HashSet<>();
        VarUtils.addVarsFromTriple(patternVarsScope, triples.get(0));

        while (tripleToIt.size() > 0) {
            // #A contains all
            var filtered = tripleToIt.stream().filter(p -> patternVarsScope.containsAll(VarUtils.getVars(p.getLeft()))).toList();
            if (filtered.isEmpty()) {
                // #B contains one
                filtered = tripleToIt.stream().filter(p -> patternVarsScope.stream().anyMatch(v -> VarUtils.getVars(p.getLeft()).contains(v))).toList();
                if (filtered.isEmpty()) {
                    // #C contains none
                    filtered = tripleToIt; // everyone is candidate
                }
            }
            Triple toAdd = filtered.get(0).getLeft();
            tripleToIt = tripleToIt.stream().filter(p -> p.getLeft() != toAdd).collect(Collectors.toList());
            VarUtils.addVarsFromTriple(patternVarsScope, toAdd);
            triples.add(toAdd);
        }


        // List<Triple> triples = tripleToIt.stream().map(Pair::getLeft).collect(Collectors.toList());
        return new OpBGP(BasicPattern.wrap(triples));
    }

}
