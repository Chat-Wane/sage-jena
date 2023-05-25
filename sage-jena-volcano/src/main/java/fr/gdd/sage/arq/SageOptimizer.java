package fr.gdd.sage.arq;

import fr.gdd.sage.generics.LazyIterator;
import fr.gdd.sage.generics.Pair;
import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.jena.JenaBackend;
import org.apache.jena.dboe.trans.bplustree.ProgressJenaIterator;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.algebra.optimize.VariableUsagePusher;
import org.apache.jena.sparql.algebra.optimize.VariableUsageTracker;
import org.apache.jena.sparql.algebra.optimize.VariableUsageVisitor;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.util.VarUtils;
import org.apache.jena.tdb2.store.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * It reorders graph patterns using a heuristic based on estimated cardinalities.
 * The topest the smallest; candidates patterns are chosen amongst set variables, all if none.
 */
public class SageOptimizer extends TransformCopy {
    Logger log = LoggerFactory.getLogger(SageOptimizer.class);

    JenaBackend backend;
    Dataset dataset;

    VariableUsageTracker alreadySetVars = new VariableUsageTracker();

    SageOptimizer(Dataset dataset) {
        backend = new JenaBackend(dataset);
        this.dataset = dataset;
    }

    SageOptimizer(Dataset dataset, VariableUsageTracker vars) {
        backend = new JenaBackend(dataset);
        this.dataset = dataset;
        this.alreadySetVars = vars;
    }

    @Override
    public Op transform(OpLeftJoin opLeftJoin, Op left, Op right) {
        VariableUsageTracker tracker = new VariableUsageTracker();
        var variablesVisitor = new VariableUsagePusher(tracker);
        left.visit(variablesVisitor);
        // We get the variable set on the left side and inform right side
        return OpLeftJoin.create(left, Transformer.transform(new SageOptimizer(dataset, tracker), right), opLeftJoin.getExprs());
    }

    @Override
    public Op transform(OpBGP opBGP) {
        List<Pair<Triple, ProgressJenaIterator>> tripleToIt = opBGP.getPattern().getList().stream().map(triple -> {
            NodeId s = triple.getSubject().isVariable() ? backend.any() : backend.getId(triple.getSubject());
            NodeId p = triple.getPredicate().isVariable() ? backend.any() : backend.getId(triple.getPredicate());
            NodeId o = triple.getObject().isVariable() ? backend.any() : backend.getId(triple.getObject());

            BackendIterator<?, ?> it = backend.search(s, p, o);
            ProgressJenaIterator casted = (ProgressJenaIterator) ((LazyIterator<?, ?>) it).iterator;
            log.debug("triple {} => {} elements", triple, casted.cardinality());
            return new Pair<>(triple, casted);
        }).sorted((p1, p2) -> { // sort ASC by cardinality
            long c1 = p1.right.cardinality();
            long c2 = p2.right.cardinality();
            return Long.compare(c1, c2);
        }).collect(Collectors.toList());

        List<Triple> triples = new ArrayList<>();
        Set<Var> patternVarsScope = new HashSet<>();
        while (tripleToIt.size() > 0) {
            // #A contains at least one variable
            var filtered = tripleToIt.stream().filter(p -> patternVarsScope.stream()
                    .anyMatch(v -> VarUtils.getVars(p.getLeft()).contains(v)) ||
                            VarUtils.getVars(p.getLeft()).stream().anyMatch(v2 -> alreadySetVars.getUsageCount(v2) > 0))
                    .toList();
            if (filtered.isEmpty()) {
                // #B contains none
                filtered = tripleToIt; // everyone is candidate
            }
            Triple toAdd = filtered.get(0).getLeft();
            tripleToIt = tripleToIt.stream().filter(p -> p.getLeft() != toAdd).collect(Collectors.toList());
            VarUtils.addVarsFromTriple(patternVarsScope, toAdd);
            triples.add(toAdd);
        }

        return new OpBGP(BasicPattern.wrap(triples));
    }

}
