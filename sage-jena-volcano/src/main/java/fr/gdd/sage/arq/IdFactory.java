package fr.gdd.sage.arq;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * Explore the tree of operators to assign a unique identifier
 * per element that can save/resume its execution.
 */
public class IdFactory extends OpVisitorBase {

    Logger log = LoggerFactory.getLogger(IdFactory.class);

    TreeMap<String, List<Integer>> ids = new TreeMap<>();
    Integer counter = 0;

    /**
     * Registers the operator uniquely described by `description` so it gets
     * an identifier assigned.
     */
    public Integer register(String description) {
        if (!ids.containsKey(description)) {
            ids.put(description, new ArrayList<>());
        } else {
            log.warn("Collision in description for {}. The order of gets matters now.", description);
        }
        Integer assigned = counter;
        ids.get(description).add(assigned);
        counter += 1;
        return assigned;
    }

    /**
     * Gets the identifier for the operator described by description. There should
     * be exactly one id per registered operator in the query.
     */
    public Integer get(String description) {
        Integer id = ids.get(description).remove(0);
        if (ids.get(description).isEmpty()) {
            ids.remove(description);
        }
        return id;
    }

    public Integer get(Op op) {
        return get(op.toString());
    }

    /* ****************************************************************************************** */

    @Override
    public void visit(OpTriple opTriple) {
        Integer assigned = register(opTriple.toString());
        log.info("{} gets {} assigned as identifier.", opTriple, assigned);
    }

    @Override
    public void visit(OpBGP opBGP) {
        for (Triple triple : opBGP.getPattern().getList()) {
            visit(new OpTriple(triple));
        }
    }

    @Override
    public void visit(OpQuad opQuad) {
        Integer assigned = register(opQuad.toString());
        log.info("{} gets {} assigned as identifier.", opQuad, assigned);
    }

    @Override
    public void visit(OpQuadPattern quadPattern) {
        for (Quad quad : quadPattern.getPattern().getList()) {
            visit(new OpQuad(quad));
        }
    }

    @Override
    public void visit(OpUnion opUnion) {
        log.info("Visited Union "+ opUnion.toString());
        register(opUnion.toString());
        opUnion.getLeft().visit(this);
        opUnion.getRight().visit(this);
    }

    @Override
    public void visit(OpJoin opJoin) {
        opJoin.getLeft().visit(this);
        opJoin.getRight().visit(this);
    }
}
