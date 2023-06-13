package fr.gdd.sage.arq;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.OpVisitorByType;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.*;

/**
 * Associate with each operator a range of identifiers that it can allocate.
 * Identifiers are useful to save/resume the state of every operator.
 */
public class IdentifierAllocator extends OpVisitorBase {

    HashMap<Op, List<Integer>> op2Id = new HashMap<>();
    HashMap<Integer, Op> id2Op = new HashMap<>();

    Integer current = 1;

    public static void create(ExecutionContext ec, Op op) {
        IdentifierAllocator ids = new IdentifierAllocator();
        op.visit(ids);
        ec.getContext().setIfUndef(SageConstants.identifiers, ids);
    }

    public IdentifierAllocator() {}

    public IdentifierAllocator(Integer start) {
        current = start;
    }

    public Integer getCurrent() {
        return current;
    }

    /* ******************************************************************** */

    @Override
    public void visit(OpTriple opTriple) {
        op2Id.put(opTriple, List.of(current));
        id2Op.put(current, opTriple);
        current += 1;
    }

    @Override
    public void visit(OpQuad opQuad) {
        op2Id.put(opQuad, List.of(current));
        id2Op.put(current, opQuad);
        current += 1;
    }

    @Override
    public void visit(OpBGP opBGP) {
        List<Integer> ids = new ArrayList<>();
        for (int i= 0; i < opBGP.getPattern().size(); ++i) {
            ids.add(current + i);
            id2Op.put(current + i, opBGP);
        }
        op2Id.put(opBGP, ids);
        current += opBGP.getPattern().size();
    }

    @Override
    public void visit(OpQuadPattern quadPattern) {
        List<Integer> ids = new ArrayList<>();
        for (int i= 0; i < quadPattern.getPattern().size(); ++i) {
            ids.add(current + i);
            id2Op.put(current + i, quadPattern);
        }
        op2Id.put(quadPattern, ids);
        current += quadPattern.getPattern().size();
    }

    @Override
    public void visit(OpFilter opFilter) {
        opFilter.getSubOp().visit(this);
    }

    @Override
    public void visit(OpProject opProject) {
        opProject.getSubOp().visit(this);
    }

    @Override
    public void visit(OpLeftJoin opLeftJoin) {
        opLeftJoin.getLeft().visit(this);
        // one id dedicated to optional
        op2Id.put(opLeftJoin, List.of(current));
        id2Op.put(current, opLeftJoin);
        current += 1;
        opLeftJoin.getRight().visit(this);
    }

    @Override
    public void visit(OpConditional opConditional) {
        opConditional.getLeft().visit(this);
        // one id dedicated to optional
        op2Id.put(opConditional, List.of(current));
        id2Op.put(current, opConditional);
        current += 1;
        opConditional.getRight().visit(this);
    }

    @Override
    public void visit(OpJoin opJoin) {
        opJoin.getLeft().visit(this);
        opJoin.getRight().visit(this);
    }

    @Override
    public void visit(OpUnion opUnion) {
        List<Op> flattened = flattenUnion(opUnion);
        op2Id.put(opUnion, List.of(current));  // one id dedicated to union
        id2Op.put(current, opUnion);
        current += 1;
        for (Op subOp : flattened) {
            subOp.visit(this);
        }
    }

    @Override
    public void visit(OpSlice opSlice) {
        opSlice.getSubOp().visit(this);
    }

    /* ************************************************************************************ */

    // Based on code from Olaf Hartig.
    // Comes from {@link OpExecutorTDB}
    public static List<Op> flattenUnion(OpUnion opUnion) {
        List<Op> x = new ArrayList<>();
        flattenUnion(x, opUnion);
        return x;
    }

    public static void flattenUnion(List<Op> acc, OpUnion opUnion) {
        if ( opUnion.getLeft() instanceof OpUnion )
            flattenUnion(acc, (OpUnion)opUnion.getLeft());
        else
            acc.add(opUnion.getLeft());

        if ( opUnion.getRight() instanceof OpUnion )
            flattenUnion(acc, (OpUnion)opUnion.getRight());
        else
            acc.add(opUnion.getRight());
    }

}
