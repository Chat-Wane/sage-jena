package fr.gdd.sage.arq;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.OpVisitorByType;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.*;

public class IdentifierLinker extends OpVisitorBase {

    HashMap<Integer, Integer> childToParent = new HashMap<>();
    IdentifierAllocator identifiers;

    GetMostLeftOp getLeftest = new GetMostLeftOp();
    GetMostRightOp getRightest = new GetMostRightOp();

    public static void create(ExecutionContext ec, Op op, boolean... force) {
        if (Objects.isNull(force) || force.length == 0 || !force[0]) {
            ec.getContext().setIfUndef(SageConstants.identifiers, new IdentifierLinker(op));
        } else {
            ec.getContext().set(SageConstants.identifiers, new IdentifierLinker(op));
        }
    }

    public IdentifierLinker(Op op) {
        this.identifiers = new IdentifierAllocator();
        op.visit(this.identifiers);
        op.visit(this);
    }

    public List<Integer> getIds(Op op) {
        return this.identifiers.getIds(op);
    }

    /* ******************************************************************* */

    @Override
    public void visit(OpProject opProject) {
        opProject.visit(this);
    }

    @Override
    public void visit(OpSlice opSlice) {
        // (TODO) should have an identifier to save
        opSlice.getSubOp().visit(this);
    }

    @Override
    public void visit(OpLeftJoin opLeftJoin) {
        opLeftJoin.getLeft().visit(getRightest);
        Op rightestOfLeftOp = getRightest.result;

        opLeftJoin.getRight().visit(getLeftest);
        Op leftestOfRightOp = getLeftest.result;

        // Integer idLeftest = identifiers.op2Id.get(leftestOp).stream().max(Integer::compare).orElseThrow();
        Integer idRightestOfLeft = identifiers.op2Id.get(rightestOfLeftOp).stream().max(Integer::compare).orElseThrow();
        Integer idLeftestOfRight = identifiers.op2Id.get(leftestOfRightOp).stream().min(Integer::compare).orElseThrow();
        Integer idOptional = identifiers.op2Id.get(opLeftJoin).get(0);

        add(idRightestOfLeft, idOptional);
        add(idOptional, idLeftestOfRight);

        opLeftJoin.getLeft().visit(this);
        opLeftJoin.getRight().visit(this);
    }

    @Override
    public void visit(OpConditional opCond) {
        opCond.getLeft().visit(getRightest);
        Op rightestOfLeftOp = getRightest.result;

        opCond.getRight().visit(getLeftest);
        Op leftestOfRightOp = getLeftest.result;

        // Integer idLeftest = identifiers.op2Id.get(leftestOp).stream().max(Integer::compare).orElseThrow();
        Integer idRightestOfLeft = identifiers.op2Id.get(rightestOfLeftOp).stream().max(Integer::compare).orElseThrow();
        Integer idLeftestOfRight = identifiers.op2Id.get(leftestOfRightOp).stream().min(Integer::compare).orElseThrow();
        Integer idOptional = identifiers.op2Id.get(opCond).get(0);

        add(idRightestOfLeft, idOptional);
        add(idOptional, idLeftestOfRight);

        opCond.getLeft().visit(this);
        opCond.getRight().visit(this);
    }

    @Override
    public void visit(OpJoin opJoin) {
        GetMostLeftOp visitor = new GetMostLeftOp();
        opJoin.getLeft().visit(visitor);
        Op leftestOp = visitor.result;

        opJoin.getRight().visit(visitor);
        Op leftestOfRightOp = visitor.result;

        Integer idLeftest = identifiers.op2Id.get(leftestOp).stream().max(Integer::compare).orElseThrow();
        Integer idLeftestOfRight = identifiers.op2Id.get(leftestOfRightOp).stream().min(Integer::compare).orElseThrow();

        add(idLeftest, idLeftestOfRight);

        opJoin.getLeft().visit(this);
        opJoin.getRight().visit(this);
    }

    @Override
    public void visit(OpBGP opBGP) {
        List<Integer> bgpIds = identifiers.op2Id.get(opBGP);
        for (int i = 0; i < bgpIds.size() - 1; ++i) {
            add(bgpIds.get(i), bgpIds.get(i+1));
        }
    }

    @Override
    public void visit(OpQuadPattern opQuad) {
        List<Integer> quadIds = identifiers.op2Id.get(opQuad);
        for (int i = 0; i < quadIds.size() - 1; ++i) {
            add(quadIds.get(i), quadIds.get(i+1));
        }
    }

    /* ***************************************************************************** */

    /**
     * State that the iterator child is the child of parent.
     * @param parent The identifier of the parent iterator.
     * @param child The child iterator.
     */
    public void add(Integer parent, Integer child) {
        childToParent.put(child, parent);
    }

    /**
     * @param child The unique identifier of the iterator
     * @return A list of parents of the iterator.
     */
    public Set<Integer> getParents(Integer child) {
        boolean done = false;
        Set<Integer> parents = new TreeSet<>();
        while (!done) {
            Integer parent = childToParent.get(child);
            if (Objects.nonNull(parent)) {
                parents.add(parent);
                child = parent;
            } else {
                done = true;
            }
        }
        return parents;
    }

    public boolean inRightSideOf(Integer parent, Integer child) {
        Op parentOp = identifiers.id2Op.get(parent);

        if (!(parentOp instanceof OpN || parentOp instanceof Op2 || parentOp instanceof OpLeftJoin)) {
            return false;
        }

        if (!getParents(child).contains(parent)) {
            return false;
        }

        Op2 parentOp2 = (Op2) parentOp;

        Op childOp = identifiers.id2Op.get(child);
        IsIncludedIn visitor = new IsIncludedIn(childOp);
        parentOp2.getRight().visit(visitor);
        return visitor.result;
    }

    /* ******************************************************************* */

    static class IsIncludedIn extends OpVisitorByType {

        public boolean result = false;
        public Op toFind;

        public IsIncludedIn(Op toFind) {
            this.toFind = toFind;
        }

        @Override
        protected void visitN(OpN op) {
            int i = 0;
            while (!result && i < op.size()) {
                op.get(i).visit(this);
                ++i;
            }
        }

        @Override
        protected void visit2(Op2 op) {
            if (!result) op.getLeft().visit(this);
            if (!result) op.getRight().visit(this);
        }

        @Override
        protected void visit1(Op1 op) {
            result = result || op == toFind;
            if (!result) op.getSubOp().visit(this);

        }

        @Override
        protected void visit0(Op0 op) {
            result = result || op == toFind;
        }

        @Override
        protected void visitFilter(OpFilter op) {
            op.getSubOp().visit(this);
        }

        @Override
        protected void visitLeftJoin(OpLeftJoin op) {
            op.getLeft().visit(this);
            if (!result) op.getRight().visit(this);
        }
    }

    static class GetMostLeftOp extends OpVisitorByType {

        public Op result;

        @Override
        protected void visitN(OpN op) {
            op.get(0).visit(this);
        }

        @Override
        protected void visit2(Op2 op) {
            op.getLeft().visit(this);
        }

        @Override
        protected void visit1(Op1 op) {
            op.getSubOp().visit(this);
        }

        @Override
        protected void visit0(Op0 op) {
            result = op;
        }

        @Override
        protected void visitFilter(OpFilter op) {
            op.getSubOp().visit(this);
        }

        @Override
        protected void visitLeftJoin(OpLeftJoin op) {
            op.getLeft().visit(this);
        }
    }

    static class GetMostRightOp extends OpVisitorByType {

        public Op result;

        @Override
        protected void visitN(OpN op) {
            op.get(op.size()-1).visit(this);
        }

        @Override
        protected void visit2(Op2 op) {
            op.getRight().visit(this);
        }

        @Override
        protected void visit1(Op1 op) {
            op.getSubOp().visit(this);
        }

        @Override
        protected void visit0(Op0 op) {
            result = op;
        }

        @Override
        protected void visitFilter(OpFilter op) {
            op.getSubOp().visit(this);
        }

        @Override
        protected void visitLeftJoin(OpLeftJoin op) {
            op.getRight().visit(this);
        }
    }

}
