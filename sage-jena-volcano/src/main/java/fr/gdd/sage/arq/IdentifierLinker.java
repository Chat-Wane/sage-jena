package fr.gdd.sage.arq;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.OpVisitorByType;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

public class IdentifierLinker extends OpVisitorBase {

    HashMap<Integer, Integer> childToParent = new HashMap<>();

    @Override
    public void visit(OpProject opProject) {
        opProject.visit(this);
    }

    @Override
    public void visit(OpLeftJoin opLeftJoin) {
        GetMostLeftOp visitor = new GetMostLeftOp();
        opLeftJoin.getLeft().visit(visitor);
        Op leftestOp = visitor.result;
        System.out.println(leftestOp);

        chain.add(op2Id.get(leftestOp), op2Id.get(opLeftJoin));
        chain.add(op2Id.get(opLeftJoin), op2Id.get(opLeftJoin.getRight()));
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
    public List<Integer> getParents(Integer child) {
        boolean done = false;
        List<Integer> parents = new ArrayList<>();
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

    /* ******************************************************************* */


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

}
