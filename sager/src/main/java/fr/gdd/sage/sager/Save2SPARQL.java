package fr.gdd.sage.sager;

import fr.gdd.jena.visitors.ReturningOpBaseVisitor;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.generics.LazyIterator;
import fr.gdd.sage.interfaces.BackendIterator;
import org.apache.jena.dboe.trans.bplustree.ProgressJenaIterator;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.algebra.op.OpTriple;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Save2SPARQL extends ReturningOpBaseVisitor {

    final Op root; // origin
    Op saved; // preempted
    final Map<Op, BackendIterator<?, ?>> op2it = new HashMap<>(); // TODO check pointer's identity.

    public Save2SPARQL(Op root) {
        this.root = root;
    }

    public void register(Op op, BackendIterator<?, ?> it) {
        op2it.put(op, it);
    }
    public void unregister(Op op) {
        op2it.remove(op);
    }

    /* **************************************************************************** */

    public Op save(Op caller) {
        this.saved = ReturningOpVisitorRouter.visit(this, root);
        return this.saved;
    }

    @Override
    public Op visit(OpTriple triple) {
        BackendIterator<?, ?> it = op2it.get(triple);

        if (Objects.nonNull(it)) { // should save it
            ProgressJenaIterator casted = (ProgressJenaIterator) ((LazyIterator) it).iterator; // TODO remove ugly cast
            return new OpSlice(triple, casted.getOffset(), Long.MIN_VALUE);
        }
        // else nothing
        return triple;
    }
}
