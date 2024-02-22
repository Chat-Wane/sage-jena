package fr.gdd.sage.sager;

import fr.gdd.jena.visitors.ReturningOpBaseVisitor;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.interfaces.BackendIterator;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpTriple;

import java.util.HashMap;
import java.util.Map;

public class Save2SPARQL extends ReturningOpBaseVisitor {

    final Op root; // origin
    Op saved; // preempted
    final Map<Op, BackendIterator<?, ?>> op2it = new HashMap<>(); // TODO check pointer identity.

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
        // TODO TODO TODO
        return super.visit(triple);
    }
}
