package fr.gdd.sage.sager;

import fr.gdd.sage.interfaces.BackendIterator;
import org.apache.jena.sparql.algebra.Op;

import java.util.HashMap;
import java.util.Map;

public class Save2SPARQL {

    Map<Op, BackendIterator<?, ?>> op2it = new HashMap<>(); // TODO check pointer identity.

    public void register(Op op, BackendIterator<?, ?> it) {
        op2it.put(op, it);
    }

    public void unregister(Op op) {
        op2it.remove(op);
    }

    // TODO implement the visitor

}
