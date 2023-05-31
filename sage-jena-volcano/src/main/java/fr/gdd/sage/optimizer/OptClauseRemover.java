package fr.gdd.sage.optimizer;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;

/**
 * Remove the OPTIONAL clause of request to better find sources that match both
 * mandatory and optional parts. It also aims to improve join ordering in this context.
 */
public class OptClauseRemover extends TransformCopy {

    @Override
    public Op transform(OpLeftJoin opLeftJoin, Op left, Op right) {
        return OpJoin.create(left, right);
    }
}
