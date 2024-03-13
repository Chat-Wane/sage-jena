package fr.gdd.sage.sager.optimizers;

import fr.gdd.jena.visitors.ReturningOpBaseVisitor;
import fr.gdd.sage.sager.HashMapWithPtrs;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Explore the plan and identify the skipping triple pattern when they can.
 */
public class Offset2Skip extends ReturningOpBaseVisitor {

    private static final Logger log = LoggerFactory.getLogger(ReturningOpBaseVisitor.class);

    HashMapWithPtrs<Op, Long> op2offset = new HashMapWithPtrs<>();

    /**
     * @param op A Triple or Quad Op.
     * @return The offset to skip to, 0 by default
     */
    public Long getOffset(Op op) {
        Long offset = op2offset.get(op);
        if (Objects.isNull(offset)) {
            return 0L;
        } else {
            return offset;
        }
    }

    @Override
    public Op visit(OpSlice slice) {
        // TODO check if order_by
        // TODO check if 1 triple pattern only
        if (slice.getSubOp() instanceof OpTriple triple) {
            log.debug("{} should skip {}.", triple.getTriple(), slice.getStart());
            op2offset.put(triple, slice.getStart());
            return triple;
        }
        return super.visit(slice);
    }

}
