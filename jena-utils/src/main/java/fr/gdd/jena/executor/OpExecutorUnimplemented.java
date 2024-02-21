package fr.gdd.jena.executor;

import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.main.OpExecutor;

/**
 * An executor that as nothing implemented, and throws when the superclass
 * does not implement a behavior.
 */
public class OpExecutorUnimplemented extends OpExecutor {

    protected OpExecutorUnimplemented(ExecutionContext execCxt) {
        super(execCxt);
    }

    @Override
    protected QueryIterator execute(OpBGP opBGP, QueryIterator input) {
        throw new UnsupportedOperationException("opBGP");
    }

    @Override
    protected QueryIterator execute(OpTriple opTriple, QueryIterator input) {
        throw new UnsupportedOperationException("opTriple");
    }

    @Override
    protected QueryIterator execute(OpGraph opGraph, QueryIterator input) {
        throw new UnsupportedOperationException("opGraph");
    }

    @Override
    protected QueryIterator execute(OpQuad opQuad, QueryIterator input) {
        throw new UnsupportedOperationException("opQuad");
    }

    @Override
    protected QueryIterator execute(OpQuadPattern quadPattern, QueryIterator input) {
        throw new UnsupportedOperationException("opQuadPattern");
    }

    @Override
    protected QueryIterator execute(OpQuadBlock quadBlock, QueryIterator input) {
        throw new UnsupportedOperationException("opQuadBlock");
    }

    @Override
    protected QueryIterator execute(OpPath opPath, QueryIterator input) {
        throw new UnsupportedOperationException("opPath");
    }

    @Override
    protected QueryIterator execute(OpProcedure opProc, QueryIterator input) {
        throw new UnsupportedOperationException("opProc");
    }

    @Override
    protected QueryIterator execute(OpPropFunc opPropFunc, QueryIterator input) {
        throw new UnsupportedOperationException("opPropFunc");
    }

    @Override
    protected QueryIterator execute(OpJoin opJoin, QueryIterator input) {
        throw new UnsupportedOperationException("opJoin");
    }

    @Override
    protected QueryIterator execute(OpSequence opSequence, QueryIterator input) {
        throw new UnsupportedOperationException("opSequence");
    }

    @Override
    protected QueryIterator execute(OpLeftJoin opLeftJoin, QueryIterator input) {
        throw new UnsupportedOperationException("opLeftJoin");
    }

    @Override
    protected QueryIterator execute(OpLateral opLateral, QueryIterator input) {
        throw new UnsupportedOperationException("opLateral");
    }

    @Override
    protected QueryIterator execute(OpConditional opCondition, QueryIterator input) {
        throw new UnsupportedOperationException("opCondition");
    }

    @Override
    protected QueryIterator execute(OpDiff opDiff, QueryIterator input) {
        throw new UnsupportedOperationException("opDiff");
    }

    @Override
    protected QueryIterator execute(OpMinus opMinus, QueryIterator input) {
        throw new UnsupportedOperationException("opMinus");
    }

    @Override
    protected QueryIterator execute(OpDisjunction opDisjunction, QueryIterator input) {
        throw new UnsupportedOperationException("opDisjunction");
    }

    @Override
    protected QueryIterator execute(OpUnion opUnion, QueryIterator input) {
        throw new UnsupportedOperationException("opUnion");
    }

    @Override
    protected QueryIterator execute(OpFilter opFilter, QueryIterator input) {
        throw new UnsupportedOperationException("opFilter");
    }

    @Override
    protected QueryIterator execute(OpService opService, QueryIterator input) {
        throw new UnsupportedOperationException("opService");
    }

    @Override
    protected QueryIterator execute(OpDatasetNames dsNames, QueryIterator input) {
        throw new UnsupportedOperationException("opDatasetNames");
    }

    @Override
    protected QueryIterator execute(OpTable opTable, QueryIterator input) {
        throw new UnsupportedOperationException("opTable");
    }

    @Override
    protected QueryIterator execute(OpExt opExt, QueryIterator input) {
        throw new UnsupportedOperationException("opExt");
    }

    @Override
    protected QueryIterator execute(OpLabel opLabel, QueryIterator input) {
        throw new UnsupportedOperationException("opLabel");
    }

    @Override
    protected QueryIterator execute(OpNull opNull, QueryIterator input) {
        throw new UnsupportedOperationException("opNull");
    }

    @Override
    protected QueryIterator execute(OpList opList, QueryIterator input) {
        throw new UnsupportedOperationException("opList");
    }

    @Override
    protected QueryIterator execute(OpOrder opOrder, QueryIterator input) {
        throw new UnsupportedOperationException("opOrder");
    }

    @Override
    protected QueryIterator execute(OpTopN opTop, QueryIterator input) {
        throw new UnsupportedOperationException("opTop");
    }

    @Override
    protected QueryIterator execute(OpProject opProject, QueryIterator input) {
        throw new UnsupportedOperationException("opProject");
    }

    @Override
    protected QueryIterator execute(OpSlice opSlice, QueryIterator input) {
        throw new UnsupportedOperationException("opSlice");
    }

    @Override
    protected QueryIterator execute(OpGroup opGroup, QueryIterator input) {
        throw new UnsupportedOperationException("opGroup");
    }

    @Override
    protected QueryIterator execute(OpDistinct opDistinct, QueryIterator input) {
        throw new UnsupportedOperationException("opDistinct");
    }

    @Override
    protected QueryIterator execute(OpReduced opReduced, QueryIterator input) {
        throw new UnsupportedOperationException("opReduced");
    }

    @Override
    protected QueryIterator execute(OpAssign opAssign, QueryIterator input) {
        throw new UnsupportedOperationException("opAssign");
    }

    @Override
    protected QueryIterator execute(OpExtend opExtend, QueryIterator input) {
        throw new UnsupportedOperationException("opExtend");
    }

}
