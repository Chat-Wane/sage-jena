package fr.gdd.sage.sager.iterators;

import fr.gdd.sage.sager.BindingId2Value;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprEvalException;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.tdb2.solver.BindingNodeId;

import java.util.Iterator;
import java.util.Objects;

/**
 * We focus on BIND(<http://that_exists_in_db> AS ?variable)
 */
public class SagerBind implements Iterator<BindingId2Value> {

    final Iterator<BindingId2Value> input;
    final ExecutionContext context;
    final VarExprList exprs;

    public SagerBind(Iterator<BindingId2Value> input, OpExtend op, ExecutionContext context) {
        this.exprs =  op.getVarExprList();
        this.context = context;
        this.input = input;
    }

    @Override
    public boolean hasNext() {
        return input.hasNext();
    }

    @Override
    public BindingId2Value next() {
        BindingId2Value current = input.next();
        BindingId2Value b = new BindingId2Value().setParent(current).setDefaultTable(current.getDefaultTable());

        for (Var v : exprs.getVars()) {
            Expr expr = exprs.getExpr(v);
            if (Objects.isNull(expr)) {
                b.put(v, b.getId(v));
            } else {
                try {
                    NodeValue nv = expr.eval(b, context);
                    if (Objects.nonNull(nv))
                        b.put(v, nv.asNode());
                } catch (ExprEvalException ex) {}
            }
        }

        return b;
    }
}
