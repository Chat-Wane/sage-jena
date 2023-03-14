package fr.gdd.sage;

import fr.gdd.sage.arq.OpExecutorSage;
import fr.gdd.sage.configuration.SageServerConfiguration;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpQuadPattern;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.mgt.Explain;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.tdb2.solver.OpExecutorTDB2;
import org.apache.jena.tdb2.solver.PatternMatchTDB2;
import org.apache.jena.tdb2.solver.QC2;
import org.apache.jena.tdb2.solver.QueryEngineTDB;
import org.apache.jena.tdb2.store.DatasetGraphTDB;
import org.apache.jena.tdb2.store.GraphTDB;
import org.apache.jena.tdb2.store.NodeId;

import java.util.function.Predicate;

public class OpExecutorTDB2ForceOrder extends OpExecutor {
    Predicate<Tuple<NodeId>> filter = null;

    public OpExecutorTDB2ForceOrder(ExecutionContext execCxt) {
        super(execCxt);
        filter = QC2.getFilter(execCxt.getContext());
    }

    public static class OpExecutorTDB2ForceOrderFactory implements OpExecutorFactory {
        @Override
        public OpExecutor create(ExecutionContext context) {
            return new OpExecutorTDB2ForceOrder(context);
        }
    }

    @Override
    public QueryIterator execute(OpBGP opBGP, QueryIterator input)
    {
        Graph g = execCxt.getActiveGraph();

        if ( g instanceof GraphTDB)
        {
            BasicPattern bgp = opBGP.getPattern();
            Explain.explain("Execute", bgp, execCxt.getContext());
            // Triple-backed (but may be named as explicit default graph).
            GraphTDB gtdb = (GraphTDB)g;
            Node gn = OpExecutorTDB2.decideGraphNode(gtdb.getGraphName(), execCxt);
            return PatternMatchTDB2.execute(gtdb.getDSG(), gn, bgp, input, filter, execCxt);
        }
        Log.warn(this, "Non-GraphTDB passed to OpExecutorPlainTDB: "+g.getClass().getSimpleName());
        return super.execute(opBGP, input);
    }

    @Override
    public QueryIterator execute(OpQuadPattern opQuadPattern, QueryIterator input)
    {
        Node gn = opQuadPattern.getGraphNode();
        gn = OpExecutorTDB2.decideGraphNode(gn, execCxt);

        if ( execCxt.getDataset() instanceof DatasetGraphTDB )
        {
            DatasetGraphTDB ds = (DatasetGraphTDB)execCxt.getDataset();
            Explain.explain("Execute", opQuadPattern.getPattern(), execCxt.getContext());
            BasicPattern bgp = opQuadPattern.getBasicPattern();
            return PatternMatchTDB2.execute(ds, gn, bgp, input, filter, execCxt);
        }
        // Maybe a TDB named graph inside a non-TDB dataset.
        Graph g = execCxt.getActiveGraph();
        if ( g instanceof GraphTDB )
        {
            // Triples graph from TDB (which is the default graph of the dataset),
            // used a named graph in a composite dataset.
            BasicPattern bgp = opQuadPattern.getBasicPattern();
            Explain.explain("Execute", bgp, execCxt.getContext());
            // Don't pass in G -- gn may be different.
            return PatternMatchTDB2.execute(((GraphTDB)g).getDSG(), gn, bgp, input, filter, execCxt);
        }
        Log.warn(this, "Non-DatasetGraphTDB passed to OpExecutorPlainTDB");
        return super.execute(opQuadPattern, input);
    }
}

