package fr.gdd.sage.arq;

import fr.gdd.sage.ReflectionUtils;
import fr.gdd.sage.configuration.SageInputBuilder;
import fr.gdd.sage.configuration.SageServerConfiguration;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.iterator.RandomQueryIterUnion;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.mgt.Explain;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.tdb2.solver.OpExecutorTDB2;
import org.apache.jena.tdb2.solver.PatternMatchSage;
import org.apache.jena.tdb2.solver.PatternMatchTDB2;
import org.apache.jena.tdb2.solver.QC2;
import org.apache.jena.tdb2.store.DatasetGraphTDB;
import org.apache.jena.tdb2.store.GraphTDB;
import org.apache.jena.tdb2.store.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;


/**
 * Some operators need rewriting to enable pausing/resuming their
 * operation.
 **/
public class OpExecutorSage extends OpExecutorTDB2 {
    static Logger log = LoggerFactory.getLogger(OpExecutorSage.class);
    
    SageOutput<?> output; // where pausing state is saved when need be.
    public Map<Integer, VolcanoIteratorQuad> iterators; // all iterators that may need saving

    /**
     * Factory to be registered in Jena ARQ. It creates an OpExecutor for
     * Sage in charge of operations customized for pausing/resuming
     * queries.
     */
    public static class OpExecutorSageFactory implements OpExecutorFactory {
        SageServerConfiguration configuration;

        public OpExecutorSageFactory(Context context) {
            configuration = new SageServerConfiguration(context);

            // Modify the plain factory so it creates our own preemptable iterators when need be.
            // Since its a `static` field, it globally modifies the default factory of `TDB2`.
            // Using Sage AND TDB2 in a same thread could prove difficult for now.
            Field plainFactoryField = ReflectionUtils._getField(OpExecutorTDB2.class, "plainFactory");
            try {
                plainFactoryField.set(this, new OpExecutorPlainFactorySage());
            } catch (Exception e) {
                e.printStackTrace();
            }


            // Method m = ReflectionUtils._getMethod(
        }

        @Override
        public OpExecutor create(ExecutionContext context) {
            return new OpExecutorSage(context, configuration);
        }
    }


    OpExecutorSage(ExecutionContext context, SageServerConfiguration configuration) {
        super(context);
        
        SageInput<?> input = new SageInputBuilder()
            .globalConfig(configuration)
            .localInput(context.getContext().get(SageConstants.input))
            .build();
        
        this.output = new SageOutput<>();
        this.iterators = new TreeMap<>();
        
        execCxt.getContext().set(SageConstants.output, output);
        execCxt.getContext().set(SageConstants.input, input);
        execCxt.getContext().set(SageConstants.iterators, iterators);
        execCxt.getContext().set(SageConstants.scanFactory, new VolcanoIteratorFactory(execCxt));

    }

    @Override
    protected QueryIterator execute(OpBGP opBGP, QueryIterator input) {
        log.info("Executing a BGP…");
        if (execCxt.getContext().isFalse(ARQ.optimization)) { // force order
            return PatternMatchSage.matchTriplePattern(opBGP.getPattern(), input, execCxt);
        } else { // order of TDB2
            return super.execute(opBGP, input);
        }
    }
    
    @Override
    protected QueryIterator execute(OpTriple opTriple, QueryIterator input) {
        log.info("Executing a triple…");
        return PatternMatchSage.matchTriplePattern(opTriple.asBGP().getPattern(), input, execCxt);
    }
    
    @Override
    protected QueryIterator execute(OpQuadPattern quadPattern, QueryIterator input) {
        log.info("Executing a quad…");
        if (execCxt.getContext().isFalse(ARQ.optimization)) { // force order
            return PatternMatchSage.matchQuadPattern(quadPattern.getBasicPattern(), quadPattern.getGraphNode(), input, execCxt);
        } else { // order of TDB2
            return super.execute(quadPattern, input);
        }
    }

    @Override
    public QueryIterator execute(OpUnion union, QueryIterator input) {
        log.info("Executing a union");
        // (TODO) maybe RandomOpExecutor would be more appropriate.
        SageInput sageInput = execCxt.getContext().get(SageConstants.input);
        if (!sageInput.isRandomWalking()) {
            return super.execute(union, input);
        }
        // copy from `OpExecutor`
        List<Op> x = flattenUnion(union);
        QueryIterator cIter = new RandomQueryIterUnion(input, x, execCxt);
        return cIter;
    }

    /**
     * This is a copy/pasta of the inner factory classes of {@link OpExecutorTDB2}.
     * The only difference with the original is the use of a different {@link PatternMatchTDB2}
     * that will create {@link VolcanoIteratorQuad} that enable pausing/resuming of query execution.
     **/
    private static class OpExecutorPlainFactorySage implements OpExecutorFactory {
        @Override
        public OpExecutor create(ExecutionContext execCxt) {return new OpExecutorPlainSage(execCxt);}
    }

    private static class OpExecutorPlainSage extends OpExecutor {
        Predicate<Tuple<NodeId>> filter = null; // (TODO) filter

        public OpExecutorPlainSage(ExecutionContext execCxt) {
            super(execCxt);
            filter = QC2.getFilter(execCxt.getContext());
        }

        @Override
        public QueryIterator execute(OpBGP opBGP, QueryIterator input) {
            Graph g = execCxt.getActiveGraph();

            if ( g instanceof GraphTDB) {
                BasicPattern bgp = opBGP.getPattern();
                Explain.explain("Execute", bgp, execCxt.getContext());
                // Triple-backed (but may be named as explicit default graph).
                GraphTDB gtdb = (GraphTDB)g;
                Node gn = decideGraphNode(gtdb.getGraphName(), execCxt);
                // return PatternMatchTDB2.execute(gtdb.getDSG(), gn, bgp, input, filter, execCxt);
                return PatternMatchSage.matchTriplePattern(bgp, input, execCxt);
            }
            Log.warn(this, "Non-GraphTDB passed to OpExecutorPlainSage: "+g.getClass().getSimpleName());
            return super.execute(opBGP, input);
        }

        @Override
        public QueryIterator execute(OpQuadPattern opQuadPattern, QueryIterator input) {
            Node gn = opQuadPattern.getGraphNode();
            gn = decideGraphNode(gn, execCxt);

            if ( execCxt.getDataset() instanceof DatasetGraphTDB) {
                DatasetGraphTDB ds = (DatasetGraphTDB)execCxt.getDataset();
                Explain.explain("Execute", opQuadPattern.getPattern(), execCxt.getContext());
                BasicPattern bgp = opQuadPattern.getBasicPattern();
                // return PatternMatchTDB2.execute(ds, gn, bgp, input, filter, execCxt);
                return PatternMatchSage.matchQuadPattern(bgp, gn, input, execCxt);
            }
            // Maybe a TDB named graph inside a non-TDB dataset.
            Graph g = execCxt.getActiveGraph();
            if ( g instanceof GraphTDB ) {
                // Triples graph from TDB (which is the default graph of the dataset),
                // used a named graph in a composite dataset.
                BasicPattern bgp = opQuadPattern.getBasicPattern();
                Explain.explain("Execute", bgp, execCxt.getContext());
                // Don't pass in G -- gn may be different.
                // return PatternMatchTDB2.execute(((GraphTDB)g).getDSG(), gn, bgp, input, filter, execCxt);
                // (TODO) double check this part, possibly throw for now
                return PatternMatchSage.matchQuadPattern(bgp, gn, input, execCxt);
            }
            Log.warn(this, "Non-DatasetGraphTDB passed to OpExecutorPlainTDB");
            return super.execute(opQuadPattern, input);
        }
    }

}
