package fr.gdd.sage;

import fr.gdd.sage.arq.OpExecutorSage;
import fr.gdd.sage.arq.QueryEngineSage;
import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.generics.Pair;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.solver.OpExecutorTDB2;
import org.apache.jena.tdb2.solver.QueryEngineTDB;
import org.openjdk.jmh.annotations.*;

import java.lang.reflect.Field;
import java.util.Objects;

/**
 * Have the utility functions to initialize the dataset and set up the proper engine
 * for query execution.
 */
public class SetupBenchmark {

    @State(Scope.Benchmark)
    public static class ExecutionContext {
        volatile Dataset dataset;
        volatile QueryExecution queryExecution;
        volatile String query = null;
    }


    public static void setup(ExecutionContext context, String dbPath, String engine) throws Exception {
        context.dataset = TDB2Factory.connectDataset(dbPath);
        if (!context.dataset.isInTransaction()) {
            context.dataset.begin(ReadWrite.READ);
        }

        switch (engine) {
            case EngineTypes.TDB -> {
                context.dataset.getContext().set(ARQ.optimization, true);
                QC.setFactory(context.dataset.getContext(), OpExecutorTDB2.OpExecFactoryTDB);
                QueryEngineTDB.register();
            }
            case EngineTypes.TDBForceOrder -> {
                Field plainFactoryField = ReflectionUtils._getField(OpExecutorTDB2.class, "plainFactory");
                OpExecutorFactory opExecutorTDB2ForceOrderFactory = (OpExecutorFactory) ReflectionUtils._callField(plainFactoryField, OpExecutorTDB2.class, null);
                context.dataset.getContext().set(ARQ.optimization, false);
                QC.setFactory(context.dataset.getContext(), opExecutorTDB2ForceOrderFactory);
                QueryEngineTDB.register();
            }

            case EngineTypes.Sage -> {
                context.dataset.getContext().set(ARQ.optimization, true);
                context.dataset.getContext().remove(SageConstants.limit);
                context.dataset.getContext().remove(SageConstants.timeout);
            }
            case EngineTypes.SageTimeout60s -> {
                context.dataset.getContext().set(ARQ.optimization, true);
                context.dataset.getContext().set(SageConstants.timeout, 60000);
                context.dataset.getContext().remove(SageConstants.limit);
            }
            case EngineTypes.SageTimeout1s -> {
                context.dataset.getContext().set(ARQ.optimization, true);
                context.dataset.getContext().set(SageConstants.timeout, 1000);
                context.dataset.getContext().remove(SageConstants.limit);
            }
            case EngineTypes.SageForceOrder -> {
                context.dataset.getContext().set(ARQ.optimization, false);
                context.dataset.getContext().remove(SageConstants.limit);
                context.dataset.getContext().remove(SageConstants.timeout);
            }
            case EngineTypes.SageForceOrderTimeout60s -> {
                forceOrderWithTimeout(context, 60000);
            }
            case EngineTypes.SageForceOrderTimeout30s -> {
                forceOrderWithTimeout(context, 30000);
            }
            case EngineTypes.SageForceOrderTimeout10s -> {
                forceOrderWithTimeout(context, 10000);
            }
            case EngineTypes.SageForceOrderTimeout1s -> {
                forceOrderWithTimeout(context, 1000);
            }
            case EngineTypes.SageForceOrderTimeout1ms -> {
                forceOrderWithTimeout(context, 1);
            }
            case EngineTypes.SageForceOrderLimit1 -> {
                context.dataset.getContext().set(ARQ.optimization, false);
                context.dataset.getContext().set(SageConstants.limit, 1);
                context.dataset.getContext().remove(SageConstants.timeout);
            }
            case default -> {
                throw new Exception("The configuration such an engine does not exist");
            }
        }

        switch (engine) {
            case EngineTypes.TDB, EngineTypes.TDBForceOrder -> {} // nothing
            default -> {
                QC.setFactory(context.dataset.getContext(), new OpExecutorSage.OpExecutorSageFactory(context.dataset.getContext()));
                QueryEngineSage.register();
            }
        }
    }

    /**
     * Sets the context value of the dataset so Sage knows the timeout value
     * before pausing/resuming query execution (in milliseconds)
     */
    static public void forceOrderWithTimeout(ExecutionContext context, long timeout) {
        context.dataset.getContext().set(ARQ.optimization, false);
        context.dataset.getContext().set(SageConstants.timeout, timeout);
        context.dataset.getContext().remove(SageConstants.limit);
    }

    public static void setdown(ExecutionContext context, String engine) {
        switch (engine) {
            case EngineTypes.TDB, EngineTypes.TDBForceOrder -> QueryEngineTDB.unregister();
            case default -> QueryEngineSage.unregister(); // Sage is default because there are numerous options
        }

        if (context.dataset.isInTransaction()) {
            context.dataset.end();
        }
    }


    public static Pair<Long, Long> execute(ExecutionContext context, String engine) {
        switch (engine) {
            case EngineTypes.TDB, EngineTypes.TDBForceOrder -> {
                return ExecuteUtils.executeTDB(context.dataset, context.query);
            }
            case default -> {
                return ExecuteUtils.executeTillTheEnd(context.dataset, context.query);
            }
        }

    }
}
