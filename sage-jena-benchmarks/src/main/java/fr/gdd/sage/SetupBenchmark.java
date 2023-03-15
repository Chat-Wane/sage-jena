package fr.gdd.sage;

import fr.gdd.sage.arq.OpExecutorSage;
import fr.gdd.sage.arq.QueryEngineSage;
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

public class SetupBenchmark {

    @State(Scope.Benchmark)
    public static class ExecutionContext {
        volatile Dataset dataset;
        volatile QueryExecution queryExecution;
        volatile String query = null;
    }


    public static void setup(ExecutionContext context, String dbPath, String engine) {
        context.dataset = TDB2Factory.connectDataset(dbPath);
        if (!context.dataset.isInTransaction()) {
            context.dataset.begin(ReadWrite.READ);
        }

        if (engine.equals("tdb")) {
            context.dataset.getContext().set(ARQ.optimization, true);
            QC.setFactory(context.dataset.getContext(), OpExecutorTDB2.OpExecFactoryTDB);
            QueryEngineTDB.register();
        } else if (engine.equals("sage")) {
            context.dataset.getContext().set(ARQ.optimization, true);
            QC.setFactory(context.dataset.getContext(), new OpExecutorSage.OpExecutorSageFactory(ARQ.getContext()));
            QueryEngineSage.register();
        } else if (engine.equals("tdb force order")) {
            Field plainFactoryField = ReflectionUtils._getField(OpExecutorTDB2.class, "plainFactory");
            OpExecutorFactory opExecutorTDB2ForceOrderFactory = (OpExecutorFactory) ReflectionUtils._callField(plainFactoryField, OpExecutorTDB2.class, null);
            context.dataset.getContext().set(ARQ.optimization, false);
            QC.setFactory(context.dataset.getContext(), opExecutorTDB2ForceOrderFactory);
            QueryEngineTDB.register();
        } else if (engine.equals("sage force order")) {
            context.dataset.getContext().set(ARQ.optimization, false);
            QC.setFactory(context.dataset.getContext(), new OpExecutorSage.OpExecutorSageFactory(ARQ.getContext()));
            QueryEngineSage.register();
        }
    }

    public static void setdown(ExecutionContext context, String engine) {
        if (engine.equals("tdb") || engine.equals("tdb force order")) {
            QueryEngineTDB.unregister();
        } else if (engine.equals("sage") || engine.equals("sage force order")) {
            QueryEngineSage.unregister();
        }
        if (context.dataset.isInTransaction()) {
            context.dataset.end();
        }
    }
}
