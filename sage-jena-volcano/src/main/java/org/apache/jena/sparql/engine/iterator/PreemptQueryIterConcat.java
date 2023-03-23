package org.apache.jena.sparql.engine.iterator;

import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.generics.Pair;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Objects;

// (TODO) documentation
public class PreemptQueryIterConcat extends QueryIterConcat {

    int offset = 0;

    SageOutput output;
    SageInput sageInput;

    static Integer ids;
    int id;

    public PreemptQueryIterConcat(ExecutionContext context) {
        super(context);
        output = context.getContext().get(SageConstants.output);
        sageInput  = context.getContext().get(SageConstants.input);
        if (Objects.isNull(ids)) {
            ids = 1000;
        }
        ids += 1;

        id = ids;

        if (this.sageInput.getState().containsKey(id)) {
            skip((int) sageInput.getState(id));
        }
    }

    @Override
    protected boolean hasNextBinding() {
        if  (System.currentTimeMillis() >= sageInput.getDeadline() || output.size() >= sageInput.getLimit()) {
            System.out.println("CONCAT SAVE");
            this.output.save(new Pair(id, offset - 1));
            return false;
        }
        return super.hasNextBinding();
    }

    public void skip(int to){
        System.out.println("CONCAT Skip to " + to);
        this.offset = to;
    }

}
