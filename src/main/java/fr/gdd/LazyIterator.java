package fr.gdd;



/**
 * An iterator that enable retrieving values from the dictionary. Once
 * retrieved, the value is cached and only gets erased when the
 * underlying identifier changes.
 */
public class LazyIterator implements BackendIterator<ID, SKIP> {
    
    private BackendIterator<ID, SKIP> iterator;
    private Backend<ID, SKIP> backend;
    
    private ID subject_id   = null;
    private ID predicate_id = null;
    private ID object_id    = null;
    private ID context_id   = null;

    private boolean subject_has_changed   = true;
    private boolean predicate_has_changed = true;
    private boolean object_has_changed    = true;
    private boolean context_has_changed   = true;

    private String subject   = null;
    private String predicate = null;
    private String object    = null;
    private String context   = null;



    public LazyIterator(Backend<ID, SKIP> backend, BackendIterator<ID, SKIP> wrapped) {
        this.backend  = backend;
        this.iterator = wrapped;
    }
    
    @Override public ID subjectId()   { return this.subject_id; }
    @Override public ID predicateId() { return this.predicate_id; }
    @Override public ID objectId()    { return this.object_id; }
    @Override public ID contextId()   { return this.context_id; }

    @Override
    public void next() {
        iterator.next();

        if (iterator.subjectId() != this.subjectId()) {
            this.subject_has_changed = true;
            this.suject_id = iterator.subjectId();
        }
        if (iterator.predicateId() != this.predicateId()) {
            this.predicate_has_changed = true;
            this.predicate_id = iterator.predicateId();
        }
        if (iterator.objectId() != this.objectId()) {            
            this.object_has_changed = true;
            this.object_id = iterator.objectId();
        }
        if (iterator.contextId() != this.contextId()) {
            this.context_has_changed = true;
            this.context_id = iterator.contextId();
        }
    };

    @Override
    public void reset() {
        super.reset();
        this.subject_id   = null;
        this.predicate_id = null;
        this.object_id    = null;
        this.context_id   = null;
        this.subject_has_changed   = true;
        this.predicate_has_changed = true;
        this.object_has_changed    = true;
        this.context_has_changed   = true;
    }

    @Override
    public String subject() {
        if (subject_has_changed) {
            subject = dictionary.getSubjectId(subject);
            subject_has_changed = false;
        }
        return subject;
    }

    @Override
    public String predicate() {
        if (predicate_has_changed) {
            predicate = dictionary.getPredicateId(predicate);
            predicate_has_changed = false;
        }
        return predicate;
    }

    @Override
    public String object() {
        if (object_has_changed) {
            object = dictionary.getObjectId(object);
            object_has_changed = false;
        }
        return object;
    }

    @Override
    public String context() {
        if (context_has_changed) {
            context = dictionary.getContextId(context);
            context_has_changed = false;
        }
        return context;
    }
}
