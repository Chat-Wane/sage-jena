package fr.gdd.sage.arq;

import java.io.Serializable;
import java.util.Map;

public class PauseException extends RuntimeException {

    Map<Integer, ?> state;

    public PauseException(Map<Integer, ?> state) {
        this.state = state;
    }

}
