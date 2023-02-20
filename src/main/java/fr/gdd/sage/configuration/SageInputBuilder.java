package fr.gdd.sage.configuration;

import fr.gdd.sage.io.SageInput;



/**
 * Builder to ease the creation of sage input.
 *
 * Global configurations have more priority than local ones,
 * eg. global represents server wide configuration while local
 * represents a user defined preference.
 **/
public class SageInputBuilder {

    SageServerConfiguration global = new SageServerConfiguration();
    SageInput<?> local = new SageInput<>();


    
    public SageInputBuilder() { }

    // public SageInputBuilder() {
    // }

    public SageInputBuilder globalConfig(SageServerConfiguration global) {
        this.global = global;
        return this;
    }

    public SageInputBuilder localInput(SageInput<?> local) {
        this.local = local;
        return this;
    }

    public SageInput<?> build() {
        SageInput<?> merge = new SageInput<>()
            .setBackjumping(local.isBackjumping())
            .setRandomWalking(local.isRandomWalking())
            .setTimeout(Math.min(global.getLimit(), local.getLimit()))
            .setLimit(Math.min(global.getLimit(), local.getLimit()));
        return merge;
    }

}
