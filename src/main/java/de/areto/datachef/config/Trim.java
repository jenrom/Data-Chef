package de.areto.datachef.config;

import org.aeonbits.owner.Preprocessor;

public class Trim implements Preprocessor {
    public String process(String input) {
        return input.trim();
    }
}