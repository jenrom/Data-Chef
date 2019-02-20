package de.areto.common.util;

import lombok.Getter;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.util.ArrayList;
import java.util.List;

public class ParseErrorListener extends BaseErrorListener {

    @Getter
    private final List<String> errors = new ArrayList<>();

    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) throws ParseCancellationException {
        final String error = String.format("line %d:%d %s", line, charPositionInLine, msg);
        errors.add(error);
    }
}
