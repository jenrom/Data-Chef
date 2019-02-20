package de.areto.datachef.parser;

import de.areto.common.util.ParseErrorListener;
import de.areto.datachef.model.mart.Mart;
import de.areto.datachef.parser.antlr4.MartDSLBaseVisitor;
import de.areto.datachef.parser.antlr4.MartDSLLexer;
import de.areto.datachef.parser.antlr4.MartDSLParser;
import de.areto.datachef.parser.mart.MartParser;
import lombok.NonNull;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.hibernate.Session;

public class MartScriptParser {

    private final ParseErrorListener errorListener = new ParseErrorListener();

    private final String martName;
    private final Session session;

    public MartScriptParser(@NonNull String martName, @NonNull Session session) {
        this.martName = martName;
        this.session = session;
    }

    public Mart parse(@NonNull String martCode) {
        final ANTLRInputStream in = new ANTLRInputStream(martCode);
        final MartDSLLexer lexer = new MartDSLLexer(in);
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);

        final CommonTokenStream tokens = new CommonTokenStream(lexer);
        final MartDSLParser parser = new MartDSLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);

        // One cycle for syntax errors
        final MartDSLBaseVisitor<Void> baseVisitor = new MartDSLBaseVisitor<>();
        baseVisitor.visitCompilationUnit(parser.compilationUnit());

        final Mart mart;

        if(!errorListener.hasErrors()) {
            tokens.reset();
            parser.reset();
            final MartParser martParser = new MartParser(session, martName);
            mart = martParser.visitCompilationUnit(parser.compilationUnit());
            mart.setScriptCode(martCode);
            return mart;
        } else {
            mart = new Mart();
            mart.setName(martName);
            mart.setIssueList(errorListener.getErrors());
            return mart;
        }
    }
}
