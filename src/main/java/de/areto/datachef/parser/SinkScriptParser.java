package de.areto.datachef.parser;

import de.areto.common.util.ParseErrorListener;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.parser.antlr4.SinkDSLBaseVisitor;
import de.areto.datachef.parser.antlr4.SinkDSLLexer;
import de.areto.datachef.parser.antlr4.SinkDSLParser;
import de.areto.datachef.parser.mapping.MappingParser;
import lombok.NonNull;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.hibernate.Session;

public class SinkScriptParser {

    final ParseErrorListener errorListener = new ParseErrorListener();

    private final String mappingName;
    private final Session session;

    public SinkScriptParser(@NonNull String mappingName, @NonNull Session session) {
        this.mappingName = mappingName;
        this.session = session;
    }

    public Mapping parse(@NonNull String mappingCode) {
        final ANTLRInputStream in = new ANTLRInputStream(mappingCode);
        final SinkDSLLexer lexer = new SinkDSLLexer(in);
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);

        final CommonTokenStream tokens = new CommonTokenStream(lexer);
        final SinkDSLParser parser = new SinkDSLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);

        // One cycle for syntax errors
        final SinkDSLBaseVisitor<Void> baseVisitor = new SinkDSLBaseVisitor<>();
        baseVisitor.visitCompilationUnit(parser.compilationUnit());

        final Mapping mapping;
        if(!errorListener.hasErrors()) {
            tokens.reset();
            parser.reset();
            final MappingParser mappingParser = new MappingParser(mappingName, session);
            mapping = mappingParser.visitCompilationUnit(parser.compilationUnit());
            mapping.setScriptCode(mappingCode);
            return mapping;
        } else {
            mapping = new Mapping(mappingName);
            mapping.setIssueList(errorListener.getErrors());
            return mapping;

        }
    }
}
