package io.github.pastorgl.datacooker.cli;

import io.github.pastorgl.datacooker.scripting.TDL4Lexicon;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser;
import org.jline.reader.SyntaxError;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class TDL4Parser implements Parser {
    private String input = "";

    public final List<Token> tokens = new LinkedList<>();

    public Integer curToken = null;

    public void update(String contd) {
        input += contd;
    }

    public void reset() {
        input = "";
        tokens.clear();
        curToken = null;
    }

    @Override
    public ParsedLine parse(String line, int cursor, ParseContext context) throws SyntaxError {
        String current = input + line;

        TDL4Lexicon lexer = new TDL4Lexicon(CharStreams.fromString(current));
        lexer.removeErrorListeners();
        CommonTokenStream stream = new CommonTokenStream(lexer);
        stream.fill();

        List<Token> allTokens = stream.getTokens();

        List<Token> meaningfulTokens = allTokens.stream()
                .filter(token -> (token.getChannel() == TDL4Lexicon.DEFAULT_TOKEN_CHANNEL) && (token.getType() != Token.EOF))
                .collect(Collectors.toList());

        tokens.clear();
        tokens.addAll(meaningfulTokens);

        final int[] boundary = new int[]{input.length()};
        List<Token> lineTokens = allTokens.stream()
                .filter(token -> (token.getStartIndex() >= boundary[0]) && (token.getType() != Token.EOF))
                .collect(Collectors.toList());

        curToken = null;
        boundary[0] += cursor;
        for (int i = 0, len = meaningfulTokens.size(); i < len; i++) {
            Token token = meaningfulTokens.get(i);
            if (token.getStartIndex() >= boundary[0]) {
                curToken = i - 1;
                break;
            }
        }
        if ((cursor > 0) && (curToken == null)) {
            curToken = meaningfulTokens.size() - 1;
        }

        List<String> words = new LinkedList<>();
        Integer index = null;
        for (Token token : lineTokens) {
            if (token.getType() == TDL4Lexicon.L_SPACES) {
                if (words.isEmpty()) {
                    continue;
                }
                words.add(words.remove(words.size() - 1) + token.getText());
            } else {
                words.add(token.getText());

                if ((words.size() == 1) && (lineTokens.get(0).getType() == TDL4Lexicon.L_SPACES)) {
                    words.add(lineTokens.get(0).getText() + words.remove(0));
                }
            }
            if ((index == null) && (token.getStopIndex() >= cursor)) {
                index = words.size() - 1;
            }
        }
        if (words.isEmpty() && !lineTokens.isEmpty()) {
            words.add(lineTokens.get(0).getText());
        }
        if ((index == null) && (cursor > 0)) {
            index = words.size() - 1;
        }

        return new TDL4ParsedLine(line, cursor, words, index);
    }

    @Override
    public boolean isEscapeChar(char ch) {
        return false;
    }

    @Override
    public boolean validCommandName(String name) {
        return false;
    }

    @Override
    public boolean validVariableName(String name) {
        return false;
    }

    @Override
    public String getCommand(String line) {
        return null;
    }

    @Override
    public String getVariable(String line) {
        return null;
    }
}
