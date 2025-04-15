/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl;

import io.github.pastorgl.datacooker.scripting.TDLLexicon;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser;
import org.jline.reader.SyntaxError;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ReplParser implements Parser {
    static final Pattern COMMAND = Pattern.compile("(?<cmd>\\S+\\s*)(?<args>.+)?", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

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
        if (input.isEmpty() && line.trim().startsWith("\\")) {
            List<String> words = new LinkedList<>();
            int index = 0;

            String cmdLine = line.trim().substring(1);
            if (cmdLine.endsWith(";")) {
                cmdLine = cmdLine.substring(0, cmdLine.length() - 1);
            }
            Matcher cmdMatcher = COMMAND.matcher(cmdLine);
            if (cmdMatcher.matches()) {
                String cmd = cmdMatcher.group("cmd");
                words.add("\\" + cmd);

                String args = cmdMatcher.group("args");
                if (args != null) {
                    words.add(args);

                    if (cursor > cmd.length() + 1) {
                        index = 1;
                    }
                }
            } else {
                words.add(line);
            }

            return new ReplParsedLine(true, line, cursor, words, index);
        }

        String current = input + line;

        TDLLexicon lexer = new TDLLexicon(CharStreams.fromString(current));
        lexer.removeErrorListeners();
        CommonTokenStream stream = new CommonTokenStream(lexer);
        stream.fill();

        List<Token> allTokens = stream.getTokens();

        List<Token> meaningfulTokens = allTokens.stream()
                .filter(token -> (token.getChannel() == TDLLexicon.DEFAULT_TOKEN_CHANNEL) && (token.getType() != Token.EOF))
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
            if (token.getType() == TDLLexicon.L_SPACES) {
                if (words.isEmpty()) {
                    continue;
                }
                words.add(words.remove(words.size() - 1) + token.getText());
            } else {
                words.add(token.getText());

                if ((words.size() == 1) && (lineTokens.get(0).getType() == TDLLexicon.L_SPACES)) {
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

        return new ReplParsedLine(false, line, cursor, words, index);
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
