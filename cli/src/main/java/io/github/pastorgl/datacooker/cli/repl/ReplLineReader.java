/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl;

import org.jline.keymap.KeyMap;
import org.jline.reader.*;
import org.jline.reader.impl.LineReaderImpl;
import org.jline.reader.impl.ReaderUtils;
import org.jline.terminal.Attributes;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.AbstractTerminal;
import org.jline.utils.*;

import java.io.IOError;
import java.io.InterruptedIOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReplLineReader extends LineReaderImpl {
    static Field type;
    static Field rows;

    static {
        try {
            type = AbstractTerminal.class.getDeclaredField("type");
            type.setAccessible(true);
            rows = Display.class.getDeclaredField("rows");
            rows.setAccessible(true);
        } catch (Exception ignore) {
        }
    }

    protected AtomicBoolean ctrlC;

    public ReplLineReader(AtomicBoolean ctrlC, Terminal terminal, String appName, Map<String, Object> variables) {
        super(terminal, appName, variables);
        this.ctrlC = ctrlC;
    }

    /**
     * Need to copy-pasta this entire method because of hard-coded SIGINT handling. We want to handle it ourselves.
     */
    @Override
    public String readLine(String prompt, String rightPrompt, MaskingCallback maskingCallback, String buffer)
            throws UserInterruptException, EndOfFileException {
        if (!commandsBuffer.isEmpty()) {
            String cmd = commandsBuffer.remove(0);
            boolean done = false;
            do {
                try {
                    parser.parse(cmd, cmd.length() + 1, Parser.ParseContext.ACCEPT_LINE);
                    done = true;
                } catch (EOFError e) {
                    if (commandsBuffer.isEmpty()) {
                        throw new IllegalArgumentException("Incompleted command: \n" + cmd);
                    }
                    cmd += "\n";
                    cmd += commandsBuffer.remove(0);
                } catch (SyntaxError e) {
                    done = true;
                } catch (Exception e) {
                    commandsBuffer.clear();
                    throw new IllegalArgumentException(e.getMessage());
                }
            } while (!done);
            AttributedStringBuilder sb = new AttributedStringBuilder();
            sb.styled(AttributedStyle::bold, cmd);
            sb.toAttributedString().println(terminal);
            terminal.flush();
            return finish(cmd);
        }

        if (!startedReading.compareAndSet(false, true)) {
            throw new IllegalStateException();
        }

        Terminal.SignalHandler previousIntrHandler = null;
        Terminal.SignalHandler previousWinchHandler = null;
        Terminal.SignalHandler previousContHandler = null;
        Attributes originalAttributes = null;

        //copy-pasta isTerminalDumb()
        boolean dumb = Terminal.TYPE_DUMB.equals(terminal.getType()) || Terminal.TYPE_DUMB_COLOR.equals(terminal.getType());
        //end
        try {

            this.maskingCallback = maskingCallback;

            repeatCount = 0;
            mult = 1;
            regionActive = RegionType.NONE;
            regionMark = -1;

            smallTerminalOffset = 0;

            state = State.NORMAL;

            modifiedHistory.clear();

            setPrompt(prompt);
            setRightPrompt(rightPrompt);
            buf.clear();
            if (buffer != null) {
                buf.write(buffer);
            }
            if (nextCommandFromHistory && nextHistoryId > 0) {
                if (history.size() > nextHistoryId) {
                    history.moveTo(nextHistoryId);
                } else {
                    history.moveTo(history.last());
                }
                buf.write(history.current());
            } else {
                nextHistoryId = -1;
            }
            nextCommandFromHistory = false;
            undo.clear();
            parsedLine = null;
            keyMap = MAIN;

            if (history != null) {
                history.attach(this);
            }

            try {
                lock.lock();

                this.reading = true;

                //NEXT FUCKING LINE
                //previousIntrHandler = terminal.handle(Terminal.Signal.INT, signal -> readLineThread.interrupt());
                previousIntrHandler = terminal.handle(Terminal.Signal.INT, signal -> ctrlC.set(true));
                previousWinchHandler = terminal.handle(Terminal.Signal.WINCH, this::handleSignal);
                previousContHandler = terminal.handle(Terminal.Signal.CONT, this::handleSignal);
                originalAttributes = terminal.enterRawMode();

                //copy-pasta doDisplay()
                size.copy(terminal.getBufferSize());

                display = new Display(terminal, false);
                display.resize(size.getRows(), size.getColumns());
                if (isSet(Option.DELAY_LINE_WRAP)) display.setDelayLineWrap(true);
                //end

                if (!dumb) {
                    terminal.puts(InfoCmp.Capability.keypad_xmit);
                    if (isSet(Option.AUTO_FRESH_LINE)) callWidget(FRESH_LINE);
                    if (isSet(Option.MOUSE)) terminal.trackMouse(Terminal.MouseTracking.Normal);
                    if (isSet(Option.BRACKETED_PASTE)) terminal.writer().write(BRACKETED_PASTE_ON);
                } else {
                    Attributes attr = new Attributes(originalAttributes);
                    attr.setInputFlag(Attributes.InputFlag.IGNCR, true);
                    terminal.setAttributes(attr);
                }

                callWidget(CALLBACK_INIT);

                if (!isSet(Option.DISABLE_UNDO)) undo.newState(buf.copy());

                redrawLine();
                redisplay();
            } finally {
                lock.unlock();
            }

            while (true) {

                KeyMap<Binding> local = null;
                if (isInViCmdMode() && regionActive != RegionType.NONE) {
                    local = keyMaps.get(VISUAL);
                }
                Binding o = readBinding(getKeys(), local);
                if (o == null) {
                    throw new EndOfFileException().partialLine(buf.length() > 0 ? buf.toString() : null);
                }
                Log.trace("Binding: ", o);
                if (buf.length() == 0
                        && getLastBinding().charAt(0) == originalAttributes.getControlChar(Attributes.ControlChar.VEOF)) {
                    throw new EndOfFileException();
                }

                isArgDigit = false;
                count = ((repeatCount == 0) ? 1 : repeatCount) * mult;
                isUndo = false;
                if (regionActive == RegionType.PASTE) {
                    regionActive = RegionType.NONE;
                }

                try {
                    lock.lock();
                    Buffer copy = buf.length() <= ReaderUtils.getInt(this, FEATURES_MAX_BUFFER_SIZE, DEFAULT_FEATURES_MAX_BUFFER_SIZE)
                            ? buf.copy()
                            : null;
                    Widget w = getWidget(o);
                    if (!w.apply()) {
                        beep();
                    }
                    if (!isSet(Option.DISABLE_UNDO)
                            && !isUndo
                            && copy != null
                            && buf.length() <= ReaderUtils.getInt(this, FEATURES_MAX_BUFFER_SIZE, DEFAULT_FEATURES_MAX_BUFFER_SIZE)
                            && !copy.toString().equals(buf.toString())) {
                        undo.newState(buf.copy());
                    }

                    switch (state) {
                        case DONE:
                            return ctrlC.get() ? "" : finishBuffer();
                        case IGNORE:
                            return "";
                        case EOF:
                            throw new EndOfFileException();
                        case INTERRUPT:
                            throw new UserInterruptException(buf.toString());
                    }

                    if (!isArgDigit) {
                        repeatCount = 0;
                        mult = 1;
                    }

                    if (!dumb) {
                        redisplay();
                    }
                } finally {
                    lock.unlock();
                }
            }
        } catch (IOError e) {
            if (e.getCause() instanceof InterruptedIOException) {
                throw new UserInterruptException(buf.toString());
            } else {
                throw e;
            }
        } finally {
            try {
                lock.lock();

                this.reading = false;

                cleanup();
                if (originalAttributes != null) {
                    terminal.setAttributes(originalAttributes);
                }
                if (previousIntrHandler != null) {
                    terminal.handle(Terminal.Signal.INT, previousIntrHandler);
                }
                if (previousWinchHandler != null) {
                    terminal.handle(Terminal.Signal.WINCH, previousWinchHandler);
                }
                if (previousContHandler != null) {
                    terminal.handle(Terminal.Signal.CONT, previousContHandler);
                }
            } finally {
                lock.unlock();
                startedReading.set(false);
            }
        }
    }

    @Override
    protected void redisplay(boolean flush) {
        try {
            int oldRows = rows.getInt(display);
            AbstractTerminal at = (AbstractTerminal) this.terminal;
            String oldType = (String) type.get(at);

            type.set(at, Terminal.TYPE_DUMB_COLOR);

            List<AttributedString> newLines = getDisplayedBufferWithPrompts(new ArrayList<>())
                    .columnSplitLength(size.getColumns(), true, display.delayLineWrap());

            rows.setInt(this.display, newLines.size());

            super.redisplay(flush);

            rows.setInt(display, oldRows);
            type.set(at, oldType);
        } catch (Exception ignore) {
        }
    }
}
