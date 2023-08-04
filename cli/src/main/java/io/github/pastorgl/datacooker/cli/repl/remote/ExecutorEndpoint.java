/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl.remote;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.scripting.TDL4ErrorListener;
import io.github.pastorgl.datacooker.scripting.TDL4Interpreter;
import io.github.pastorgl.datacooker.scripting.VariablesContext;

import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import java.util.ArrayList;
import java.util.List;

@Singleton
@Path("exec")
public class ExecutorEndpoint {
    DataContext dc;
    VariablesContext vc;

    @Inject
    public ExecutorEndpoint(VariablesContext vc, DataContext dc) {
        this.vc = vc;
        this.dc = dc;
    }

    @PUT
    @Path("line")
    public String line(@QueryParam("line") String line) {
        TDL4ErrorListener errorListener = new TDL4ErrorListener();
        TDL4Interpreter tdl4 = new TDL4Interpreter(line, vc, null, errorListener);
        if (errorListener.errorCount > 0) {
            List<String> errors = new ArrayList<>();
            for (int i = 0; i < errorListener.errorCount; i++) {
                errors.add("'" + errorListener.messages.get(i) + "' @ " + errorListener.lines.get(i) + ":" + errorListener.positions.get(i));
            }

            return errorListener.errorCount + " error(s).\n" +
                    String.join("\n", errors);
        }
        tdl4.interpret(dc);
        return null;
    }
}
