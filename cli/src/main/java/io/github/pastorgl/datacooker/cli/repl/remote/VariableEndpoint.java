package io.github.pastorgl.datacooker.cli.repl.remote;

import com.google.inject.Singleton;
import io.github.pastorgl.datacooker.scripting.VariablesContext;

import javax.inject.Inject;
import javax.validation.constraints.NotEmpty;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Singleton
@Path("variable")
public class VariableEndpoint {
    VariablesContext vc;

    @Inject
    public VariableEndpoint(VariablesContext vc) {
        this.vc = vc;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> variable() {
        return new ArrayList<>(vc.getAll());
    }

    @GET
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public Object variable(@PathParam("name") @NotEmpty String name) {
        return vc.getVar(name);
    }
}
