package io.github.pastorgl.datacooker.rest;

import io.github.pastorgl.datacooker.scripting.VariablesContext;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.validation.constraints.NotEmpty;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

@Singleton
@Path("variable")
public class VariableEndpoint {
    final VariablesContext vc;

    @Inject
    public VariableEndpoint(VariablesContext vc) {
        this.vc = vc;
    }

    @GET
    @Path("enum")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> variables() {
        return new ArrayList<>(vc.getAll());
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Object variable(@QueryParam("name") @NotEmpty String name) {
        return vc.getVar(name);
    }
}
