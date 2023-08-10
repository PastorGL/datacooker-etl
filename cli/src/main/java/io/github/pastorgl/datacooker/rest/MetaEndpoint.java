/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.rest;

import io.github.pastorgl.datacooker.Options;
import io.github.pastorgl.datacooker.cli.repl.OptionsInfo;
import io.github.pastorgl.datacooker.scripting.OptionsContext;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
@Path("")
public class MetaEndpoint {
    final String version;
    final OptionsContext oc;

    @Inject
    public MetaEndpoint(@Named("version") String version, OptionsContext oc) {
        this.version = version;
        this.oc = oc;
    }

    @GET
    @Path("version")
    @Produces(MediaType.APPLICATION_JSON)
    public String version() {
        return version;
    }

    @GET
    @Path("options/enum")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> options() {
        return Arrays.stream(Options.values()).map(Enum::name).collect(Collectors.toList());
    }

    @GET
    @Path("options")
    @Produces(MediaType.APPLICATION_JSON)
    public OptionsInfo options(@QueryParam("name") String name) {
        if (Arrays.stream(Options.values()).map(Enum::name).anyMatch(e -> e.equals(name))) {
            return new OptionsInfo(Options.valueOf(name), oc.getOption(name));
        }

        return null;
    }
}
