/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl.remote;

import com.google.inject.Singleton;
import io.github.pastorgl.datacooker.RegisteredPackages;

import javax.validation.constraints.NotEmpty;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

@Singleton
@Path("package")
public class PackageEndpoint {
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> registeredPackage() {
        return new ArrayList<>(RegisteredPackages.REGISTERED_PACKAGES.keySet());
    }

    @GET
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public String registeredPackage(@PathParam("name") @NotEmpty String name) {
        return RegisteredPackages.REGISTERED_PACKAGES.get(name);
    }
}
