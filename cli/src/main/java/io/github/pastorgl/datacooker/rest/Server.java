/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.rest;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.github.pastorgl.datacooker.Options;
import io.github.pastorgl.datacooker.cli.Configuration;
import io.github.pastorgl.datacooker.cli.Helper;
import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.scripting.Library;
import io.github.pastorgl.datacooker.scripting.OptionsContext;
import io.github.pastorgl.datacooker.scripting.Utils;
import io.github.pastorgl.datacooker.scripting.VariablesContext;
import io.logz.guice.jersey.JerseyModule;
import io.logz.guice.jersey.JerseyServer;
import io.logz.guice.jersey.configuration.JerseyConfiguration;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;

import java.util.ArrayList;
import java.util.List;

public class Server {
    private final Configuration config;
    private final String version;
    private final DataContext dataContext;
    private final Library library;
    private final OptionsContext optionsContext;
    private final VariablesContext variablesContext;

    public Server(Configuration config, String version, DataContext dataContext, Library library, OptionsContext optionsContext, VariablesContext variablesContext) {
        this.config = config;
        this.version = version;
        this.dataContext = dataContext;
        this.library = library;
        this.optionsContext = optionsContext;
        this.variablesContext = variablesContext;
    }

    public void serve() throws Exception {
        String appPackage = Server.class.getPackage().getName();

        ResourceConfig resourceConfig = new ResourceConfig()
                .property(ServerProperties.PROVIDER_PACKAGES, appPackage)
                .property(ServerProperties.BV_SEND_ERROR_IN_RESPONSE, true)
                .property(ServerProperties.WADL_FEATURE_DISABLE, true)
                .register(JacksonFeature.class);

        String host = config.hasOption("host") ? config.getOptionValue("host") : "0.0.0.0";
        int port = config.hasOption("port") ? Utils.parseNumber(config.getOptionValue("port")).intValue() : 9595;

        JerseyConfiguration configuration = JerseyConfiguration.builder()
                .withResourceConfig(resourceConfig)
                .addPackage(appPackage)
                .addHost(host, port)
                .build();

        optionsContext.put(Options.batch_verbose.name(), true);
        optionsContext.put(Options.log_level.name(), "INFO");

        List<Module> modules = new ArrayList<>();
        modules.add(new JerseyModule(configuration));
        modules.add(new AbstractModule() {
            @Override
            protected void configure() {
                bind(DataContext.class).toInstance(dataContext);
                bind(OptionsContext.class).toInstance(optionsContext);
                bind(VariablesContext.class).toInstance(variablesContext);
                bind(Library.class).toInstance(library);
                bindConstant().annotatedWith(Names.named("version")).to(version);
            }
        });

        Helper.populateEntities();

        Helper.log(new String[]{"Starting REST server on " + host + ":" + port});

        Guice.createInjector(modules)
                .getInstance(JerseyServer.class).start();
    }
}
