/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl.remote;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Module;
import io.github.pastorgl.datacooker.Options;
import io.github.pastorgl.datacooker.cli.Configuration;
import io.github.pastorgl.datacooker.data.DataContext;
import io.github.pastorgl.datacooker.scripting.VariablesContext;
import io.logz.guice.jersey.JerseyModule;
import io.logz.guice.jersey.JerseyServer;
import io.logz.guice.jersey.configuration.JerseyConfiguration;
import org.apache.spark.api.java.JavaSparkContext;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;

import java.util.ArrayList;
import java.util.List;

public class Server {
    public static void serve(Configuration config, JavaSparkContext context) throws Exception {
        String appPackage = Server.class.getPackage().getName();

        ResourceConfig resourceConfig = new ResourceConfig()
                .property(ServerProperties.PROVIDER_PACKAGES, appPackage)
                .property(ServerProperties.BV_SEND_ERROR_IN_RESPONSE, true);

        String iface = config.hasOption("iface") ? config.getOptionValue("iface") : "0.0.0.0";
        int port = config.hasOption("port") ? Integer.parseInt(config.getOptionValue("port")) : 9595;

        JerseyConfiguration configuration = JerseyConfiguration.builder()
                .withResourceConfig(resourceConfig)
                .addPackage(appPackage)
                .addHost(iface, port)
                .build();

        VariablesContext variablesContext = config.variables(context);
        VariablesContext options = new VariablesContext();
        options.put(Options.log_level.name(), "WARN");
        DataContext dataContext = new DataContext(context);
        dataContext.initialize(options);

        List<Module> modules = new ArrayList<>();
        modules.add(new JerseyModule(configuration));
        modules.add(new AbstractModule() {
            @Override
            protected void configure() {
                bind(JavaSparkContext.class).toInstance(context);
                bind(VariablesContext.class).toInstance(variablesContext);
                bind(DataContext.class).toInstance(dataContext);
            }
        });

        Guice.createInjector(modules)
                .getInstance(JerseyServer.class).start();
    }
}
