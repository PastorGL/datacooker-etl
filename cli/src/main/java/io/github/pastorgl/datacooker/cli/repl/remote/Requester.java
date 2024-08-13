/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.cli.repl.remote;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import org.glassfish.jersey.client.JerseyClient;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.client.JerseyWebTarget;
import org.glassfish.jersey.jackson.JacksonFeature;

import java.util.Map;

public class Requester {
    private final JerseyClient client;
    private final String root;

    public Requester(String host, int port) {
        this.root = "http://" + host + ":" + port + "/";

        this.client = new JerseyClientBuilder().register(JacksonFeature.class).build();
    }

    private JerseyWebTarget getTarget(String path, Map<String, Object>[] qp) {
        JerseyWebTarget jwt = client.target(root + path);
        if (qp.length > 0) {
            for (Map.Entry<String, Object> entry : qp[0].entrySet()) {
                jwt = jwt.queryParam(entry.getKey(), entry.getValue());
            }
        }
        return jwt;
    }

    public <T> T get(String path, Class<T> cls, Map<String, Object>... qp) {
        JerseyWebTarget jwt = getTarget(path, qp);
        return jwt.request().get()
                .readEntity(cls);
    }

    public <T> T post(String path, Object ent, Class<T> cls, Map<String, Object>... qp) {
        JerseyWebTarget jwt = getTarget(path, qp);
        return jwt.request()
                .post(Entity.entity(ent, MediaType.APPLICATION_JSON_TYPE))
                .readEntity(cls);
    }

    public <T> T put(String path, Object ent, Class<T> cls, Map<String, Object>... qp) {
        JerseyWebTarget jwt = getTarget(path, qp);
        return jwt.request()
                .put(Entity.entity(ent, MediaType.APPLICATION_JSON_TYPE))
                .readEntity(cls);
    }
}
