/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.doc;

import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.data.TransformInfo;
import io.github.pastorgl.datacooker.data.Transforms;
import io.github.pastorgl.datacooker.scripting.OperationInfo;
import io.github.pastorgl.datacooker.scripting.Operations;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.apache.velocity.util.introspection.UberspectImpl;
import org.apache.velocity.util.introspection.UberspectPublicFields;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

public class DocGen {
    public static void main(String[] args) {
        try {
            final String outputDirectory = args[0];

            if (new File(outputDirectory).exists()) {
                Files.walk(Paths.get(outputDirectory))
                        .map(Path::toFile)
                        .sorted((o1, o2) -> -o1.compareTo(o2))
                        .forEach(File::delete);

            }
            Files.createDirectories(Paths.get(outputDirectory, "package"));
            Files.createDirectories(Paths.get(outputDirectory, "operation"));
            Files.createDirectories(Paths.get(outputDirectory, "transform"));

            Velocity.setProperty(RuntimeConstants.RESOURCE_LOADERS, "classpath");
            Velocity.setProperty(RuntimeConstants.RESOURCE_LOADER + ".classpath.class", ClasspathResourceLoader.class.getCanonicalName());
            Velocity.setProperty(RuntimeConstants.UBERSPECT_CLASSNAME, UberspectImpl.class.getName() + "," + UberspectPublicFields.class.getName());
//Velocity.setProperty(RuntimeConstants.RUNTIME_LOG_INSTANCE, new org.apache.logging.slf4j.Log4jLogger(new SimpleLogger("velocity", Level.ALL, false, true, false,false,null,null,new PropertiesUtil(new Properties()),System.err), "velocity"));
            Velocity.init();

            Map<String, String> pkgs = RegisteredPackages.REGISTERED_PACKAGES;

            try (FileWriter writer = new FileWriter(outputDirectory + "/index.md")) {
                VelocityContext ic = new VelocityContext();
                ic.put("pkgs", pkgs);

                Template index = Velocity.getTemplate("index.vm", UTF_8.name());
                index.merge(ic, writer);
            } catch (Exception e) {
                throw new Exception("Index", e);
            }

            for (String pkgName : pkgs.keySet()) {
                Map<String, OperationInfo> ops = Operations.packageOperations(pkgName);
                Map<String, TransformInfo> transforms = Transforms.packageTransforms(pkgName);

                try (FileWriter writer = new FileWriter(outputDirectory + "/package/" + pkgName + ".md")) {
                    String descr = RegisteredPackages.REGISTERED_PACKAGES.get(pkgName);

                    VelocityContext ic = new VelocityContext();
                    ic.put("name", pkgName);
                    ic.put("descr", descr);
                    ic.put("ops", ops);
                    ic.put("transforms", transforms);

                    Template index = Velocity.getTemplate("package.vm", UTF_8.name());
                    index.merge(ic, writer);
                } catch (Exception e) {
                    throw new Exception("Package '" + pkgName + "'", e);
                }

                for (Map.Entry<String, OperationInfo> entry : ops.entrySet()) {
                    String verb = entry.getKey();
                    OperationInfo opInfo = entry.getValue();

                    String opDir = outputDirectory + "/operation/" + verb;
                    try (FileWriter mdWriter = new FileWriter(opDir + ".md")) {
                        VelocityContext vc = new VelocityContext();
                        vc.put("op", opInfo.meta);
                        vc.put("pkgName", pkgName);

                        String example = null;
                        try (InputStream exStream = DocGen.class.getResourceAsStream("/test." + verb + ".tdl")) {
                            if (exStream != null) {
                                example = new BufferedReader(new InputStreamReader(exStream)).lines().collect(Collectors.joining("\n"));
                            }
                        } catch (Exception ignore) {
                        }
                        vc.put("example", example);

                        Template operation = Velocity.getTemplate("operation.vm", StandardCharsets.UTF_8.name());
                        operation.merge(vc, mdWriter);
                    } catch (Exception e) {
                        throw new Exception("Operation '" + verb + "'", e);
                    }
                }

                for (Map.Entry<String, TransformInfo> entry : transforms.entrySet()) {
                    String verb = entry.getKey();
                    TransformInfo opInfo = entry.getValue();

                    String opDir = outputDirectory + "/transform/" + verb;
                    try (FileWriter mdWriter = new FileWriter(opDir + ".md")) {
                        VelocityContext vc = new VelocityContext();
                        vc.put("op", opInfo.meta);
                        vc.put("pkgName", pkgName);

                        String example = null;
                        try (InputStream exStream = DocGen.class.getResourceAsStream("/test." + verb + ".tdl")) {
                            if (exStream != null) {
                                example = new BufferedReader(new InputStreamReader(exStream)).lines().collect(Collectors.joining("\n"));
                            }
                        } catch (Exception ignore) {
                        }
                        vc.put("example", example);

                        Template operation = Velocity.getTemplate("transform.vm", StandardCharsets.UTF_8.name());
                        operation.merge(vc, mdWriter);
                    } catch (Exception e) {
                        throw new Exception("Transform '" + verb + "'", e);
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Error while generating documentation:");
            e.printStackTrace();

            System.exit(-7);
        }
    }
}
