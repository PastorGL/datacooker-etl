/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.doc;

import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.scripting.OperationInfo;
import io.github.pastorgl.datacooker.scripting.Operations;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.apache.velocity.util.introspection.UberspectImpl;
import org.apache.velocity.util.introspection.UberspectPublicFields;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

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
//            Files.createDirectories(Paths.get(outputDirectory, "adapter", "input"));
//            Files.createDirectories(Paths.get(outputDirectory, "adapter", "output"));

            Velocity.setProperty(RuntimeConstants.RESOURCE_LOADERS, "classpath");
            Velocity.setProperty(RuntimeConstants.RESOURCE_LOADER + ".classpath.class", ClasspathResourceLoader.class.getCanonicalName());
            Velocity.setProperty(RuntimeConstants.UBERSPECT_CLASSNAME, UberspectImpl.class.getName() + "," + UberspectPublicFields.class.getName());
//Velocity.setProperty(RuntimeConstants.RUNTIME_LOG_INSTANCE, new org.apache.logging.slf4j.Log4jLogger(new SimpleLogger("velocity", Level.ALL, false, true, false,false,null,null,new PropertiesUtil(new Properties()),System.err), "velocity"));
            Velocity.init();

            Map<String, String> pkgs = Operations.PACKAGES;

            for (String pkgName : pkgs.keySet()) {
/*                ObjectMapper om = new TDLObjectMapper();
                om.configure(SerializationFeature.INDENT_OUTPUT, true);
                DefaultPrettyPrinter pp = new DefaultPrettyPrinter();
                pp.indentArraysWith(DefaultIndenter.SYSTEM_LINEFEED_INSTANCE);
                final ObjectWriter ow = om.writer(pp);

                for (Map.Entry<String, OperationInfo> entry : Operations.packageOperations(pkgName).entrySet()) {
                    String verb = entry.getKey();
                    OperationInfo opInfo = entry.getValue();
                    try (FileWriter mdWriter = new FileWriter(outputDirectory + "/operation/" + verb + ".md")) {
                        VelocityContext vc = new VelocityContext();
                        vc.put("op", opInfo.meta);
                        vc.put("pkgName", pkgName);

                        Template operation = Velocity.getTemplate("operation.vm", StandardCharsets.UTF_8.name());
                        operation.merge(vc, mdWriter);

                        TaskDefinitionLanguage.Task exampleTask = DocumentationGenerator.createExampleTask(opInfo, null);
                        String exampleDir = outputDirectory + "/operation/" + verb;
                        Files.createDirectories(Paths.get(exampleDir));
                        try (FileWriter jsonWriter = new FileWriter(new File(exampleDir, "example.json"))) {
                            ow.writeValue(jsonWriter, exampleTask);
                        }
                        try (final Writer iniWriter = new BufferedWriter(new FileWriter(new File(exampleDir, "example.ini")))) {
                            PropertiesWriter.writeProperties(exampleTask, iniWriter);
                        }
                    } catch (Exception e) {
                        throw new Exception("Operation '" + verb + "'", e);
                    }
                }*/
            }

/*            pkgs = Adapters.INPUT_PACKAGES;
            adapterDoc(outputDirectory, pkgs, "input");
            pkgs = Adapters.OUTPUT_PACKAGES;
            adapterDoc(outputDirectory, pkgs, "output");
*/
            pkgs = RegisteredPackages.REGISTERED_PACKAGES;

            for (String pkgName : pkgs.keySet()) {
                try (FileWriter writer = new FileWriter(outputDirectory + "/package/" + pkgName + ".md")) {
//                    DocumentationGenerator.packageDoc(pkgName, writer);
                } catch (Exception e) {
                    throw new Exception("Package '" + pkgName + "'", e);
                }
            }

            try (FileWriter writer = new FileWriter(outputDirectory + "/index.md")) {
//                DocumentationGenerator.indexDoc(pkgs, writer);
            } catch (Exception e) {
                throw new Exception("Index", e);
            }
        } catch (Exception e) {
            System.out.println("Error while generating documentation:");
            e.printStackTrace();

            System.exit(-7);
        }
    }

    private static void adapterDoc(String outputDirectory, Map<String, String> pkgs, String kind) throws Exception {
/*        for (String pkgName : pkgs.keySet()) {
            Map<String, AdapterInfo> adapters = "input".equals(kind) ? Adapters.packageInputs(pkgName) : Adapters.packageOutputs(pkgName);
            for (Map.Entry<String, AdapterInfo> entry : adapters.entrySet()) {
                String verb = entry.getKey();
                AdapterInfo info = entry.getValue();
                try (FileWriter mdWriter = new FileWriter(outputDirectory + "/adapter/" + kind + "/" + verb + ".md")) {
                    VelocityContext vc = new VelocityContext();
                    vc.put("op", info.meta);
                    vc.put("pkgName", pkgName);
                    vc.put("kind", kind);

                    Template operation = Velocity.getTemplate("adapter.vm", StandardCharsets.UTF_8.name());
                    operation.merge(vc, mdWriter);
                } catch (Exception e) {
                    throw new Exception("Adapter '" + verb + "' (" + kind + ")", e);
                }
            }
        }*/
    }
}
