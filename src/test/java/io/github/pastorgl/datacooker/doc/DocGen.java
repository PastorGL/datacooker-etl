/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.doc;

import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.metadata.AdapterMeta;
import io.github.pastorgl.datacooker.storage.AdapterInfo;
import io.github.pastorgl.datacooker.storage.Adapters;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.io.Resources;
import com.openhtmltopdf.pdfboxout.PdfRendererBuilder;
import org.apache.commons.codec.Charsets;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.apache.velocity.util.introspection.UberspectImpl;
import org.apache.velocity.util.introspection.UberspectPublicFields;
import org.jsoup.Jsoup;
import org.jsoup.helper.W3CDom;
import org.jsoup.nodes.Document;

import java.io.File;
import java.io.FileWriter;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

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
            Files.createDirectories(Paths.get(outputDirectory, "input"));
            Files.createDirectories(Paths.get(outputDirectory, "output"));

            Velocity.setProperty(RuntimeConstants.RESOURCE_LOADERS, "classpath");
            Velocity.setProperty(RuntimeConstants.RESOURCE_LOADER + ".classpath.class", ClasspathResourceLoader.class.getCanonicalName());
            Velocity.setProperty(RuntimeConstants.UBERSPECT_CLASSNAME, UberspectImpl.class.getName() + "," + UberspectPublicFields.class.getName());
//Velocity.setProperty(RuntimeConstants.RUNTIME_LOG_INSTANCE, new org.apache.logging.slf4j.Log4jLogger(new SimpleLogger("velocity", Level.ALL, false, true, false,false,null,null,new PropertiesUtil(new Properties()),System.err), "velocity"));
            Velocity.init();

            String header = Resources.toString(Resources.getResource("header.htm"), Charsets.UTF_8);
            String footer = Resources.toString(Resources.getResource("footer.htm"), Charsets.UTF_8);

            StringWriter m = new StringWriter();
            m.append(header);

            Map<String, String> pkgs = RegisteredPackages.REGISTERED_PACKAGES;
            try (FileWriter writer = new FileWriter(outputDirectory + "/index.html"); StringWriter sw = new StringWriter()) {
                VelocityContext ic = new VelocityContext();
                ic.put("pkgs", pkgs);

                Velocity.getTemplate("index.vm", UTF_8.name()).merge(ic, sw);

                String index = sw.toString();

                m.append(index.replace("href=\"package/", "href=\"#package/"));
                writer.append(header);
                writer.append(index.replace("href=\"package/", "href=\"./package/"));
                writer.append(footer);
            } catch (Exception e) {
                throw new Exception("Index", e);
            }

            for (String pkgName : pkgs.keySet()) {
                Map<String, AdapterInfo> ins = Adapters.packageInputs(pkgName);
                Map<String, AdapterInfo> outs = Adapters.packageOutputs(pkgName);

                try (FileWriter writer = new FileWriter(outputDirectory + "/package/" + pkgName + ".html"); StringWriter sw = new StringWriter()) {
                    String descr = RegisteredPackages.REGISTERED_PACKAGES.get(pkgName);

                    VelocityContext ic = new VelocityContext();
                    ic.put("name", pkgName);
                    ic.put("descr", descr);
                    ic.put("ins", ins);
                    ic.put("outs", outs);

                    Velocity.getTemplate("package.vm", UTF_8.name()).merge(ic, sw);

                    String pkg = sw.toString();

                    m.append(pkg.replace("href=\"input/", "href=\"#input/")
                            .replace("href=\"output/", "href=\"#output/")
                            .replace("href=\"index", "href=\"#index"));
                    writer.append(header);
                    writer.append(pkg.replace("href=\"input/", "href=\"../input/")
                            .replace("href=\"output/", "href=\"../output/")
                            .replace("href=\"index", "href=\"../index"));
                    writer.append(footer);
                } catch (Exception e) {
                    throw new Exception("Package '" + pkgName + "'", e);
                }

                for (Map.Entry<String, AdapterInfo> entry : ins.entrySet()) {
                    String verb = entry.getKey();
                    AdapterInfo opInfo = entry.getValue();

                    try (FileWriter writer = new FileWriter(outputDirectory + "/input/" + verb + ".html"); StringWriter sw = new StringWriter()) {
                        VelocityContext vc = new VelocityContext();
                        vc.put("op", opInfo.meta);
                        vc.put("pkgName", pkgName);

                        String example =  genExampleConf("source", verb, opInfo.meta);
                        vc.put("example", example);

                        Velocity.getTemplate("input.vm", StandardCharsets.UTF_8.name()).merge(vc, sw);

                        String op = sw.toString();

                        m.append(op.replace("href=\"package/", "href=\"#package/")
                                .replace("href=\"index", "href=\"#index"));
                        writer.append(header);
                        writer.append(op.replace("href=\"package/", "href=\"../package/")
                                .replace("href=\"index", "href=\"../index"));
                        writer.append(footer);
                    } catch (Exception e) {
                        throw new Exception("Input adapter '" + verb + "'", e);
                    }
                }

                for (Map.Entry<String, AdapterInfo> entry : outs.entrySet()) {
                    String verb = entry.getKey();
                    AdapterInfo opInfo = entry.getValue();

                    try (FileWriter writer = new FileWriter(outputDirectory + "/output/" + verb + ".html"); StringWriter sw = new StringWriter()) {
                        VelocityContext vc = new VelocityContext();
                        vc.put("op", opInfo.meta);
                        vc.put("pkgName", pkgName);

                        String example = genExampleConf("dest", verb, opInfo.meta);
                        vc.put("example", example);

                        Velocity.getTemplate("output.vm", StandardCharsets.UTF_8.name()).merge(vc, sw);

                        String tr = sw.toString();

                        m.append(tr.replace("href=\"package/", "href=\"#package/")
                                .replace("href=\"index", "href=\"#index"));
                        writer.append(header);
                        writer.append(tr.replace("href=\"package/", "href=\"../package/")
                                .replace("href=\"index", "href=\"../index"));
                        writer.append(footer);
                    } catch (Exception e) {
                        throw new Exception("Output adapter '" + verb + "'", e);
                    }
                }
            }

            try (FileWriter writer = new FileWriter(outputDirectory + "/merged.html")) {
                m.append(footer);

                String merged = m.toString();

                writer.append(merged);
            } catch (Exception e) {
                throw new Exception("Merged HTML", e);
            }

            try (OutputStream os = Files.newOutputStream(Paths.get(outputDirectory + "/merged.pdf"))) {
                PdfRendererBuilder builder = new PdfRendererBuilder();
                builder.useFastMode();
                Document doc = Jsoup.parse(new File(outputDirectory + "/merged.html"), null);
                builder.withW3cDocument(new W3CDom().fromJsoup(doc), ".");
                builder.toStream(os);
                builder.run();
            } catch (Exception e) {
                throw new Exception("Merged PDF", e);
            }
        } catch (Exception e) {
            System.out.println("Error while generating documentation:");
            e.printStackTrace();

            System.exit(-7);
        }
    }

    private static String genExampleConf(String dir, String verb, AdapterMeta am) throws Exception {
        Map<String, Object> adapter = new HashMap<>();
        adapter.put("adapter", verb);
        adapter.put("path", "see description for examples");
        Map<String, Object> params = new HashMap<>();
        am.definitions.forEach((name, meta) -> params.put(name, meta.defaults));
        adapter.put("params", params);
        Map<String, Map<String, Object>> task = new HashMap<>();
        task.put(dir, adapter);
        task.put("dest".equals(dir) ? "source" : "dest", Collections.emptyMap());
        Map<String, List<Map<String, Map<String, Object>>>> example = Collections.singletonMap("example", Collections.singletonList(task));

        return new ObjectMapper()
                .enable(SerializationFeature.INDENT_OUTPUT)
                .writeValueAsString(example);
    }
}
