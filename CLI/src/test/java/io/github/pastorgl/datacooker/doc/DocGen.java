/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.doc;

import com.google.common.io.Resources;
import com.openhtmltopdf.pdfboxout.PdfRendererBuilder;
import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.data.TransformInfo;
import io.github.pastorgl.datacooker.data.Transforms;
import io.github.pastorgl.datacooker.scripting.OperationInfo;
import io.github.pastorgl.datacooker.scripting.Operations;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.apache.velocity.util.introspection.UberspectImpl;
import org.apache.velocity.util.introspection.UberspectPublicFields;
import org.jsoup.Jsoup;
import org.jsoup.helper.W3CDom;
import org.jsoup.nodes.Document;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

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
                Map<String, OperationInfo> ops = Operations.packageOperations(pkgName);
                Map<String, TransformInfo> transforms = Transforms.packageTransforms(pkgName);

                try (FileWriter writer = new FileWriter(outputDirectory + "/package/" + pkgName + ".html"); StringWriter sw = new StringWriter()) {
                    String descr = RegisteredPackages.REGISTERED_PACKAGES.get(pkgName);

                    VelocityContext ic = new VelocityContext();
                    ic.put("name", pkgName);
                    ic.put("descr", descr);
                    ic.put("ops", ops);
                    ic.put("transforms", transforms);

                    Velocity.getTemplate("package.vm", UTF_8.name()).merge(ic, sw);

                    String pkg = sw.toString();

                    m.append(pkg.replace("href=\"transform/", "href=\"#transform/")
                            .replace("href=\"operation/", "href=\"#operation/")
                            .replace("href=\"index", "href=\"#index"));
                    writer.append(header);
                    writer.append(pkg.replace("href=\"transform/", "href=\"../transform/")
                            .replace("href=\"operation/", "href=\"../operation/")
                            .replace("href=\"index", "href=\"../index"));
                    writer.append(footer);
                } catch (Exception e) {
                    throw new Exception("Package '" + pkgName + "'", e);
                }

                for (Map.Entry<String, TransformInfo> entry : transforms.entrySet()) {
                    String verb = entry.getKey();
                    TransformInfo opInfo = entry.getValue();

                    try (FileWriter writer = new FileWriter(outputDirectory + "/transform/" + verb + ".html"); StringWriter sw = new StringWriter()) {
                        VelocityContext vc = new VelocityContext();
                        vc.put("op", opInfo.meta);
                        vc.put("pkgName", pkgName);

                        String example = null;
                        try (InputStream exStream = DocGen.class.getResourceAsStream("/test." + verb + ".tdl")) {
                            if (exStream != null) {
                                example = IOUtils.toString(exStream, Charsets.UTF_8);

                                example = new SyntaxHighlighter(example).highlight();
                            }
                        } catch (Exception ignore) {
                        }
                        vc.put("example", example);

                        Velocity.getTemplate("transform.vm", StandardCharsets.UTF_8.name()).merge(vc, sw);

                        String tr = sw.toString();

                        m.append(tr.replace("href=\"package/", "href=\"#package/")
                                .replace("href=\"index", "href=\"#index"));
                        writer.append(header);
                        writer.append(tr.replace("href=\"package/", "href=\"../package/")
                                .replace("href=\"index", "href=\"../index"));
                        writer.append(footer);
                    } catch (Exception e) {
                        throw new Exception("Transform '" + verb + "'", e);
                    }
                }

                for (Map.Entry<String, OperationInfo> entry : ops.entrySet()) {
                    String verb = entry.getKey();
                    OperationInfo opInfo = entry.getValue();

                    try (FileWriter writer = new FileWriter(outputDirectory + "/operation/" + verb + ".html"); StringWriter sw = new StringWriter()) {
                        VelocityContext vc = new VelocityContext();
                        vc.put("op", opInfo.meta);
                        vc.put("pkgName", pkgName);

                        String example = null;
                        try (InputStream exStream = DocGen.class.getResourceAsStream("/test." + verb + ".tdl")) {
                            if (exStream != null) {
                                example = IOUtils.toString(exStream, Charsets.UTF_8);

                                example = new SyntaxHighlighter(example).highlight();
                            }
                        } catch (Exception ignore) {
                        }
                        vc.put("example", example);

                        Velocity.getTemplate("operation.vm", StandardCharsets.UTF_8.name()).merge(vc, sw);

                        String op = sw.toString();

                        m.append(op.replace("href=\"package/", "href=\"#package/")
                                .replace("href=\"index", "href=\"#index"));
                        writer.append(header);
                        writer.append(op.replace("href=\"package/", "href=\"../package/")
                                .replace("href=\"index", "href=\"../index"));
                        writer.append(footer);
                    } catch (Exception e) {
                        throw new Exception("Operation '" + verb + "'", e);
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
}
