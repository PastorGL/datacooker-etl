/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.doc;

import com.google.common.io.Resources;
import com.openhtmltopdf.pdfboxout.PdfRendererBuilder;
import io.github.pastorgl.datacooker.Options;
import io.github.pastorgl.datacooker.PackageInfo;
import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.cli.Helper;
import io.github.pastorgl.datacooker.cli.Highlighter;
import io.github.pastorgl.datacooker.metadata.*;
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
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
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
            Files.createDirectories(Paths.get(outputDirectory, "operator"));
            Files.createDirectories(Paths.get(outputDirectory, "function"));
            Files.createDirectories(Paths.get(outputDirectory, "pluggable"));

            Velocity.setProperty(RuntimeConstants.RESOURCE_LOADERS, "classpath");
            Velocity.setProperty(RuntimeConstants.RESOURCE_LOADER + ".classpath.class", ClasspathResourceLoader.class.getCanonicalName());
            Velocity.setProperty(RuntimeConstants.UBERSPECT_CLASSNAME, UberspectImpl.class.getName() + "," + UberspectPublicFields.class.getName());
            Velocity.init();

            String header = Resources.toString(Resources.getResource("header.htm"), StandardCharsets.UTF_8);
            String footer = Resources.toString(Resources.getResource("footer.htm"), StandardCharsets.UTF_8);

            StringWriter m = new StringWriter();
            m.append(header);

            Helper.populateEntities();

            Map<String, PackageInfo> pkgs = RegisteredPackages.REGISTERED_PACKAGES;
            try (FileWriter writer = new FileWriter(outputDirectory + "/index.html"); StringWriter sw = new StringWriter()) {
                VelocityContext ic = new VelocityContext();
                ic.put("distro", args[1]);
                ic.put("pkgs", pkgs);
                ic.put("opts", Arrays.stream(Options.values()).collect(Collectors.toMap(Enum::name, o -> o, (a, b) -> a, TreeMap::new)));

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
                Map<String, PluggableInfo> pluggables = RegisteredPackages.REGISTERED_PACKAGES.get(pkgName).pluggables;
                Map<String, OperatorInfo> operators = RegisteredPackages.REGISTERED_PACKAGES.get(pkgName).operators;
                Map<String, FunctionInfo> functions = RegisteredPackages.REGISTERED_PACKAGES.get(pkgName).functions;

                try (FileWriter writer = new FileWriter(outputDirectory + "/package/" + pkgName + ".html"); StringWriter sw = new StringWriter()) {
                    String descr = RegisteredPackages.REGISTERED_PACKAGES.get(pkgName).descr;

                    VelocityContext ic = new VelocityContext();
                    ic.put("name", pkgName);
                    ic.put("descr", descr);
                    ic.put("operators", operators);
                    ic.put("functions", functions);
                    ic.put("pluggables", pluggables);

                    Velocity.getTemplate("package.vm", UTF_8.name()).merge(ic, sw);

                    String pkg = sw.toString();

                    m.append(pkg.replace("href=\"operator/", "href=\"#operator/")
                            .replace("href=\"function/", "href=\"#function/")
                            .replace("href=\"pluggable/", "href=\"#pluggable/")
                            .replace("href=\"index", "href=\"#index"));
                    writer.append(header);
                    writer.append(pkg.replace("href=\"operator/", "href=\"../operator/")
                            .replace("href=\"function/", "href=\"../function/")
                            .replace("href=\"pluggable/", "href=\"../pluggable/")
                            .replace("href=\"index", "href=\"../index"));
                    writer.append(footer);
                } catch (Exception e) {
                    throw new Exception("Package '" + pkgName + "'", e);
                }

                for (Map.Entry<String, OperatorInfo> entry : operators.entrySet()) {
                    String verb = entry.getKey();
                    OperatorInfo evInfo = entry.getValue();

                    try (FileWriter writer = new FileWriter(outputDirectory + "/operator/" + verb.hashCode() + ".html"); StringWriter sw = new StringWriter()) {
                        VelocityContext vc = new VelocityContext();
                        vc.put("op", evInfo);
                        vc.put("pkgName", pkgName);

                        Velocity.getTemplate("operator.vm", StandardCharsets.UTF_8.name()).merge(vc, sw);

                        String op = sw.toString();

                        m.append(op.replace("href=\"package/", "href=\"#package/")
                                .replace("href=\"index", "href=\"#index"));
                        writer.append(header);
                        writer.append(op.replace("href=\"package/", "href=\"../package/")
                                .replace("href=\"index", "href=\"../index"));
                        writer.append(footer);
                    } catch (Exception e) {
                        throw new Exception("Operator '" + verb + "'", e);
                    }
                }

                for (Map.Entry<String, FunctionInfo> entry : functions.entrySet()) {
                    String verb = entry.getKey();
                    FunctionInfo evInfo = entry.getValue();

                    try (FileWriter writer = new FileWriter(outputDirectory + "/function/" + verb + ".html"); StringWriter sw = new StringWriter()) {
                        VelocityContext vc = new VelocityContext();
                        vc.put("op", evInfo);
                        vc.put("pkgName", pkgName);

                        String example = null;
                        try (InputStream exStream = DocGen.class.getResourceAsStream("/test." + verb + ".tdl")) {
                            if (exStream != null) {
                                example = IOUtils.toString(exStream, StandardCharsets.UTF_8);

                                example = new Highlighter(example).highlight();
                            }
                        } catch (Exception ignore) {
                        }
                        vc.put("example", example);

                        Velocity.getTemplate("function.vm", StandardCharsets.UTF_8.name()).merge(vc, sw);

                        String op = sw.toString();

                        m.append(op.replace("href=\"package/", "href=\"#package/")
                                .replace("href=\"index", "href=\"#index"));
                        writer.append(header);
                        writer.append(op.replace("href=\"package/", "href=\"../package/")
                                .replace("href=\"index", "href=\"../index"));
                        writer.append(footer);
                    } catch (Exception e) {
                        throw new Exception("Function '" + verb + "'", e);
                    }
                }

                for (Map.Entry<String, PluggableInfo> entry : pluggables.entrySet()) {
                    String verb = entry.getKey();
                    PluggableInfo opInfo = entry.getValue();

                    try (FileWriter writer = new FileWriter(outputDirectory + "/pluggable/" + verb + ".html"); StringWriter sw = new StringWriter()) {
                        VelocityContext vc = new VelocityContext();
                        vc.put("verb", verb);
                        vc.put("pkgName", pkgName);
                        vc.put("kind", opInfo.meta.kind());
                        vc.put("objLvls", opInfo.meta.objLvls());
                        vc.put("descr", opInfo.meta.descr);
                        InputOutputMeta im = opInfo.meta.input;
                        if (im instanceof InputMeta) {
                            im = new NamedInputMeta(Map.of("", (InputMeta) im));
                        }
                        vc.put("input", im);
                        vc.put("definitions", opInfo.meta.definitions);
                        InputOutputMeta om = opInfo.meta.output;
                        if (om instanceof OutputMeta) {
                            om = new NamedOutputMeta(Map.of("", (OutputMeta) om));
                        }
                        vc.put("output", om);

                        String example = null;
                        try (InputStream exStream = DocGen.class.getResourceAsStream("/test." + verb + ".tdl")) {
                            if (exStream != null) {
                                example = IOUtils.toString(exStream, StandardCharsets.UTF_8);

                                example = new Highlighter(example).highlight();
                            }
                        } catch (Exception ignore) {
                        }
                        vc.put("example", example);

                        Velocity.getTemplate("pluggable.vm", StandardCharsets.UTF_8.name()).merge(vc, sw);

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

            System.exit(4);
        }
    }
}
