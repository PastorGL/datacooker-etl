/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.doc;

import com.google.common.io.Resources;
import com.openhtmltopdf.pdfboxout.PdfRendererBuilder;
import io.github.pastorgl.datacooker.Options;
import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.cli.Highlighter;
import io.github.pastorgl.datacooker.data.TransformInfo;
import io.github.pastorgl.datacooker.data.Transforms;
import io.github.pastorgl.datacooker.metadata.AdapterMeta;
import io.github.pastorgl.datacooker.metadata.EvaluatorInfo;
import io.github.pastorgl.datacooker.metadata.InputAdapterMeta;
import io.github.pastorgl.datacooker.scripting.Functions;
import io.github.pastorgl.datacooker.scripting.OperationInfo;
import io.github.pastorgl.datacooker.scripting.Operations;
import io.github.pastorgl.datacooker.scripting.Operators;
import io.github.pastorgl.datacooker.storage.Adapters;
import io.github.pastorgl.datacooker.storage.InputAdapterInfo;
import io.github.pastorgl.datacooker.storage.OutputAdapterInfo;
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
import java.util.HashMap;
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
            Files.createDirectories(Paths.get(outputDirectory, "input"));
            Files.createDirectories(Paths.get(outputDirectory, "output"));
            Files.createDirectories(Paths.get(outputDirectory, "operation"));
            Files.createDirectories(Paths.get(outputDirectory, "transform"));

            Velocity.setProperty(RuntimeConstants.RESOURCE_LOADERS, "classpath");
            Velocity.setProperty(RuntimeConstants.RESOURCE_LOADER + ".classpath.class", ClasspathResourceLoader.class.getCanonicalName());
            Velocity.setProperty(RuntimeConstants.UBERSPECT_CLASSNAME, UberspectImpl.class.getName() + "," + UberspectPublicFields.class.getName());
//Velocity.setProperty(RuntimeConstants.RUNTIME_LOG_INSTANCE, new org.apache.logging.slf4j.Log4jLogger(new SimpleLogger("velocity", Level.ALL, false, true, false,false,null,null,new PropertiesUtil(new Properties()),System.err), "velocity"));
            Velocity.init();

            String header = Resources.toString(Resources.getResource("header.htm"), StandardCharsets.UTF_8);
            String footer = Resources.toString(Resources.getResource("footer.htm"), StandardCharsets.UTF_8);

            StringWriter m = new StringWriter();
            m.append(header);

            Map<String, String> pkgs = RegisteredPackages.REGISTERED_PACKAGES;
            try (FileWriter writer = new FileWriter(outputDirectory + "/index.html"); StringWriter sw = new StringWriter()) {
                VelocityContext ic = new VelocityContext();
                ic.put("pkgs", pkgs);
                ic.put("distro", args[1]);

                Velocity.getTemplate("index.vm", UTF_8.name()).merge(ic, sw);

                ic = new VelocityContext();
                ic.put("opts", Arrays.stream(Options.values()).collect(Collectors.toMap(Enum::name, o -> o, (a, b) -> a, TreeMap::new)));

                Velocity.getTemplate("options.vm", UTF_8.name()).merge(ic, sw);

                String index = sw.toString();

                m.append(index.replace("href=\"package/", "href=\"#package/"));
                writer.append(header);
                writer.append(index.replace("href=\"package/", "href=\"./package/"));
                writer.append(footer);
            } catch (Exception e) {
                throw new Exception("Index", e);
            }

            for (String pkgName : pkgs.keySet()) {
                Map<String, InputAdapterInfo> ins = Adapters.packageInputs(pkgName);
                Map<String, OutputAdapterInfo> outs = Adapters.packageOutputs(pkgName);
                Map<String, OperationInfo> ops = Operations.packageOperations(pkgName);
                Map<String, TransformInfo> transforms = Transforms.packageTransforms(pkgName);
                Map<String, EvaluatorInfo> operators = Operators.packageOperators(pkgName);
                Map<String, EvaluatorInfo> functions = Functions.packageFunctions(pkgName);

                try (FileWriter writer = new FileWriter(outputDirectory + "/package/" + pkgName + ".html"); StringWriter sw = new StringWriter()) {
                    String descr = RegisteredPackages.REGISTERED_PACKAGES.get(pkgName);

                    VelocityContext ic = new VelocityContext();
                    ic.put("name", pkgName);
                    ic.put("descr", descr);
                    ic.put("operators", operators);
                    ic.put("functions", functions);
                    ic.put("ins", ins);
                    ic.put("outs", outs);
                    ic.put("ops", ops);
                    ic.put("transforms", transforms);

                    Velocity.getTemplate("package.vm", UTF_8.name()).merge(ic, sw);

                    String pkg = sw.toString();

                    m.append(pkg.replace("href=\"operator/", "href=\"#operator/")
                            .replace("href=\"function/", "href=\"#function/")
                            .replace("href=\"input/", "href=\"#input/")
                            .replace("href=\"output/", "href=\"#output/")
                            .replace("href=\"transform/", "href=\"#transform/")
                            .replace("href=\"operation/", "href=\"#operation/")
                            .replace("href=\"index", "href=\"#index"));
                    writer.append(header);
                    writer.append(pkg.replace("href=\"operator/", "href=\"../operator/")
                            .replace("href=\"function/", "href=\"../function/")
                            .replace("href=\"input/", "href=\"../input/")
                            .replace("href=\"output/", "href=\"../output/")
                            .replace("href=\"transform/", "href=\"../transform/")
                            .replace("href=\"operation/", "href=\"../operation/")
                            .replace("href=\"index", "href=\"../index"));
                    writer.append(footer);
                } catch (Exception e) {
                    throw new Exception("Package '" + pkgName + "'", e);
                }

                for (Map.Entry<String, EvaluatorInfo> entry : operators.entrySet()) {
                    String verb = entry.getKey();
                    EvaluatorInfo evInfo = entry.getValue();

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

                for (Map.Entry<String, EvaluatorInfo> entry : functions.entrySet()) {
                    String verb = entry.getKey();
                    EvaluatorInfo evInfo = entry.getValue();

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

                for (Map.Entry<String, InputAdapterInfo> entry : ins.entrySet()) {
                    String verb = entry.getKey();
                    InputAdapterInfo inInfo = entry.getValue();

                    try (FileWriter writer = new FileWriter(outputDirectory + "/input/" + verb + ".html"); StringWriter sw = new StringWriter()) {
                        VelocityContext vc = new VelocityContext();
                        vc.put("op", inInfo.meta);
                        vc.put("pkgName", pkgName);

                        String example = genAdapterExample(inInfo.meta);
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

                for (Map.Entry<String, OutputAdapterInfo> entry : outs.entrySet()) {
                    String verb = entry.getKey();
                    OutputAdapterInfo outInfo = entry.getValue();

                    try (FileWriter writer = new FileWriter(outputDirectory + "/output/" + verb + ".html"); StringWriter sw = new StringWriter()) {
                        VelocityContext vc = new VelocityContext();
                        vc.put("op", outInfo.meta);
                        vc.put("pkgName", pkgName);

                        String example = genAdapterExample(outInfo.meta);
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
                                example = IOUtils.toString(exStream, StandardCharsets.UTF_8);

                                example = new Highlighter(example).highlight();
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
                                example = IOUtils.toString(exStream, StandardCharsets.UTF_8);

                                example = new Highlighter(example).highlight();
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

            System.exit(4);
        }
    }

    private static String genAdapterExample(AdapterMeta am) {
        Map<String, Object> params = new HashMap<>();
        am.definitions.forEach((name, meta) -> params.put(name, meta.defaults));
        String operator = (am instanceof InputAdapterMeta) ? "CREATE" : "COPY";
        String dir = (am instanceof InputAdapterMeta) ? "FROM" : "INTO";

        return new Highlighter(operator + " example " + params.entrySet().stream()
                .filter(e -> (e.getValue() != null))
                .map(e -> {
                    String ret = "@" + e.getKey() + "=";
                    Object def = e.getValue();
                    ret += (def instanceof String) ? "'" + def + "'" : String.valueOf(def).toUpperCase();
                    return ret;
                })
                .collect(Collectors.joining(",\n", "(", ")"))
                + "\n" + dir + " '" + am.paths[0] + "';"
        ).highlight();
    }
}
