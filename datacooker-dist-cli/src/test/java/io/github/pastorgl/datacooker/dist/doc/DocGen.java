/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.dist.doc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.io.Resources;
import io.github.pastorgl.datacooker.PackageInfo;
import io.github.pastorgl.datacooker.RegisteredPackages;
import io.github.pastorgl.datacooker.data.ObjLvl;
import io.github.pastorgl.datacooker.metadata.*;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.apache.velocity.util.introspection.UberspectImpl;
import org.apache.velocity.util.introspection.UberspectPublicFields;

import java.io.File;
import java.io.FileWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
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
            Files.createDirectories(Paths.get(outputDirectory, "pluggable"));

            Velocity.setProperty(RuntimeConstants.RESOURCE_LOADERS, "classpath");
            Velocity.setProperty(RuntimeConstants.RESOURCE_LOADER + ".classpath.class", ClasspathResourceLoader.class.getCanonicalName());
            Velocity.setProperty(RuntimeConstants.UBERSPECT_CLASSNAME, UberspectImpl.class.getName() + "," + UberspectPublicFields.class.getName());
            Velocity.init();

            String header = Resources.toString(Resources.getResource("header.htm"), StandardCharsets.UTF_8);
            String footer = Resources.toString(Resources.getResource("footer.htm"), StandardCharsets.UTF_8);

            StringWriter m = new StringWriter();
            m.append(header);

            Pluggables.load();

            Map<String, PackageInfo> pkgs = RegisteredPackages.REGISTERED_PACKAGES;
            try (FileWriter writer = new FileWriter(outputDirectory + "/index.html"); StringWriter sw = new StringWriter()) {
                VelocityContext ic = new VelocityContext();
                ic.put("distro", args[1]);
                ic.put("rev", args[2]);
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
                List<PluggableInfo> pluggables = RegisteredPackages.REGISTERED_PACKAGES.get(pkgName).pluggables.stream()
                        .filter(e -> {
                            PluggableMeta meta = e.meta;
                            return meta.execFlag(ExecFlag.INPUT) || meta.execFlag(ExecFlag.OUTPUT) || meta.execFlag(ExecFlag.TRANSFORM);
                        })
                        .toList();

                try (FileWriter writer = new FileWriter(outputDirectory + "/package/" + pkgName + ".html"); StringWriter sw = new StringWriter()) {
                    String descr = RegisteredPackages.REGISTERED_PACKAGES.get(pkgName).descr;

                    VelocityContext ic = new VelocityContext();
                    ic.put("name", pkgName);
                    ic.put("descr", descr);
                    ic.put("pluggables", pluggables);

                    Velocity.getTemplate("package.vm", UTF_8.name()).merge(ic, sw);

                    String pkg = sw.toString();

                    m.append(pkg.replace("href=\"pluggable/", "href=\"#pluggable/")
                            .replace("href=\"index", "href=\"#index"));
                    writer.append(header);
                    writer.append(pkg.replace("href=\"pluggable/", "href=\"../pluggable/")
                            .replace("href=\"index", "href=\"../index"));
                    writer.append(footer);
                } catch (Exception e) {
                    throw new Exception("Package '" + pkgName + "'", e);
                }

                for (PluggableInfo opInfo : pluggables) {
                    String verb = opInfo.meta.verb;

                    try (FileWriter writer = new FileWriter(outputDirectory + "/pluggable/" + opInfo.meta.execFlags.hashCode() + verb + ".html"); StringWriter sw = new StringWriter()) {
                        VelocityContext vc = new VelocityContext();
                        vc.put("verb", verb);
                        vc.put("efhc", opInfo.meta.execFlags.hashCode());
                        vc.put("pkgName", pkgName);
                        vc.put("kind", opInfo.meta.kind());
                        vc.put("reqObjLvl", opInfo.meta.dsFlag(DSFlag.REQUIRES_OBJLVL));
                        vc.put("keyBefore", opInfo.meta.dsFlag(DSFlag.KEY_BEFORE));
                        ObjLvl[] objLvls = opInfo.meta.objLvls();
                        vc.put("objLvls", (objLvls == null) ? null : Arrays.stream(objLvls).map(ObjLvl::toString).collect(Collectors.toList()));
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

                        String example = genExampleConf(opInfo.meta);
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
                        throw new Exception("Adapter '" + verb + "'", e);
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
        } catch (Exception e) {
            System.out.println("Error while generating documentation:");
            e.printStackTrace();

            System.exit(-7);
        }
    }

    private static String genExampleConf(PluggableMeta am) throws Exception {
        Map<String, Object> plg = new HashMap<>();
        plg.put("adapter", am.verb);
        if (am.execFlag(ExecFlag.INPUT)) {
            plg.put("part_count", 100);
            plg.put("path", ((PathMeta) am.input).examples[0]);
        }
        if (am.execFlag(ExecFlag.OUTPUT)) {
            plg.put("path", ((PathMeta) am.output).examples[0]);
        }
        Map<String, Object> params = new HashMap<>();
        if (am.definitions != null) {
            am.definitions.forEach((name, meta) -> params.put(name, meta.defaults));
        }
        plg.put("params", params);
        Map<String, Object> task = new HashMap<>();
        task.put("name", am.verb + "_example");
        task.put("source", am.execFlag(ExecFlag.INPUT) ? plg : Collections.emptyMap());
        task.put("transform", new Object[]{am.execFlag(ExecFlag.TRANSFORM) ? plg : Collections.emptyMap()});
        task.put("dest", am.execFlag(ExecFlag.OUTPUT) ? plg : Collections.emptyMap());
        Map<String, List<Map<String, Object>>> example = Collections.singletonMap("example", Collections.singletonList(task));

        return new ObjectMapper()
                .enable(SerializationFeature.INDENT_OUTPUT)
                .writeValueAsString(example);
    }
}
