### Build How-to

To build Data Cooker Dist, you need Java 11 and Apache Maven. Exact version of Maven is enforced in the [project file](datacooker-dist-cli/pom.xml), so please look into enforcer plugin section. For Java, [Amazon's Corretto](https://corretto.aws/) is the preferred distribution.

As a prerequisite, you need an artifact `io.github.pastorgl.datacooker:config` from [Data Cooker ETL](https://github.com/PastorGL/datacooker-etl) available in your local Maven repo, of the same version. Refer there for build instructions.

There are two profiles to target AWS EMR production environment (`EMR` — selected by default) and for local testing of ETL processes (`local`), so you have to call
```bash
mvn clean package
```
or
```bash
mvn -Plocal clean package
```
to build a shaded executable 'Fat JAR' artifact, [datacooker-dist.jar](./target/datacooker-dist.jar).

Currently supported version of EMR is 6.9. For local testing, Ubuntu 22.04 is recommended (either native or inside WSL).

As well as executable artifact, modular documentation is automatically built from the modules' metadata at [docs](./docs/) directory, in both HTML ([single-file](./docs/merged.html) and [linked files](./docs/index.html)) and [PDF](./docs/merged.pdf) formats.