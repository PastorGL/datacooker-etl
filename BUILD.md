### How to Build 

To build Data Cooker ETL executable FatJAR artifact, you need Java 17 and Apache Maven.

Minimum supported version of Maven is enforced in the [project file](./pom.xml), so please look into enforcer plugin section. For Java, [Amazon's Corretto](https://corretto.aws/) is the preferred distribution.

There are two profiles to target AWS EMR production environment (`EMR` — selected by default) and for local debugging of ETL processes (`local`), so you have to call
```bash
mvn clean package
```
or
```bash
mvn -Plocal clean package
```
to build a desired flavor of [datacooker-etl.jar](./datacooker-etl-cli/target/datacooker-etl.jar).

Currently supported version of EMR is 7.2. For local debugging, Ubuntu 22.04 is recommended (either native or inside WSL).

### Documentation Generator

This companion utility is automatically called in the build process to extract the modules' metadata, and provides the evergreen, always updated documentation.

Its main class is `io.github.pastorgl.datacooker.doc.DocGen`, execution scope is `test`, and 3 required command line parameters specify documentation location directory, distro name, and version string.

By default, directory is [docs](./datacooker-etl-cli/docs/), where both merged ([single-file](./datacooker-etl-cli/docs/merged.html) and [linked files](./datacooker-etl-cli/docs/index.html)) HTML docs are placed.
