### Build How-to

To build Data Cooker ETL, you need Java 11 and Apache Maven. Exact version of Maven is enforced in the [project file](./pom.xml), so please look into enforcer plugin section. For Java, [Amazon's Corretto](https://corretto.aws/) is the preferred distribution.

There are two profiles to target AWS EMR production environment (`EMR` — selected by default) and for local testing of ETL processes (`local`), so you have to call
```bash
mvn clean package
```
or
```bash
mvn -Plocal clean package
```
to build a shaded executable 'Fat JAR' artifact, [datacooker-etl-cli.jar](./cli/target/datacooker-etl-cli.jar).

Currently supported version of EMR is 6.9. For local testing, Ubuntu 22.04 is recommended (either native or inside WSL).

As well as executable artifact, modular documentation is automatically built from the modules' metadata at [docs](./cli/docs/) directory, in both HTML ([single-file](./cli/docs/merged.html) and [linked files](./cli/docs/index.html)) and [PDF](./cli/docs/merged.pdf) formats.