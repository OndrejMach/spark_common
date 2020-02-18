# SPARK COMMONS
## Where to find it?
Spark Common library contains some interesting components useful for data retrieval and writing from external sources, job configuration and logging.
you can get it via gradle from the CDCP workbench repository nexus3, maven-releases. 
```$groovy
implementation 'spark_common:spark_common:1.1' // use the latest version instead of 1.1
```

## Sturcture
Project contains three main parts:
1) Config - classes useful for configuration handling using Scala Pure config. __GenericSettings__ can be used for data wrappers and displaying/validation of parameters, __ServiceConfig__ is used for parameters retrieval from your environment.
2) Readers - classed for reading CSV, MS ACCESS, Excel and JDBC data sources. All of them implement Reader trait. Data is then retrieved as a DataFrame using method Read. Any specific parameters are passed using class parameters when instantiating. Readers have companion objects. Example follows:
```$scala
val csvReader = CSVReader("src/test/resources/testData/testCsv.csv", header = true)
val df = csvReader.read() // DataFrame
```
CSV Readers are implemented in two instances: one reads a single CSV file, second is able to read muptiple files (Seq of filenames) for a path putting all the data in a single returned DataFrame.
All the Readers can be heavily parametrised depending on the structures they are supposed to read. You can specify schema in the spark format, infer one, define certain special behaviour such as escape character, quoting character etc. All the parameters are normally defaulted to the common values. It is generally recommended to specify schema even though this parameter is optional.
3) Writers - Classes which can be used for writing CSVs, Excel files, JDBC data stores. Follow very similar approach you can see in readers. Again all of them implement trait Writer which is a useful fact for testing and other potential enhancements.
4) Logger - creates an instance of SLF4J Logger which can be configured in log4j.properties file. Extending your classed with the Logger trait implemented here will give you ***logger*** which can be used.. guess for... logging.

Any potential enhancements should be discussed with the team and shall follow already defined concepts (a new reader to inherit from Reader, the same for writer). This should help to keep consistency, minimise risk of incompatibilities and enable testability.
## Publishing to nexus3
As mentioned above library is published in nexus3, maven-releases. You shall always increment the version number. Deployment is now done manually via nexus3 HTML user interface.