# D4GE

Distributed 4-node Graphlet Enumeration

X. Liu, Y. Santoso, V. Srinivasan, A. Thomo. University of Victoria.

## Compile
`mvn clean compile assembly:single`

## Prepare graphs
1. Download the original and transpose from <http://law.di.unimi.it/datasets.php>
2. Symmetrize the graph from the original and transpose by

```shell
java -cp target/triangles-1.0.jar it.unimi.dsi.webgraph.Transform union \
$(pwd)/<original> \
$(pwd)/<transpose> \
$(pwd)/<symmetrized output>
```

## Run
1. Install Spark
2. Execute

```shell
spark-submit \
--class quad.CD \
target/quad-1.0.jar \
--input-dir <path to symm graph input> \
--original <symm graph input> \
--num-colors <your rho val> \
--query-graph all \
--enable-preproc
``` 
 