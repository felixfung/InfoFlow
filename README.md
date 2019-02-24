# InfoFlow
An Apache Spark implementation of the InfoMap community detection algorithm

## Overview

InfoFlow is a community detection algorithm based on InfoMap. InfoMap (http://www.mapequation.org/ https://www.pnas.org/content/105/4/1118) is an algorithm that takes advantage of the duality between module detection and coding, to provide community detection based on information flow. Specifically, InfoMap greedily merges communities together to minimize entropy of random walk on the network.

In this project, there are two advancements: (1) I develop the discrete maths in InfoMap so that it can be adapted into the Spark RDD structure; (2) based on this discrete maths, I devise a logarithmic time complexity algorithm, compared to the original linear time algorithm.

The `Notebooks/` directory provides explanation, in various depth, on the ideas behind InfoFlow. The detailed math explanation behind InfoFlow is in `InfoFlow Maths.pdf`. Jupyter notebook `Algorithm Demo.ipynb` provides a graphical demonstration of InfoFlow. Jupyter notebook `Citation Demo.ipynb` demonstrates an example usage of InfoFlow on a science citation network.

## AWS deployment

The `ops-aws/` directory provides scripts to deploy InfoFlow on AWS EMR.

## How to use

### (Optional)

Run `sbt test` to run unit tests

### Edit config.json:

  `config.json` contains parameters to run InfoFlow. Edit appropriately:

  `Graph`: file path of the graph file. Currently supported graph file formats are local Pajek net format (.net) and Parquet format. If Parquet format is used, then two Parquet files are required, one holding the vertex information (node index, node name, module index) and one holding edge information (from index, to index, edge weight). The file path points to a Json file with `.parquet` extension, in this format:
```
{
  "Vertex File": "path to vertex file",
  "Edge File": "path to edge file"
}
```

  `spark configs`: Spark related configurations

  `spark configs, Master`: spark master config, probably `local[*]` or `yarn`

  `PageRank`: Page Rank related parameter values

  `PageRank, tele`: probability of teleportation in PageRank, equal 1-(damping factor)

  `PageRank, error threshold factor`: PageRank termination condition is set to termination when Euclidean distance between previous vector and current vector is within 1/(node number)/(PageRank error threshold factor).

  `Community Detection`: community detection algorithm parametersInfoFlow

  `Community Detection, name`: InfoFlow or InfoMap

  `Community Detection, merge direction`: for InfoFlow, setting this to `symmetric` means module a would not seek to merge module b, unless module a has an edge to module b, even if module b has an edge to module a. If it is set to `symmetric`, then module a would seek to merge with module b

  `log, log path`: file path of text log file

  `log, Parquet path`: file path to save final graph in Parquet format

  `log, RDD path`: file path to save final graph in RDD format

  `log, txt path`: file path to save partitioning data in local txt format

  `log, Full Json path`: file path to save full graph in Json format, where each node is annotated with its associated module

  `log, Reduced Json path`: file path to save reduced graph in Json format, where each node is a module

  `log, debug`: boolean value; set true to save all intermediate graphs

To suppress a logging feature, put in an empty file path. For example, if you do not want to save in RDD format, set `RDD path` to "".

### Package and run

The Makefile provides a basic packaging and running method. Use `make run` to execute InfoFlow, although it will likely not provide optimal execution.

## Author

* **Felix Fung** [Github](https://github.com/felixfung)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
