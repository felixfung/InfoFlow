# InfoFlow
An Apache Spark implementation of the InfoMap community detection algorithm

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

  `Community Detection, merge nonedge`: when set to true, after community detection is completed, community detection would be attempted again, this time where unconnected modules may also merge

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

## Notebooks

The `Notebooks/` directory provides detailed math explanation behind InfoFlow in `InfoFlow Maths.pdf`, and `Algorithm Demo.ipynb` provides a graphical demonstration of InfoFlow. `InfoFlow.ipynb` provides a notebook that runs InfoFlow and performs basic graph analysis on the resultant communities.

## AWS deployment

The `ops-aws/` directory provides scripts to deploy InfoFlow on AWS EMR.

## Author

* **Felix Fung** [Github](https://github.com/felixfung)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
