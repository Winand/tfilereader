# Apache Hadoop TFile Reader
This is a pure Python implementation of Hadoop TFile format parser.

It allows to read aggregated YARN logs directly from HDFS without using `yarn logs` command.

Only zlib (gz) [compression](https://github.com/facebookarchive/hadoop-20/blob/master/src/core/org/apache/hadoop/io/file/tfile/Compression.java)
is supported. LZMA support can be added using *lzma* module, LZO requires an external library.

Requirements: Python 3.6+

## See also
[HADOOP-3315 New binary file format](https://issues.apache.org/jira/browse/HADOOP-3315)
