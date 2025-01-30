t<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Reading Data From S3 Storage.

One of the most important --and performance sensitive-- parts
of the S3A connector is reading data from storage.
This is always evolving, based on experience, and benchmarking,
and in collaboration with other projects.

## Key concepts
 
* Data is read from S3 through an instance of an `ObjectInputStream`.
* There are different implementations of this in the codebase:
  `classic`, `prefetch` and `analytics`; these are called "stream types"
* The choice of which stream type to use is made in the hadoop configuration.

Configuration Options


| Property                   | Permitted Values                                        | Default   | Meaning                    |
|----------------------------|---------------------------------------------------------|-----------|----------------------------|
| `fs.s3a.input.stream.type` | `default`, `classic`, `prefetch`, `analytics`, `custom` | `classic` | name of stream type to use |


## Vector IO and Stream Types

All streams support VectorIO to some degree. 

| Stream | Support |
|--------|---------|
|  `classic`      | Parallel issuing of GET request with range coalescing         |
| `prefetch`  | Sequential reads, using prefetched blocks as appropriate | 
| `analytics`  | Sequential reads, using prefetched blocks as where possible |

Because the analytics streams is doing parquet-aware RowGroup prefetch


## Developer Topics

### Stream IOStatistics

Some of the streams support detailed IOStatistics, which will get aggregated into
the filesystem IOStatistics when the stream is closed(), or possibly after `unbuffer()`.

The filesystem aggregation can be displayed when the instance is closed, which happens
in process termination, if not earlier:
```xml
  <property>
    <name>fs.thread.level.iostatistics.enabled</name>
    <value>true</value>
  </property>
```

### Capabilities Probe for stream type and features.

`StreamCapabilities.hasCapability()` can be used to probe for the active
stream type and its capabilities.

### Unbuffer() support

The `unbuffer()` operation requires the stream to release all client-side
resources: buffer, connections to remote servers, cached files etc.
This is used in some query engines, including Apache Impala, to keep
streams open for rapid re-use, avoiding the overhead of re-opening files.

Only the classic stream supports `CanUnbuffer.unbuffer()`; 
the other streams must be closed rather than kept open for an extended
period of time.

### Stream Leak alerts

All input streams MUST be closed via a `close()` call once no-longer needed
-this is the only way to guarantee a timely release of HTTP connections
and local resources.

Some applications/libraries neglect to close the stram

### Custom Stream Types

There is a special stream type `custom`.
This is primarily used internally for testing, however it may also be used by
anyone who wishes to experiment with alternative input stream implementations.

If it is requested, then the name of the _factory_ for streams must be set in the
property `fs.s3a.input.stream.custom.factory`.

This must be a classname to an implementation of the factory service,
`org.apache.hadoop.fs.s3a.impl.streams.ObjectInputStreamFactory`.
Consult the source and javadocs of package `org.apache.hadoop.fs.s3a.impl.streams` for
details.

*Note* this is very much internal code and unstable: any use of this should be considered
experimental, unstable -and is not recommended for production use.



| Property                             | Permitted Values                       | Meaning                     |
|--------------------------------------|----------------------------------------|-----------------------------|
| `fs.s3a.input.stream.custom.factory` | name of factory class on the classpath | classname of custom factory |

