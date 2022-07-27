# OpenCensus OcAgent Tracing Exporter

The *OpenCensus OcAgent Tracing Exporter* is a tracing exporter that exports
tracing to [OcAgent](https://opencensus.io/service/components/agent/).

## Quickstart

### Prerequisites

Install OcAgent as the [doc](https://opencensus.io/service/components/agent/install). You can deploy an agent in the same Pod with service. Or deploy it on a remote machine.

Make sure the `opencensus` receiver so configured. Example `config.yaml` file:

```yaml
receivers:
  opencensus:
    address: "127.0.0.1:55678"

exporters:
  zipkin:
    endpoint: "http://127.0.0.1:9411/api/v2/spans"
```

Start the agent:

```shell
ocagent_linux -c config.yaml
```

### Register the exporter

Include:

```c++
#include "opencensus/exporters/trace/ocagent/ocagent_exporter.h"
```

Add a BUILD dependency on:

```
"@io_opencensus_cpp//exporters/trace/ocagent:ocagent_exporter",
```

In your application's initialization code, register the exporter:

```c++
opencensus::exporters::trace::OcAgentOptions opts;
opts.address = "localhost:55678";
OcAgentExporter::Register(std::move(opts));
```

### Adding Spans to a Trace

A trace consists of a tree of spans. You should build an `::opencensus::trace::StartSpanOptions` and then start an span.

```c++
::opencensus::trace::AlwaysSampler sampler;
::opencensus::trace::StartSpanOptions opts = {&sampler};

auto span1 = ::opencensus::trace::Span::StartSpan("Span1", nullptr, opts);
```

Set some fields of the span.

```c++
span1.AddAnnotation("Annotation1", {{"TestBool", true}});
```

When the Span is finish, call the `End()` function and after that the span will be exported.

```c++
span1.End();
```

For more usage details, you can find out in the test file `ocagent_exporter_test.cc`.
