# OpenCensus C++ Trace library.

This directory tree contains the C++ API and implementation of OpenCensus
tracing.

The main entry point to tracing is the [`span.h`](span.h) file.
It's a relatively short file and the comments are a reference guide
to the tracing API.

For a quick use guide, look at the `examples/` directory.

Also see the tracing
[tutorial](https://opencensus.io/quickstart/cpp/tracing/) on the OpenCensus
website.

## Directory structure

* `./` - headers that are the public API for tracing.
* `exporter/` - headers that are the public API for trace exporting and
in-process storage.

* `examples/` - usage examples for the public API.
* `internal/` - internal implementation details and tests. Do not include or
rely on headers from this directory!

* `propagation/` - headers that are the public API for trace propagation, using
different encodings.

## API overview

`Span` is the central unit of tracing: Traces are made of Spans.

`TraceId` is an opaque token that uniquely identifies a trace.

`SpanId` is an opaque token which uniquely identifies a span within a trace.

`Sampler` decides whether or not a span will be sampled for export.

`SpanContext` consists of `TraceId`, `SpanId`, and `TraceOptions`.

`TraceConfig` is a global that holds `TraceParams`.

`TraceParams` configures the maximum number of Annotations, Attributes,
MessageEvents, and Links, as well as the currently active `Sampler`.

---

The OpenCensus data model follows the
[Stackdriver tracing data model](https://cloud.google.com/trace/docs/).

`Annotations` contain a text annotation along with a set of attributes.

`Attributes` are a map of key/value pairs where the key is a string and
             value is either a string, an integer, or a bool.

`Links` represent connections between spans in different traces.

`MessageEvents` represent messages (e.g. RPCs) that were sent/received between
                spans.
