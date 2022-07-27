# Context User Guide

Context is an object that holds information that is specific to an operation
such as an RPC.

Currently, the information that is held is:
  * A TagMap.
  * A Span.

In future, this may be expanded.

Each thread has a currently active Context. It is stored using thread-local
storage and can be retrieved with `Context::Current()`.

Example:

```c++
std::cout << "The current context is: "
          << Context::Current()::DebugString() << "n\";
```

## Running under a different Context

Contexts are conceptually immutable: the contents of a Context object cannot be
modified in-place.

To run in the Context of a different Span or TagMap, use the `WithSpan` and
`WithTagMap` classes, respectively.  Both of these classes are intended to be
used as stack-allocated RAII objects.  At construction time, they install the
given object into the current Context, and restore the previous value when they
fall out of scope.

Example:

```c++
// Create a Span to track a unit of work.
auto span = Span::StartSpan("MyOperation");
{
  WithSpan ws(span);
  // Perform work in the Context of the tracking Span.
  Process(batch_);
}
span.End();
```

## Retrieving information from a Context

Use the `:context_util` libraries to retrieve information from a Context.

Example:

```c++
const Span& span = ::opencensus::trace::GetCurrentSpan();
span.AddAnnotation("Processing batch.");
Process(batch_);
```

## Callbacks

We often need to continue the current operation in an asynchronous way, via a
callback function or similar. To do this, use `Context::Wrap()`, which installs
and uninstalls the specified Context around a given function.

Example:

```c++
void MyOpStart() {
  const Span& span = ::opencensus::trace::GetCurrentSpan();
  span.AddAnnotation("Starting AsyncOp.");
  AsyncOp op;
  op.Run(/* on_done = */ Context::Current()::Wrap(MyOpResume));
}

void MyOpResume() {
  const Span& span = ::opencensus::trace::GetCurrentSpan();
  // Both annotations should end up in the same Span that's tracking MyOp.
  span.AddAnnotation("AsyncOp completed.");
}
```

## Capturing Context

Prefer using `Wrap()`, but in cases where that is not possible, the current
Context can be captured and installed later using `WithContext`.

Example:

```c++
struct MyOperation {
  Context ctx;
  std::vector<Things> things_to_be_processed;
};

void StartMyOp(MyOperation* op) {
  auto span = Span::StartSpan("MyOperation");
  WithSpan ws(span);
  // Capture the current Context so the rest of the op can run under it.
  op->ctx = Context::Current();
  span.AddAnnotation("Preparing things.");
  Prepare(op->things_to_be_processed);
}

void RunMyOp(MyOperation* op) {
  WithContext wc(op->ctx);
  const Span& span = ::opencensus::trace::GetCurrentSpan();
  span.AddAnnotation("Processing things.");
  Process(op->things_to_be_processed);
  span.End();
}
```

## Passing Context between threads

New threads start with an empty context. Treat a new thread like running a
callback and just `Wrap()` the function being run.

## Best Practices

Always have the correct Context installed as the "current" Context.

Always stack-allocate `With*` objects.

Never deallocate a `With*` object on a different thread, this will corrupt the
thread-local Context.

## See Also

* [Context and Wrap](../context/context.h)
* [WithContext](../context/with_context.h)
* [WithTagMap](../tags/with_tag_map.h) and
  [`tags/context_util.h`](../tags/context_util.h)
* [WithSpan](../trace/with_span.h) and
  [`trace/context_util.h`](../trace/context_util.h)
