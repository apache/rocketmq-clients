# Coding conventions

[TOC]

## Coding Style

We use the [Google C++ style
guide](https://google.github.io/styleguide/cppguide.html) where possible.

## Example code

Examples must NOT be in the `::opencensus` namespace, so that they are sure to
access the public API under `::opencensus` the same way as real clients of the
libraries would.

Examples should be written using the unit test framework so that they're
compiled and run as part of the test suite.

## Accessors

Should be named in lowercase and with underscores. e.g.:

```
class Foo {
 public:
  int some_number() const { return some_number_; }

 private:
  int some_number_;
};
```

String accessors should return `const std::string&` if the data is internally
already a `std::string`.

Reasoning: if the caller needs to use it as a constref, it's just a pointer
copy. If the caller needs to use it as a `string_view`, it copies two
words. If we returned a `string_view`, then using it as a string ref would
require a full string copy.

## Destructors

Use `= default` instead of `{}`.

```
virtual ~Foo() = default;
```

Reasoning: if a pre-requisite of the destructor disappears, the `= default`
variant won't break compilation.

## Classes that take an object and store it

The argument should be passed by value, and call `std::move()` in the function
body. e.g.

```
class Attribute {
 public:
  Attribute(std::string desc) : desc_(std::move(desc)) {}

 private:
  std::string desc_;
};
```

We do this for simplicity. We know it generates an extra move in some cases.
We can modify the API in a backwards compatible way later, to have
const ref and rvalue variants, and get the full optimization.

## Functions that copy arguments directly into FastLog

In the future, we will use `FastLog`. API calls that have to copy their data
into the log should take an `absl::string_view`.

Reasoning: `absl::string_view` is more versatile than `std::string` and faster
because it avoids constructing a string.

## Status objects

API calls should accept a `Status` object by value, and not two args
(`CanonicalCode` and string).

Reasoning: We believe that passing `Status` objects around is more idiomatic.
e.g. `SetStatus(SomeOperationReturningStatus())`

## Comments

No leading or trailing empty comment lines.
