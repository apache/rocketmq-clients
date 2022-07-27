# Zipkin Export Example

This is a simple example program to demonstrate how to export tracing
information to Zipkin using OpenCensus.

Start the zipkin server:
```shell
docker run -p 9411:9411 openzipkin/zipkin

# Or, download zipkin and run:
# java -jar zipkin.jar --logging.level.zipkin=DEBUG --logging.level.zipkin2=DEBUG
```

Run the OpenCensus example:
```shell
bazel run //opencensus/exporters/trace/zipkin:zipkin_exporter_test
```

Go to http://127.0.0.1:9411/ and click the "Find Traces" button to see traces.

## Zipkin

A Zipkin server needs to be running to send spans to it.
Zipkin can be found [here](https://github.com/openzipkin/zipkin).
Instructions on running the server can be found in their
[quickstart](https://zipkin.io/pages/quickstart.html).
The Zipkin server should by default listen on port 9411.

Zipkin's tracing model is not identical to the model used by OpenCensus. Here is
a list of some of the differences when converting from OpenCensus to Zipkin:

OpenCensus | Zipkin
---------- | ------
message events | annotations (formatted as: SENT/ID/size)
attributes | tags (which are key/value pairs in Zipkin)
links | not supported

## Viewing Example Trace

The traces will show up on the Zipkin webpage UI.
By default, this is http://127.0.0.1:9411/

Click the "Find Traces" button and the trace sent from the exporter should
appear.
Click on it and it should look something like this:

![Example Trace](https://i.imgur.com/7bNWraI.png)

The individual spans can be clicked on, which will list the tracing information
for that particular span.

![Example Span](https://i.imgur.com/S2yVHtu.png)
