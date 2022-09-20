all: $(wildcard *.cpp)
	c++ -std=c++11 -I../include -L../lib ExampleProducer.cpp -o example_producer -lrocketmq -Wl,-rpath,../lib
	c++ -std=c++11 -I../include -L../lib ExamplePushConsumer.cpp -o example_push_consumer -lrocketmq -Wl,-rpath,../lib
	c++ -std=c++11 -I../include -L../lib ExampleProducerWithAsync.cpp -o example_producer_with_async -lrocketmq -Wl,-rpath,../lib
	c++ -std=c++11 -I../include -L../lib ExampleProducerWithFifoMessage.cpp -o example_producer_with_fifo_message -lrocketmq -Wl,-rpath,../lib
	c++ -std=c++11 -I../include -L../lib ExampleProducerWithTimedMessage.cpp -o example_producer_with_timed_message -lrocketmq -Wl,-rpath,../lib
	c++ -std=c++11 -I../include -L../lib ExampleProducerWithTransactionalMessage.cpp.cpp -o example_producer_with_transactinoal_message -lrocketmq -Wl,-rpath,../lib
		c++ -std=c++11 -I../include -L../lib ExampleSimpleConsumer.cpp -o example_simple_consumer -lrocketmq -Wl,-rpath,../lib
clean:
	rm -fr example_producer
	rm -fr example_push_consumer
	rm -fr example_producer_with_async
	rm -fr example_producer_with_fifo_message
	rm -fr example_producer_with_timed_message
	rm -fr example_producer_with_transactinoal_message
	rm -fr example_simple_consumer