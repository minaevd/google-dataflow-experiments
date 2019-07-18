Here I'm experimenting with Google Dataflow and trying to solve various problems.
- When consuming messages from PubSub order is not guaranteed. If you need it badly you can try to put all elements into 
small sliding windows and then sort elements within each window. That way you can achieve at least some kind of order, 
however there is definitely no guarantee that all the messages will be in a correct order if there is a latency of more 
than a window size between messages that come in incorrect order. 
More about it in this test: [src/test/java/info/dminaev/OrderingWithinWindow.java](https://github.com/minaevd/google-dataflow-experiments/blob/master/src/test/java/info/dminaev/OrderingWithinWindow.java)
