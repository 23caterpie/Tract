# Example App

This is an example app that shows off featues of tracts.

A good starting point is to start the app and some tracing/metrics dbs in docker and open all UIs to them.
The base target of the makefile will do this for you:
```sh
make
```

From here you can send the app some messages on its kafka queue and view the metrics and traces that show up!

![Example of viewing traces in Zipkin's UI](/img/zipkin.png)
