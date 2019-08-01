
var sleep = require('sleep');
const jaeger = require('jaeger-client');
const initTracer = jaeger.initTracer;
const opentracing = require('opentracing');


var config = {
  serviceName: 'my-awesome-service',
  sampler: {
    type: "const",
    param: 1
  },
  reporter: {
    collectorEndpoint: 'http://localhost:14268/api/traces',
  }
};
var options = {
  tags: {
    'my-awesome-service.version': '1.1.2',
  },
  contextKey: "ubiservices-tracing"
};
var tracer = initTracer(config, options);

const span = tracer.startSpan('jaga_run');

sleep.msleep(10);

const function_span = tracer.startSpan("jaga_function", {childOf: span});

function_span.setTag(opentracing.Tags.SPAN_KIND, "producer");
function_span.setTag("message_bus.destination", "redis:message");

sleep.msleep(10);

var metadata = {};
tracer.inject(function_span, opentracing.FORMAT_TEXT_MAP, metadata);

var redis = require("redis"),
    client = redis.createClient();

client.set("message", JSON.stringify({meta: metadata, message: "HI"}), redis.print);


function_span.finish();

span.finish();

client.get("message", function(err, reply) {
    if(reply !== null){
      const message = JSON.parse(reply);
      console.log("Received message " + message.message);
      const transferred_span = tracer.extract(opentracing.FORMAT_TEXT_MAP, message.meta);
      console.log(message.meta);
      console.log(transferred_span);

      const span_consumer = tracer.startSpan("jaga_consumer", {
        references: [
          opentracing.followsFrom(transferred_span)
        ]});
      span_consumer.setTag(opentracing.Tags.SPAN_KIND, "consumer");
      span_consumer.setTag("message_bus.destination", "redis:message");

      setTimeout(function(){
        const span_consumer_function = tracer.startSpan("jaga_consumer_function", {childOf: span_consumer});
        span_consumer_function.setTag(opentracing.Tags.COMPONENT, "some component");
        sleep.msleep(10);
        span_consumer_function.finish();

        sleep.msleep(10);
        span_consumer.finish();

        setTimeout(
          function(){
          client.del("message",
            function(){process.exit();}
            );}
          , 1000);
      }, 100);
    }
});

