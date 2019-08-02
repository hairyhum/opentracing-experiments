/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package experiment.opentracing;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import io.jaegertracing.Configuration;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMap;
import io.opentracing.propagation.TextMapAdapter;

import java.util.HashMap;
import java.util.Map;

/**
 * WordCount example from flink examples extended with opentracing
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final ParameterTool params = ParameterTool.fromArgs(args);
		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */
		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		// get input data
		DataStream<String> text;
		if (params.has("input")) {
			// read the text file from given input path
			text = env.readTextFile(params.get("input"));
		} else {
			System.out.println("Executing WordCount example with default input data set.");
			System.out.println("Use --input to specify file input.");
			// get default test text data
			text = env.fromElements(WORDS);
		}


		DataStream<Tuple2<String, Integer>> counts =
			// split up the lines in pairs (2-tuples) containing: (word,1)
			text.map(
				new MapFunction<String, Tuple2<Map<String, String>, String>>(){
					@Override
					public Tuple2<Map<String, String>, String> map(String value) throws Exception{
						Tracer tracer = getTracer();

						Span mapFunSpan = tracer.buildSpan("map_step")
												.withTag("component", "flink")
												.withTag("word", value)
												.start();

						System.out.println("Empty map.");
						mapFunSpan.finish();
						Map<String, String> serializedMapFunSpan = serializeSpan(mapFunSpan, tracer);
						// Force close the tracer to make it send all reports.
						tracer.close();
						return new Tuple2(serializedMapFunSpan, value);
					}
				}).flatMap(
				new FlatMapFunction<Tuple2<Map<String, String>, String>, Tuple2<String, Integer>>(){
					@Override
					public void flatMap(Tuple2<Map<String, String>, String> value, Collector<Tuple2<String, Integer>> out) {
						Map<String, String> serialized = value.f0;
						Tracer tracer = getTracer();
						SpanContext parentSpanContext = deserializeSpan(serialized, tracer);
						Span mapFunSpan = tracer.buildSpan("flatmap_step")
												.asChildOf(parentSpanContext)
												.withTag("component", "flink")
												.withTag("word", value.f1)
												.start();
						// normalize and split the line
						String[] tokens = value.f1.toLowerCase().split("\\W+");

						// emit the pairs
						for (String token : tokens) {
							if (token.length() > 0) {
								out.collect(new Tuple2<>(token, 1));
							}
						}
						mapFunSpan.finish();
						tracer.close();
					}
				})
			// group by the tuple field "0" and sum up tuple field "1"
			.keyBy(0).sum(1);

		// emit result
		if (params.has("output")) {
			counts.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			counts.print();
		}

		// execute program
		env.execute("Streaming WordCount");
	}

	public static Map<String, String> serializeSpan(Span span, Tracer tracer){
		Map<String, String> map = new HashMap<>();
		TextMap spanCarrier = new TextMapAdapter(map);
		tracer.inject(span.context(), Format.Builtin.TEXT_MAP, spanCarrier);
		return map;
	}

	public static SpanContext deserializeSpan(Map<String, String> map, Tracer tracer){
		TextMap sourceCarrier = new TextMapAdapter(map);
		SpanContext rootSpanContext = tracer.extract(Format.Builtin.TEXT_MAP, sourceCarrier);
		return rootSpanContext;
	}

	public static Tracer getTracer(){
		Configuration configuration =  new Configuration("word_processing")
			.withReporter(new Configuration.ReporterConfiguration()
								.withSender(new Configuration.SenderConfiguration()
												.withEndpoint("http://172.20.0.4:14268/api/traces")
												// .withAgentHost("localhost")
												// .withAgentPort(6831)
												))
			.withSampler(new Configuration.SamplerConfiguration().withType("const").withParam(1));
		return configuration.getTracer();
	}

	public static final String[] WORDS = new String[] {
		"To be, or not to be,--that is the question:--",
		"Whether 'tis nobler in the mind to suffer",
		"The slings and arrows of outrageous fortune",
		"Or to take arms against a sea of troubles,",
		"And by opposing end them?--To die,--to sleep,--",
		"No more; and by a sleep to say we end",
		"The heartache, and the thousand natural shocks",
		"That flesh is heir to,--'tis a consummation",
		"Devoutly to be wish'd. To die,--to sleep;--",
		"To sleep! perchance to dream:--ay, there's the rub;",
		"For in that sleep of death what dreams may come,",
		"When we have shuffled off this mortal coil,",
		"Must give us pause: there's the respect",
		"That makes calamity of so long life;",
		"For who would bear the whips and scorns of time,",
		"The oppressor's wrong, the proud man's contumely,",
		"The pangs of despis'd love, the law's delay,",
		"The insolence of office, and the spurns",
		"That patient merit of the unworthy takes,",
		"When he himself might his quietus make",
		"With a bare bodkin? who would these fardels bear,",
		"To grunt and sweat under a weary life,",
		"But that the dread of something after death,--",
		"The undiscover'd country, from whose bourn",
		"No traveller returns,--puzzles the will,",
		"And makes us rather bear those ills we have",
		"Than fly to others that we know not of?",
		"Thus conscience does make cowards of us all;",
		"And thus the native hue of resolution",
		"Is sicklied o'er with the pale cast of thought;",
		"And enterprises of great pith and moment,",
		"With this regard, their currents turn awry,",
		"And lose the name of action.--Soft you now!",
		"The fair Ophelia!--Nymph, in thy orisons",
		"Be all my sins remember'd."
	};
}
