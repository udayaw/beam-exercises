package pipelines;

import common.FSUtils;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.commons.io.FileUtils;
import org.joda.time.Instant;
import source.BoundedRandomStringSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.*;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import source.UnboundedRandomStringSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;

public class Windowing extends Pipeline {

    private final Logger LOGGER = LoggerFactory.getLogger(Windowing.class);
    public Windowing(PipelineOptions options) throws IOException {
        super(options);


        //cleanup targer dir first
        FileUtils.cleanDirectory(new File(FSUtils.getOutputDirectory(getClass())));


        //events 0s {a,b,c}, 0s {d,e,f}
        apply(
                getEventsWithOneSecondFrequencyAndTwoSecondDelay()
        //        getEventsWithSameTimestampAndTwoSecondDelay()
        )


                .apply(
                    Window.<String>into(FixedWindows.of(Duration.millis(1000)))
                .withAllowedLateness(Duration.millis(2000))
                .accumulatingFiredPanes() //accumulate to see the triggerings better

//              default window output : [a,b,c],[a,b,c,d],[a,b,c,d,e].....

                //not allowed : https://s.apache.org/finishing-triggers-drop-data, use with Repeatedly.forever()
//                .triggering(
//                    AfterPane.elementCountAtLeast(1)
//                )
//                .triggering(
//                    AfterWatermark.pastEndOfWindow()
//                )

                //but this allowed, same as default windowing
//                .triggering(
//                    AfterWatermark.pastEndOfWindow().withLateFirings(AfterPane.elementCountAtLeast(1))
//                )

//                not allowed! fires once first event processing time + 2secs
//                .triggering(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.millis(2000)))

                  //output, [a,b,c], [a...f],, gets fired at 1s,2s,3s....
                .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.millis(1000))))

                //output [a,b,c], [a,b,c], [a-f]
//                .triggering(AfterWatermark.pastEndOfWindow()
//                    .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane())
//                    .withLateFirings(AfterPane.elementCountAtLeast(1))
//                )
                )
                 .apply(TextIO.write().withWindowedWrites().withNumShards(1).to(FSUtils.getOutputDirectory(this.getClass())));

    }

    public static TestStream<String> getEventsWithSameTimestampAndTwoSecondDelay(){


        Instant eventTime = Instant.now();
        return TestStream.create(StringUtf8Coder.of())
                .advanceWatermarkTo(eventTime)
                .addElements(
                        TimestampedValue.of("a", eventTime),
                        TimestampedValue.of("b", eventTime),
                        TimestampedValue.of("c", eventTime)
                ).advanceProcessingTime(Duration.millis(2000))
                .advanceWatermarkTo(eventTime.plus(2000))
                .addElements(
                        TimestampedValue.of("e", eventTime),
                        TimestampedValue.of("f", eventTime),
                        TimestampedValue.of("g", eventTime)
                ).advanceWatermarkToInfinity();
    }

    public static TestStream<String> getEventsWithOneSecondFrequencyAndTwoSecondDelay(){


        Instant eventTime = Instant.now();
        return TestStream.create(StringUtf8Coder.of())
                .advanceWatermarkTo(eventTime)
                .addElements(
                        TimestampedValue.of("1", eventTime.plus(1000)),
                        TimestampedValue.of("2", eventTime.plus(2000)),
                        TimestampedValue.of("3", eventTime.plus(3000))
                ).advanceProcessingTime(Duration.millis(4000))
                .advanceWatermarkTo(eventTime.plus(4000))
                .addElements(
                        TimestampedValue.of("1", eventTime.plus(1000)),
                        TimestampedValue.of("2", eventTime.plus(2000)))
                .addElements(
                        TimestampedValue.of("4", eventTime.plus(4000)),
                        TimestampedValue.of("5", eventTime.plus(5000)),
                        TimestampedValue.of("6", eventTime.plus(6000))
                ).advanceWatermarkToInfinity();
    }


    public static void main(String args[]) throws IOException {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline p = new Windowing(options);
        p.run().waitUntilFinish(Duration.millis(0));
    }
}
