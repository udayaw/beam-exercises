package custom_beam_source;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.POutput;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Windowing extends Pipeline {

    private final Logger LOGGER = LoggerFactory.getLogger(Windowing.class);
    public Windowing(PipelineOptions options){
        super(options);


        apply(BoundedRandomStringSource.withDelayedStrings(new String[]{"a","b"}, new String[]{"c", "d"}, 2))


                .apply(Window.<String>into(
                        FixedWindows.of(Duration.standardMinutes(10))
                )


//                .triggering(
//                        AfterPane.elementCountAtLeast(1)
//                )

//                .triggering(AfterWatermark.pastEndOfWindow())


//                .triggering(AfterWatermark.pastEndOfWindow()
//                    .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane())
//                    .withLateFirings(AfterPane.elementCountAtLeast(1))
//                )




                ).apply(ParDo.of(new DoFn<String, PDone>() {

                    @ProcessElement
                    public void processElement(String e, OutputReceiver<PDone> out){
                        LOGGER.info(e.toString());

                        out.output(PDone.in(Windowing.this));
                    }

                }));

    }


    public static void main(String args[]){
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline p = new PipelineWithCustomSource(options);
        p.run().waitUntilFinish();
    }
}
