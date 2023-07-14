package custom_beam_source;

import common.FSUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class PipelineWithCustomSource extends Pipeline {


    protected PipelineWithCustomSource(PipelineOptions options) {
        super(options);
        apply(BoundedRandomStringSource.getRandomBoundedSource(100))
            .apply(TextIO.write().withNumShards(1).to(FSUtils.getOutputDirectory(this.getClass())));
    }


    public static void main(String[] args){

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline p = new PipelineWithCustomSource(options);
        p.run().waitUntilFinish();
    }
}
