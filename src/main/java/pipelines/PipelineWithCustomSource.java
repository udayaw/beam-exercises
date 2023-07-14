package pipelines;

import source.BoundedRandomStringSource;
import common.FSUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

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
