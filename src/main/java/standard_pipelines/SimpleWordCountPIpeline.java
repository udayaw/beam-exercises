package standard_pipelines;

import common.FSUtils;
import custom_beam_source.BoundedRandomStringSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.util.ArrayList;
import java.util.Arrays;

public class SimpleWordCountPIpeline extends Pipeline {

    public SimpleWordCountPIpeline(PipelineOptions ops){
        super(ops);

        countWordsApproach1();
    }

    void countWordsApproach1(){



        apply(BoundedRandomStringSource.getRandomBoundedSource(100))
                .apply(MapElements.into(TypeDescriptors.lists(TypeDescriptors.strings()))
                        .via(s->new ArrayList<>(Arrays.asList(s.split("\\s"))))

                )
//        .apply(MapElements.via(
//            **this requires SimpleWordCountPipeline to implement Serializable.
//           **to avoid create separate class(non nested in the pipeline) extends SimpleFunction
//            new SimpleFunction<String, ArrayList<String>>(){
//                @Override
//                public ArrayList<String> apply(String input) {
//                    return new ArrayList<>(Arrays.asList(input.split("\\s")));
//                }
//            }
//        ))
        .apply(FlatMapElements
                .into(TypeDescriptors.strings())
                    .via(s->s))
        .apply(Count.perElement())
        .apply(MapElements.into(TypeDescriptors.strings()).via(kv->
                kv.getKey() + "," + kv.getValue()))
        .apply(TextIO.write().to(FSUtils.getOutputDirectory(this.getClass())).withNumShards(1));
    }

    void countWordsApproach2(){
        apply(BoundedRandomStringSource.getRandomBoundedSource(100))
        .apply(FlatMapElements.into(TypeDescriptors.strings()).via(s-> Arrays.asList(s.split("\\s"))))
        .apply(Count.perElement())
        .apply(MapElements.into(TypeDescriptors.strings()).via(kv->
                kv.getKey() + "," + kv.getValue()))
        .apply(TextIO.write().to(FSUtils.getOutputDirectory(this.getClass())).withNumShards(1));
    }

    public static void main(String[] args){
        new SimpleWordCountPIpeline(PipelineOptionsFactory.fromArgs(args).create())
                .run().waitUntilFinish();
    }

}
