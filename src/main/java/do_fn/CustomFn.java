package do_fn;

import org.apache.beam.repackaged.direct_java.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;

public class CustomFn extends DoFn {



    StateSpec<ValueState<String>> elems = StateSpecs.value();

    ValueState<Integer> xx;



    public static void main(String args[]){
        DoFn x = new CustomFn();


        Pipeline p;








    }


    @StartBundle
    void startBundle(){

    }
    @ProcessElement
    void process(ProcessContext ctx){

    }
}



