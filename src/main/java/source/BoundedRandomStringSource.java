package source;

import org.apache.beam.repackaged.core.org.apache.commons.lang3.RandomStringUtils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.OffsetBasedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

public class BoundedRandomStringSource {



    public static PTransform<PBegin, PCollection<String>> getStaticValueOf(String val){
        return Create.ofProvider(ValueProvider.StaticValueProvider.of(val), StringUtf8Coder.of());
    }


    public static  PTransform<PBegin, PCollection<String>> getRandomCreateOf(int size){
        return Create.of(() -> new Iterator<String>(){
            private int i = 0;

            @Override
            public boolean hasNext() {
                return ++i < size;
            }

            @Override
            public String next() {
                return RandomStringUtils.randomAscii(5);
            }
        }).withCoder(StringUtf8Coder.of());

    }


    public static PTransform<PBegin, PCollection<String>> getRandomOffsetBasedSource(int size){


        class RandomStringOffsetBasedSource extends OffsetBasedSource<String> {

            private long count = 0;

            RandomStringOffsetBasedSource(long count) {
                super(0, count, 5);
                this.count = count;
            }

            @Override
            public Coder<String> getOutputCoder() {
                return StringUtf8Coder.of();
            }

            @Override
            public long getMaxEndOffset(PipelineOptions options) throws Exception {
                return getEndOffset();
            }

            @Override
            public OffsetBasedSource<String> createSourceForSubrange(long start, long end) {
                return new RandomStringOffsetBasedSource(end - start);
            }

            @Override
            public BoundedReader<String> createReader(PipelineOptions options) throws IOException {
                return new RandomStringBoundedReader(count, this, options);
            }

        }


        return Read.from(new RandomStringOffsetBasedSource(100));
    }

    public static  PTransform<PBegin, PCollection<String>> getRandomBoundedSource(int size){

         class RandomStringBoundedSource extends BoundedSource<String>{

             long wordCount;

            public RandomStringBoundedSource(long wordCount){
                this.wordCount = wordCount;
            }

             @Override
             public Coder<String> getOutputCoder(){
                 return StringUtf8Coder.of();
             }

            @Override
            public List<? extends BoundedSource<String>> split(long desiredBundleSizeBytes, PipelineOptions options) throws Exception {

                List<RandomStringBoundedSource> splits = new LinkedList<>();

                long desiredWordsPerBundle = desiredBundleSizeBytes / getWordSize();

                for(long i = 0; i < wordCount; i+=desiredWordsPerBundle ){
                    splits.add(new RandomStringBoundedSource(Long.min(wordCount - i, desiredWordsPerBundle)));
                }

                return splits;
            }

            @Override
            public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
                return wordCount * getWordSize();
            }

            @Override
            public BoundedReader<String> createReader(PipelineOptions options) throws IOException {
                return new RandomStringBoundedReader(wordCount, this, options);
            }

            public long getWordSize(){
                return 5l;
            }
        }

        return Read.from(new RandomStringBoundedSource(size));
    }

    public static class RandomStringBoundedReader extends BoundedSource.BoundedReader<String> {

        private BoundedSource<String> source;
        private PipelineOptions options;

        private long count = 0;

        int i = 0;
        RandomStringBoundedReader(long count, BoundedSource<String> source, PipelineOptions options){
            this.source = source;
            this.options = options;
            this.count = count;
        }


        @Override
        public boolean start() throws IOException {
            return i < count;
        }

        @Override
        public boolean advance() throws IOException {
            return ++i < count;
        }

        @Override
        public String getCurrent() throws NoSuchElementException {
            return RandomStringUtils.randomAscii(5);
        }

        @Override
        public void close() throws IOException {
            //nothing to close here.
        }

        @Override
        public BoundedSource<String> getCurrentSource() {
            return source;
        }
    }

}
