package info.dminaev;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class OrderingWithinWindow
{
    private static final long WINDOW_SIZE = 30L;

    private static final long SLIDE_SIZE = WINDOW_SIZE / 2;

    @Rule
    public TestPipeline p = TestPipeline.create();

    // @formatter:off
    /**
     * This is a test to show how we can do ordering of elements inside a window.
     * It can be useful if we badly need to order messages coming from Google PubSub (where order is not guaranteed).
     * The concept is to put all elements into small sliding windows (e.g. a few seconds) and then sort elements within
     * the window. Therefore achieving at least some kind of order, however there is definitely no guarantee that all
     * the messages will be in a correct order if there is a latency of more than a window size and order is incorrect.
     *
     *
     *                                +----- 4w ----+
     *                         +----- 3w ----+
     *                   +---- 2w ----+
     *           +----- 1w ----+
     *    +----- 0w ----+
     *    +------+------+------+------+------+------+
     *    |      |      |      |      |      |      |
     * ---|------|------|------|------|------|------+---
     *          0s     15s    30s    45s    60s    75s
     *           ^   ^     ^    ^   ^    ^
     *          ev1 ev2    ^   ev4 ev3  ev5       --- processing time
     *          ev1 ev2   ev3  ev4 ev5            --- event time
     *
     * ::: ev1, ev2, ev3, ev4 - events
     * ::: 0w, 1w, 2w, 3w     - sliding windows with size of 30 seconds and slide of 15 seconds
     *
     * ev1 & ev2 belong to both 0w and 1w but their timestamps are after the middle of 0w
     * and before the middle of the 1w, hence we're firing them at the end of 1w
     */
    // @formatter:on
    @Test
    public void test1()
    {
        /*
         * define timestamps of windows
         */

        // get current timestamp rounded to the beginning of the nearest window size
        Instant startOf1w = Instant.ofEpochMilli(
                System.currentTimeMillis() / (WINDOW_SIZE * 1000) * (WINDOW_SIZE * 1000)); //  0.000 ms
        Instant endOf0w = startOf1w.plus(Duration.standardSeconds(SLIDE_SIZE)).minus(1);   // 14.999 ms
        Instant startOf2w = endOf0w.plus(1);                                               // 15.000 ms
        Instant endOf1w = startOf1w.plus(Duration.standardSeconds(WINDOW_SIZE).minus(1));  // 29.999 ms
        Instant startOf3w = endOf1w.plus(1);                                               // 30.000 ms
        Instant endOf2w = startOf2w.plus(Duration.standardSeconds(WINDOW_SIZE)).minus(1);  // 44.999 ms
        Instant endOf3w = startOf3w.plus(Duration.standardSeconds(WINDOW_SIZE)).minus(1);  // 59.999 ms
        Instant endOf4w = endOf3w.plus(Duration.standardSeconds(SLIDE_SIZE));              // 74.999 ms

        /*
         * define timestamps of events
         */

        //noinspection PointlessArithmeticExpression
        Duration duration1 = Duration.millis((long) (WINDOW_SIZE * (1.0 / 30.0)) * 1000);         //  1s
        Duration duration2 = Duration.millis((long) (WINDOW_SIZE * (1.0 / 3.0)) * 1000);          // 10s
        Duration duration3 = Duration.millis((long) (WINDOW_SIZE * (2.0 / 3.0)) * 1000);          // 20s
        Duration duration4 = Duration.millis(WINDOW_SIZE * 1000).plus(duration1);                 // 31s
        Duration duration5 = Duration.millis(WINDOW_SIZE * 1000).plus(duration1).plus(duration1); // 32s
        Duration duration6 = Duration.millis(WINDOW_SIZE * 1000).plus(duration3);                 // 50s

        Instant ev1EventTs = startOf1w.plus(duration1);
        Instant ev2EventTs = startOf1w.plus(duration2);
        Instant ev3EventTs = startOf1w.plus(duration3);
        Instant ev3ProcessingTs = startOf1w.plus(duration5);
        Instant ev4EventTs = startOf1w.plus(duration4);
        //noinspection UnnecessaryLocalVariable
        Instant ev4ProcessingTs = ev4EventTs;
        //noinspection UnnecessaryLocalVariable
        Instant ev5EventTs = ev3ProcessingTs;
        Instant ev5ProcessingTs = startOf1w.plus(duration6);

        /*
         * build events
         */

        Event event1 = new Event(ev1EventTs.getMillis(), "ev1");
        Event event2 = new Event(ev2EventTs.getMillis(), "ev2");
        Event event3 = new Event(ev3EventTs.getMillis(), "ev3");
        Event event4 = new Event(ev4EventTs.getMillis(), "ev4");
        Event event5 = new Event(ev5EventTs.getMillis(), "ev5");

        /*
         * create a stream of events with timestamps
         */

        TestStream<Event> stream =
                TestStream.create(SerializableCoder.of(Event.class))
                        .advanceWatermarkTo(ev1EventTs)      // 1s  - ev1
                        .addElements(event1)
                        .advanceWatermarkTo(ev2EventTs)      // 10s - ev2
                        .addElements(event2)
                        .advanceWatermarkTo(endOf0w)         // 15s - to fire 0th window (no elements included)
                        // ...
                        .advanceWatermarkTo(endOf1w)         // 30s - to fire 1st window (ev1 & ev2 included)
                        // ...
                        .advanceWatermarkTo(ev4ProcessingTs) // 31s - ev4
                        .addElements(event4)
                        .advanceWatermarkTo(ev3ProcessingTs) // 32s - ev3
                        .addElements(event3)
                        // ...
                        .advanceWatermarkTo(endOf2w)         // 45s - to fire 2nd window (no events included)
                        // ...
                        .advanceWatermarkTo(ev5ProcessingTs) // 50s - ev5
                        .addElements(event5)
                        // ...
                        .advanceWatermarkTo(endOf3w)         // 60s - to fire 3rd window (ev3 & ev4 included)
                        // ...
                        .advanceWatermarkTo(endOf4w)         // 75s - to fire 4th window (ev5 included)
                        .advanceWatermarkToInfinity();

        /*
         * create a streaming pipeline
         */

        PCollection<Event> result = p.apply(stream)
                .apply("create a sliding window", Window.into(SlidingWindows.of(Duration.standardSeconds(WINDOW_SIZE))
                        .every(Duration.standardSeconds(SLIDE_SIZE))))
                .apply("group by window", ParDo.of(new GroupByWindow()))
                .apply("apply group by", GroupByKey.create())
                .apply("output elements", ParDo.of(new Dummy()));

        /*
         * assert results
         */

        // @formatter:off
        PAssert.that(result)
                .satisfies(new CompareArrays(new ArrayList<>(
                        Arrays.asList(
                                new Event(endOf0w.getMillis(), endOf0w.toString()),
                                event1,
                                event2,
                                new Event(endOf1w.getMillis(), endOf1w.toString()),
                                event3,
                                new Event(endOf2w.getMillis(), endOf2w.toString()),
                                event4,
                                event5,
                                new Event(endOf3w.getMillis(), endOf3w.toString()),
                                new Event(endOf4w.getMillis(), endOf4w.toString())))));
        // @formatter:on

        /*
         * run pipeline
         */

        p.run().waitUntilFinish();
    }

    public static class GroupByWindow
            extends DoFn<Event, KV<Long, Event>>
    {
        @ProcessElement
        public void processElement( ProcessContext context, BoundedWindow window )
        {
            context.output(KV.of(window.maxTimestamp().getMillis(), context.element()));
        }
    }

    public static class Dummy
            extends DoFn<KV<Long, Iterable<Event>>, Event>
    {
        @ProcessElement
        public void processElement( ProcessContext context )
        {
            KV<Long, Iterable<Event>> element = context.element();

            if ( element.getKey() == null ) {
                return;
            }

            /*
             * Here we've got an iterable of elements from the particular window, e.g. ev1 and ev2 are in the first
             * window, they'll all come to this operator as a single <code>Iterable</code>.
             * In case you want to sort these elements you can do it right here.
             */

            long windowEndTimestamp = element.getKey();
            long windowStartTimestamp = windowEndTimestamp - WINDOW_SIZE * 1000 + 1;
            long middleOfTheWindow = windowStartTimestamp + SLIDE_SIZE * 1000;

            // sort events by event timestamp and include only those that is between the window start and the middle
            List<Event> result = new ArrayList<>();
            for ( Event s : element.getValue() ) {
                if ( windowStartTimestamp <= s.getTimestamp() && s.getTimestamp() < middleOfTheWindow ) {
                    result.add(s);
                }
            }

            result.sort(Comparator.comparing(Event::getTimestamp));

            for ( Event event : result ) {
                context.output(event);
            }

            // output window end timestamp
            Instant timestamp = Instant.ofEpochMilli(windowEndTimestamp);
            Event event = new Event(windowEndTimestamp, String.valueOf(timestamp));
            context.output(event);
        }
    }

    public static class CompareArrays
            implements SerializableFunction<Iterable<Event>, Void>
    {
        private List<Event> expected;

        CompareArrays( List<Event> expected )
        {
            this.expected = expected;
        }

        @Override
        public Void apply( Iterable<Event> input )
        {
            List<Event> actual = new ArrayList<>();
            for ( Event s : input ) {
                actual.add(s);
            }

            Assert.assertArrayEquals(expected.toArray(new Event[0]), actual.toArray(new Event[0]));

            return null;
        }
    }
}
