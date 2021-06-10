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
package pro;

import static com.fasterxml.jackson.databind.util.ClassUtil.defaultValue;
import static com.google.common.base.Defaults.defaultValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import static org.apache.avro.SchemaBuilder.array;
import static org.apache.commons.math3.analysis.FunctionUtils.collector;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.StringValue;
import com.google.common.math.Quantiles;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import static java.lang.Math.abs;
import java.lang.reflect.Array;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.OptionalDouble;
import java.util.stream.Collectors;
import org.apache.commons.collections4.IterableUtils;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.shaded.curator4.org.apache.curator.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction.Context;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import static org.apache.flink.table.api.scala.ImplicitExpressionOperations$class.avg;
import org.javatuples.Sextet;
//import utils.DateUtilities;

public class PSD {

    static FileWriter myWriterAverage;
    static PrintWriter printWriterAverage;
    static FileWriter myWriterMedian;
    static PrintWriter printWriterMedian;
    static FileWriter myWriterQuantile;
    static PrintWriter printWriterQuantile;
    static FileWriter myWriterAverage10;
    static PrintWriter printWriterAverage10;
    static FileWriter myWriterSafetyMeas;
    static PrintWriter printWriterSafetyMeas;
    static FileWriter myWriterGini;
    static PrintWriter printWriterGini;
    static int windowSize = 30;

    static double totalScore[] = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
    static double totalScore1 = 0.0;
    static double totalScore2 = 0;
    static double totalScore3 = 0;
    static double totalScore4 = 0;
    static double totalScore5 = 0;
    static double totalScore6 = 0;
    static double totalScore7 = 0;

    static int iteration = 0;
    static HashMap<Integer, ArrayList<Double>> sortedLists = new HashMap<Integer, ArrayList<Double>>();

    static HashMap<Integer, Double[]> first3ElementsList = new HashMap<Integer, Double[]>();

    static ArrayList<Double> myList0 = new ArrayList<>();
    static ArrayList<Double> myList1 = new ArrayList<>();
    static ArrayList<Double> myList2 = new ArrayList<>();
    static ArrayList<Double> myList3 = new ArrayList<>();
    static ArrayList<Double> myList4 = new ArrayList<>();
    static ArrayList<Double> myList5 = new ArrayList<>();
    static ArrayList<Double> myList6 = new ArrayList<>();

    static boolean first = true;
    static boolean file = true;
    static Tuple7<Double, Double, Double, Double, Double, Double, Double> firstInWindow = new Tuple7<>();
    static Tuple7<Double, Double, Double, Double, Double, Double, Double> avg = new Tuple7<>();
    static Tuple7<Double, Double, Double, Double, Double, Double, Double> alertAvg = new Tuple7<>();
    static Tuple7<Double, Double, Double, Double, Double, Double, Double> median = new Tuple7<>();
    static Tuple7<Double, Double, Double, Double, Double, Double, Double> alertMedian = new Tuple7<>();
    static Tuple7<Double, Double, Double, Double, Double, Double, Double> quantiles = new Tuple7<>();
    static Tuple7<Double, Double, Double, Double, Double, Double, Double> alertQuantiles = new Tuple7<>();
    static Tuple7<Double, Double, Double, Double, Double, Double, Double> avg10 = new Tuple7<>();
    static Tuple7<Double, Double, Double, Double, Double, Double, Double> alertAvg10 = new Tuple7<>();
    static Tuple7<Double, Double, Double, Double, Double, Double, Double> safetyMeas = new Tuple7<>(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
    static Tuple7<Double, Double, Double, Double, Double, Double, Double> alertSafetyMeas = new Tuple7<>();
    static Tuple7<Double, Double, Double, Double, Double, Double, Double> safetyMeasGini = new Tuple7<>(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
    static Tuple7<Double, Double, Double, Double, Double, Double, Double> alertGini = new Tuple7<>();

    static Tuple7<Double, Double, Double, Double, Double, Double, Double> avg1mln = new Tuple7<>(0.0001591172, 8.137793e-05, 0.0001132182, 2.553861e-05, 8.406755e-05, 1.850137e-05, 8.903373e-05);
    static Tuple7<Double, Double, Double, Double, Double, Double, Double> median1mln = new Tuple7<>(0.0002782579, 0.000112419, 0.0001390352, 1.887087e-05, 6.102744e-05, 4.919219e-05, 9.334836e-05);
    static Tuple7<Double, Double, Double, Double, Double, Double, Double> quantile1mln = new Tuple7<>(-0.0797541, -0.07993142, -0.08001057, -0.07994938, -0.07989199, -0.08001, -0.03131036);
    static Tuple7<Double, Double, Double, Double, Double, Double, Double> avg101mln = new Tuple7<>(-0.08984917, -0.09000622, -0.09003263, -0.08999049, -0.0899371, -0.09001109, -0.04203614);
    static Tuple7<Double, Double, Double, Double, Double, Double, Double> safetyMeas1mln = new Tuple7<>(-0.0247547, -0.02493022, -0.02489311, -0.02496709, -0.02489756, -0.02497301, -0.00965448);
    static Tuple7<Double, Double, Double, Double, Double, Double, Double> gini1mln = new Tuple7<>(-0.03308185, -0.03326617, -0.03323318, -0.03329781, -0.03323261, -0.0333098, -0.03323927);
//            

//            
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple7<Double, Double, Double, Double, Double, Double, Double>> data = env.readTextFile("probki_i_portfel.csv").flatMap(new FlatMapFunction<String, Tuple7<Double, Double, Double, Double, Double, Double, Double>>() {
            @Override
            public void flatMap(String line, Collector<Tuple7<Double, Double, Double, Double, Double, Double, Double>> out) throws Exception {
                String[] nums = line.split(";");
                ArrayList<Double> myList1 = new ArrayList<>();

                for (String word : nums) {
                    myList1.add(Double.parseDouble(word));

                }
                out.collect(new Tuple7<>(myList1.get(0), myList1.get(1), myList1.get(2), myList1.get(3), myList1.get(4), myList1.get(5), myList1.get(6))
                );
            }
        });

        System.out.println(data);
        DataStreamSink<List<Tuple7<Double, Double, Double, Double, Double, Double, Double>>> reduced
                = data
                        .countWindowAll(windowSize, 1).apply(new Computations())
                        .print();

        env.execute(
                "Window PSD");

        printWriterAverage.close();
        printWriterMedian.close();
        printWriterQuantile.close();
        printWriterAverage10.close();
        printWriterSafetyMeas.close();
        printWriterGini.close();

    }

    public static class Computations implements AllWindowFunction<Tuple7<Double, Double, Double, Double, Double, Double, Double>, List<Tuple7<Double, Double, Double, Double, Double, Double, Double>>, GlobalWindow> {

        @Override
        public void apply(GlobalWindow window, Iterable<Tuple7<Double, Double, Double, Double, Double, Double, Double>> values, Collector<List<Tuple7<Double, Double, Double, Double, Double, Double, Double>>> out) throws IOException {

            safetyMeas = new Tuple7<>(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
            safetyMeasGini = new Tuple7<>(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);

            if (file) {
                myWriterAverage = new FileWriter("average.txt");
                printWriterAverage = new PrintWriter(myWriterAverage);
                myWriterMedian = new FileWriter("median.txt");
                printWriterMedian = new PrintWriter(myWriterMedian);
                myWriterQuantile = new FileWriter("quantile.txt");
                printWriterQuantile = new PrintWriter(myWriterQuantile);
                myWriterAverage10 = new FileWriter("average10.txt");
                printWriterAverage10 = new PrintWriter(myWriterAverage10);
                myWriterSafetyMeas = new FileWriter("safetyMeas.txt");
                printWriterSafetyMeas = new PrintWriter(myWriterSafetyMeas);
                myWriterGini = new FileWriter("gini.txt");
                printWriterGini = new PrintWriter(myWriterGini);
                file = false;
            } else {

                if (IterableUtils.size(values) == windowSize) {
                    if (first) {

                        for (Tuple7<Double, Double, Double, Double, Double, Double, Double> val : values) {

                            for (int i = 0; i < 7; i++) {

                                totalScore[i] += Double.valueOf(val.getField(i).toString());

                            }
                            myList0.add(val.f0);
                            myList1.add(val.f1);
                            myList2.add(val.f2);
                            myList3.add(val.f3);
                            myList4.add(val.f4);
                            myList5.add(val.f5);
                            myList6.add(val.f6);
                        }

                        for (int i = 0; i < 7; i++) {

                            avg.setField(totalScore[i] / windowSize, i);
                        }

                        sortedLists.put(0, myList0);
                        sortedLists.put(1, myList1);
                        sortedLists.put(2, myList2);
                        sortedLists.put(3, myList3);
                        sortedLists.put(4, myList4);
                        sortedLists.put(5, myList5);
                        sortedLists.put(6, myList6);

                        for (Integer i : sortedLists.keySet()) {

                            quickSort(sortedLists.get(i), 0, sortedLists.get(i).size() - 1);
                            first3ElementsList.put(i, sortedLists.get(i).stream().limit(3).collect(Collectors.toList()).toArray(new Double[3]));
                        }

                        firstInWindow = Iterables.get(values, 0);

                        first = false;

                    } else {

                        for (Integer i : sortedLists.keySet()) {

                            sortedLists.get(i).remove(firstInWindow.getField(i));
                            insertSorted(sortedLists.get(i), sortedLists.get(i).size() - 1, Iterables.getLast(values).getField(i));
                            if (first3ElementsList.get(i) != sortedLists.get(i).stream().limit(3).collect(Collectors.toList()).toArray(new Double[3])) {
                                first3ElementsList.put(i, sortedLists.get(i).stream().limit(3).collect(Collectors.toList()).toArray(new Double[3]));
                            }
                            avg.setField(Double.valueOf(avg.getField(i).toString()) - (Double.valueOf(firstInWindow.getField(i).toString()) - Double.valueOf(Iterables.getLast(values).getField(i).toString())) / windowSize, i);

                        }

                        firstInWindow = Iterables.get(values, 0);

                    }

                    for (Integer i : sortedLists.keySet()) {

                        median.setField((sortedLists.get(i).get(14) + sortedLists.get(i).get(15)) / 2, i);
                        quantiles.setField(sortedLists.get(i).get(3), i);
                        avg10.setField(Arrays.stream(first3ElementsList.get(i)).mapToDouble((x) -> x).average().orElse(0.0), i);

                    }

                    for (Tuple7<Double, Double, Double, Double, Double, Double, Double> val : values) {
                        safetyMeas.f0 += (abs(val.f0 - avg.f0));
                        safetyMeas.f1 += (abs(val.f1 - avg.f1));
                        safetyMeas.f2 += (abs(val.f2 - avg.f2));
                        safetyMeas.f3 += (abs(val.f3 - avg.f3));
                        safetyMeas.f4 += (abs(val.f4 - avg.f4));
                        safetyMeas.f5 += (abs(val.f5 - avg.f5));
                        safetyMeas.f6 += (abs(val.f6 - avg.f6));

                    }

                    safetyMeas = new Tuple7<>(avg.f0 - safetyMeas.f0 / (2 * windowSize),
                            avg.f1 - safetyMeas.f1 / (2 * windowSize),
                            avg.f2 - safetyMeas.f2 / (2 * windowSize),
                            avg.f3 - safetyMeas.f3 / (2 * windowSize),
                            avg.f4 - safetyMeas.f4 / (2 * windowSize),
                            avg.f5 - safetyMeas.f5 / (2 * windowSize),
                            avg.f6 - safetyMeas.f6 / (2 * windowSize)
                    );

                    for (Integer i : sortedLists.keySet()) {
                        int gini = 1;
                        double temp = 0;
                        for (Double el : sortedLists.get(i)) {
                            temp += (2 * gini - windowSize - 1) * el;

                            gini += 1;
                        }
                        safetyMeasGini.setField(temp, i);

                    }
                    safetyMeasGini = new Tuple7<>(avg.f0 - safetyMeasGini.f0 / Math.pow(windowSize, 2),
                            avg.f1 - safetyMeasGini.f1 / Math.pow(windowSize, 2),
                            avg.f2 - safetyMeasGini.f2 / Math.pow(windowSize, 2),
                            avg.f3 - safetyMeasGini.f3 / Math.pow(windowSize, 2),
                            avg.f4 - safetyMeasGini.f4 / Math.pow(windowSize, 2),
                            avg.f5 - safetyMeasGini.f5 / Math.pow(windowSize, 2),
                            avg.f6 - safetyMeasGini.f6 / Math.pow(windowSize, 2)
                    );

                    for (int i = 0; i < 7; i++) {

                        double overAvg = (Double.valueOf(avg1mln.getField(i).toString()) - Double.valueOf(avg.getField(i).toString())) / (1 + Double.valueOf(avg1mln.getField(i).toString()));
                        double overMedian = (Double.valueOf(median1mln.getField(i).toString()) - Double.valueOf(median.getField(i).toString())) / (1 + Double.valueOf(median1mln.getField(i).toString()));
                        double overQuantile = (Double.valueOf(quantile1mln.getField(i).toString()) - Double.valueOf(quantiles.getField(i).toString())) / (1 + Double.valueOf(quantile1mln.getField(i).toString()));
                        double overAvg10 = (Double.valueOf(avg101mln.getField(i).toString()) - Double.valueOf(avg10.getField(i).toString())) / (1 + Double.valueOf(avg101mln.getField(i).toString()));
                        double overSafetyMeas = (Double.valueOf(safetyMeas1mln.getField(i).toString()) - Double.valueOf(safetyMeas.getField(i).toString())) / (1 + Double.valueOf(safetyMeas1mln.getField(i).toString()));
                        double overGini = (Double.valueOf(gini1mln.getField(i).toString()) - Double.valueOf(safetyMeasGini.getField(i).toString())) / (1 + Double.valueOf(gini1mln.getField(i).toString()));

                        if ((Double.valueOf(avg.getField(i).toString()) < Double.valueOf(avg1mln.getField(i).toString())) & overAvg >= 0.01) {
                            alertAvg.setField(overAvg, i);

                        } else {
                            alertAvg.setField(0.0, i);
                        }
                        if ((Double.valueOf(median.getField(i).toString()) < Double.valueOf(median1mln.getField(i).toString())) && overMedian >= 0.01) {
                            alertMedian.setField(overMedian, i);
                        } else {
                            alertMedian.setField(0.0, i);
                        }
                        if ((Double.valueOf(quantiles.getField(i).toString()) < Double.valueOf(quantile1mln.getField(i).toString())) && overQuantile >= 0.01) {
                            alertQuantiles.setField(overQuantile, i);
                        } else {
                            alertQuantiles.setField(0.0, i);
                        }
                        if ((Double.valueOf(avg10.getField(i).toString()) < Double.valueOf(avg101mln.getField(i).toString())) && overAvg10 >= 0.01) {
                            alertAvg10.setField(overAvg10, i);
                        } else {
                            alertAvg10.setField(0.0, i);
                        }
                        if ((Double.valueOf(safetyMeas.getField(i).toString()) < Double.valueOf(safetyMeas1mln.getField(i).toString())) && overSafetyMeas >= 0.01) {
                            alertSafetyMeas.setField(overSafetyMeas, i);
                        } else {
                            alertSafetyMeas.setField(0.0, i);
                        }
                        if ((Double.valueOf(safetyMeasGini.getField(i).toString()) < Double.valueOf(gini1mln.getField(i).toString())) && overGini >= 0.01) {
                            alertGini.setField(overGini, i);
                        } else {
                            alertGini.setField(0.0, i);
                        }

                    }

                    System.out.println("Iteration");
                    System.out.println(iteration);
                    iteration++;
//                    System.out.println("SREDNIA");
//                    System.out.println(avg);
//                    System.out.println("median");
//                    System.out.println(median);
//                    System.out.println("quantiles");
//                    System.out.println(quantiles);
//                    System.out.println("avg10");
//                    System.out.println(avg10);
//                    System.out.println("safetyMeas");
//                    System.out.println(safetyMeas);
//                    System.out.println("safetyMeasGini");
//                    System.out.println(safetyMeasGini);

                    printWriterAverage.println(alertAvg.toString());
                    printWriterMedian.println(alertMedian.toString());
                    printWriterQuantile.println(alertQuantiles.toString());
                    printWriterAverage10.println(alertAvg10.toString());
                    printWriterSafetyMeas.println(alertSafetyMeas.toString());
                    printWriterGini.println(alertGini.toString());

                }
            }

        }
    }

    static int insertSorted(ArrayList<Double> arr, int n, double key) {

        int i;
        for (i = n; (i >= 0 && arr.get(i) > key); i--) {
            if (i + 1 > n) {
                arr.add(arr.get(i));
            } else {
                arr.set(i + 1, arr.get(i));
            }
        }

        if (i + 1 > n) {
            arr.add(key);
        } else {
            arr.set(i + 1, key);
        }

        return (n + 1);
    }

    static void swap(ArrayList<Double> arr, int i, int j) {

        double temp = arr.get(i);
        arr.set(i, arr.get(j));
        arr.set(j, temp);

    }

    static int partition(ArrayList<Double> arr, int low, int high) {

        // pivot
        double pivot = arr.get(high);

        int i = (low - 1);

        for (int j = low; j <= high - 1; j++) {

            if (arr.get(j) < pivot) {

                i++;
                swap(arr, i, j);
            }
        }
        swap(arr, i + 1, high);
        return (i + 1);
    }

    static void quickSort(ArrayList<Double> arr, int low, int high) {
        if (low < high) {

            int pi = partition(arr, low, high);

            quickSort(arr, low, pi - 1);
            quickSort(arr, pi + 1, high);
        }
    }

}
