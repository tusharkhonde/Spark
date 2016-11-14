package com.example.Spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

public class EndingWith {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> listRdd = context.parallelize(Arrays.asList("Ironman",
				"Batman", "Flash", "Superman", "io_corp", "bee_corp",
				"state_corp", "ib_inc"));
		System.out.print("ListRDD before calling an action: ");
        listRdd.foreach(new VoidFunction<String>(){
            public void call(String line) {
                System.out.print(line + " ");
            }});

		JavaRDD<String> endingWithSh = listRdd
				.filter(new Function<String, Boolean>() {

					private static final long serialVersionUID = 1L;

					public Boolean call(String item) throws Exception {
						if (item.endsWith("man"))
							return true;
						return false;
					}
				});
		System.out.println(endingWithSh.collect());

		JavaRDD<String> endingWithLabs = listRdd
				.filter(new Function<String, Boolean>() {

					private static final long serialVersionUID = 1L;

					public Boolean call(String item) throws Exception {
						if (item.endsWith("corp"))
							return true;
						return false;
					}
				});

		System.out.println(endingWithLabs.collect());
		context.close();
	}
}