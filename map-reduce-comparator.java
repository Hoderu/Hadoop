package org.myorg;

import java.io.IOException;
import java.util.*;
import java.lang.Character.*;


import org.apache.hadoop.io.WritableComparator;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {
        public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {  // êëàññ Map - íàñëåäíèê êëàññà Mapper + ïàðàìåòðèçîâàííûå àðãóìåíòû
            private final static IntWritable one = new IntWritable(1);    //êîíñòàíòà ñ èìåíåì one è íà÷àëüíûì çíà÷åíèåì 1
            private Text word = new Text();                               // íîâàÿ ïåðåìåííàÿ word òèïà text

            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {  //ô-èÿ ìàï
                // context - îáúåêò, îïèñûâàþùèé ïðàâèëà îáðàáîòêè âõîäíûõ è âûõîäíûõ äàííûõ (èíòåðôåéñ äîñòóïà ê âõîäíûì è âûõîäíûì äàííûì)
                String line = value.toString();              // ïåðåâîä òèïà ïåðåìåííîé value â ñòðîêîâûé òèï è çàïèñü â ïåðåìåííóþ line (òèïà string )
                StringTokenizer tokenizer = new StringTokenizer(line);

                while (tokenizer.hasMoreTokens()) {     //åñëè åñòü åùå ÷àñòü ñòðîêè (ò.å. ñëîâî), òî çàõîäèì â öèêë è áåðåì åùå îäíî ñëîâî
                    String str = tokenizer.nextToken().replaceAll("[^a-zA-Z\'\\-]","");
                    if (str.length() != 0) {
                      if (str.charAt(0) == '-') {
                          str = str.substring(1, str.length());
                      }
                    }
                    if (str.length() != 0) {
                      if (str.charAt(str.length() - 1) == '-') {
                          str = str.substring(0, str.length() - 1);
                      }
                    }
                    if (str.length() != 0) {
                      if (str.charAt(0) == '\'') {
                          str = str.substring(1, str.length());
                      }
                    }
                    if (str.length() != 0) {
                      if (str.charAt(str.length() - 1) == '\'') {
                          str = str.substring(0, str.length() - 1);
                      }
                    }
                    if (str.length() != 0) {
                      if (str.charAt(0) == 'A' || str.charAt(0) == 'a') {
                          word.set(str);   // ïåðåâîäèì ñëåäóþùåå ñëîâî â òèï text, êîòîðîå "ïîíÿòíî" äëÿ hadoop
                          context.write(word, one);          // âûâîä ïàðû (êëþ÷, çíà÷åíèå) íà âõîä ðåäüþñåðà
                      }
                    }
                }

          public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {  //êëàññ Reduce - íàñëåäíèê êëàññà Reducer

            public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {   //ô-èÿ ðåäüþñåðà ñî ñïèñêîì èñêëþ÷åíèé
                int sum = 0;
                        for (IntWritable val : values) {   // â ïåðåìåííóþ val çàïèñûâàþòñÿ âñå òå "êîëè÷åñòâà èç ìàïåðîâ", êîòîðûå ïðèøëè íà âõîä ðåäüþñåðà èç ìàïåðîâ
                        sum += val.get();             // ñóììèðîâàíèå âåëè÷èí êîë-âà äëÿ îäíîãî ñëîâà, val.get - ïîëó÷èòü çíà÷åíèå òèïà Int èç IntWritable
                        }
                        context.write(key, new IntWritable(sum));    // âûâîä îòâåòà â çàäàííóþ äèðåêòîðèþ
                //}
            }
         }



public class SecondarySortBasicCompKeySortComparator extends WritableComparator {

  protected SecondarySortBasicCompKeySortComparator() {
                super(LongWritable.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
                LongWritable key1 = (LongWritable) w1;
                LongWritable key2 = (LongWritable) w2;

                int cmpResult = key1.compareTo(key2);
                        //If the minus is taken out, the values will be in
                        //ascending order
                return cmpResult;
        }
}



         public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();       // êîíôèãóðàöèÿ hadoop 

            Job job = new Job(conf, "wordcount");           // ýòî óñòàðåâøèé èíòåðôåéñ, â êàâû÷êàõ - èìÿ çàäà÷è â Job Tracker'å
            job.setJarByClass(WordCount.class);             // äîáàâëåíèå äëÿ òîãî, ÷òîáû ïðîèñõîäèëà êîìïèëÿöèÿ (ñïîñîá "çàäàíèÿ" çàïóñêà áàéò-êîäà )


            job.setOutputKeyClass(Text.class);              // óñòàíîâêà ñîîòâåòñòâóþùèõ òèïîâ íà âûõîäå ðåäüþñåðà
            job.setOutputValueClass(IntWritable.class);

            job.setMapperClass(Map.class);
            job.setReducerClass(Reduce.class);

            job.setSortComparatorClass(SecondarySortBasicCompKeySortComparator.class);

            job.setInputFormatClass(TextInputFormat.class);   // íà âõîä ìàïåðà
            job.setOutputFormatClass(TextOutputFormat.class); // íà âûõîäå âñåé çàäà÷è

            job.setNumReduceTasks(1);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.waitForCompletion(true);    //çàïóñê çàäà÷è íà èñïîëíåíèå
         }

}
