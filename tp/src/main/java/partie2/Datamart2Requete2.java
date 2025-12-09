package partie2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import java.time.Instant;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/*
    -- Le mois de l'année qui coûte le plus cher

    SELECT d.mois, SUM(a.cout_total) as cout_total_mensuel
    FROM actifs a
    JOIN date_aws d ON a.id_date = d.id_date
    GROUP BY d.mois
    ORDER BY cout_total_mensuel DESC
*/

public class Datamart2Requete2 {
    private static final String INPUT_PATH_CUSTOMERS = "input-aws/date_aws.csv";
    private static final String INPUT_PATH_ORDERS = "input-aws/actifs.csv";
    private static final String OUTPUT_PATH = "output/partie2/Datamart2/Requete2/";
    private static final Logger LOG = Logger.getLogger(Datamart2Requete2.class.getName());

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n%6$s");

        try {
            FileHandler fh = new FileHandler("out.log");
            fh.setFormatter(new SimpleFormatter());
            LOG.addHandler(fh);
        } catch (SecurityException | IOException e) {
            System.exit(1);
        }
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.toString().equals("0")) {
                return;
            }

            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();

            String line = value.toString();
            String[] words = line.split(",");

            if (filename.contains("actifs")) {
                Text idDate = new Text(words[3]);
                Text coutTotal = new Text("ACT:" + words[4]);

                context.write(idDate, coutTotal);
            } else if (filename.contains("date_aws")) {
                Text idDate = new Text(words[0]);
                Text mois = new Text("DATE:" + words[5]);

                context.write(idDate, mois);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String mois = "";
            double sum = 0.0;

            for (Text val : values) {
                String strVal = val.toString();

                if (strVal.startsWith("DATE:")) {
                    mois = strVal.substring(5);
                } else if (strVal.startsWith("ACT:")) {
                    try {
                        String montantStr = strVal.substring(4);
                        sum += Double.parseDouble(montantStr);
                    } catch (NumberFormatException e) {
                        LOG.warning(e.getMessage());
                    }
                }
            }

            if (!mois.isEmpty() && sum != 0) {
                context.write(new Text(mois), new Text(String.valueOf(sum)));
            }
        }
    }

    public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split("\t");

            if (words.length == 2) {
                Text date = new Text(words[0]);
                Text coutTotal = new Text(words[1]);

                context.write(date, coutTotal);
            }
        }
    }

    public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;

            for (Text val : values) {
                try {
                    sum += Double.parseDouble(String.valueOf(val));
                } catch (NumberFormatException e) {
                    LOG.warning(e.getMessage());
                }
            }

            if ( sum != 0) {
                context.write(new Text(key), new Text(String.valueOf(sum)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path tempPath = new Path(OUTPUT_PATH + "/temp");
        Path finalPath = new Path(OUTPUT_PATH + Instant.now().getEpochSecond());

        Job job1 = new Job(conf, "Datamart1Requete2_1");
        job1.setJarByClass(Datamart2Requete2.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setMapperClass(Datamart2Requete2.Map.class);
        job1.setReducerClass(Datamart2Requete2.Reduce.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        Path input = new Path(INPUT_PATH_CUSTOMERS);
        Path input2 = new Path(INPUT_PATH_ORDERS);
        FileInputFormat.setInputPaths(job1, input, input2);

        FileOutputFormat.setOutputPath(job1, tempPath);

        if (job1.waitForCompletion(true)) {
            Job job2 = new Job(conf, "Datamart1Requete2_2");
            job2.setJarByClass(Datamart2Requete2.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            job2.setMapperClass(Datamart2Requete2.Map2.class);
            job2.setReducerClass(Datamart2Requete2.Reduce2.class);

            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.setInputPaths(job2, tempPath);
            FileOutputFormat.setOutputPath(job2, finalPath);

            job2.waitForCompletion(true);

            if (fs.exists(tempPath)) {
                fs.delete(tempPath, true);
            }
        }
    }
}