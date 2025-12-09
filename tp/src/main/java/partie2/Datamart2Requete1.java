package partie2;
import java.io.IOException;
import java.time.Instant;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

/*
    -- La localisation qui coûte le plus cher

    SELECT r.nom_az, SUM(a.cout_total) AS cout_operationnel_total
    FROM actifs a
    JOIN vue_localisation r ON a.id_localisation = r.id_localisation
    GROUP BY r.nom_az
    ORDER BY cout_operationnel_total DESC
*/

public class Datamart2Requete1 {
    private static final String INPUT_PATH_CUSTOMERS = "input-aws/actifs.csv";
    private static final String INPUT_PATH_ORDERS = "input-aws/localisation.csv";
    private static final String OUTPUT_PATH = "output/partie2/Datamart2/Requete1/";
    private static final Logger LOG = Logger.getLogger(Datamart2Requete1.class.getName());

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
                Text idLocalisation = new Text(words[1]);
                Text coutTotal = new Text("ACT:" + words[4]);

                context.write(idLocalisation, coutTotal);
            } else if (filename.contains("localisation")) {
                Text idLocalisation = new Text(words[0]);
                Text nomAz = new Text("LOC:" + words[8]);

                context.write(idLocalisation, nomAz);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String nomAz = "";
            double sum = 0.0;

            for (Text val : values) {
                String strVal = val.toString();

                if (strVal.startsWith("LOC:")) {
                    nomAz = strVal.substring(4);
                } else if (strVal.startsWith("ACT:")) {
                    try {
                        String montantStr = strVal.substring(4);
                        sum += Double.parseDouble(montantStr);
                    } catch (NumberFormatException e) {
                        LOG.warning(e.getMessage());
                    }
                }
            }

            if (!nomAz.isEmpty() && sum != 0) {
                context.write(new Text(nomAz), new Text(String.valueOf(sum)));
            }
        }
    }

    public static class MapSort extends Mapper<LongWritable, Text, DoubleWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");

            if (parts.length >= 2) {
                String nomService = parts[0];
                try {
                    double number = Double.parseDouble(parts[1]);
                    context.write(new DoubleWritable(-number), new Text(nomService));
                } catch (NumberFormatException ignored) {}
            }
        }
    }

    public static class ReduceSort extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double number = -key.get();
            for (Text service : values) {
                context.write(service, new DoubleWritable(number));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        String timestamp = String.valueOf(Instant.now().getEpochSecond());
        Path tempPath = new Path(OUTPUT_PATH + timestamp + "_temp");
        Path finalPath = new Path(OUTPUT_PATH + timestamp);

        Job job1 = new Job(conf, "Datamart2Requete1_1");
        job1.setJarByClass(Datamart2Requete1.class);

        job1.setMapperClass(Map.class);
        job1.setReducerClass(Reduce.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        Path input = new Path(INPUT_PATH_CUSTOMERS);
        Path input2 = new Path(INPUT_PATH_ORDERS);
        FileInputFormat.setInputPaths(job1, input, input2);
        FileOutputFormat.setOutputPath(job1, tempPath);

        if (job1.waitForCompletion(true)) {
            Job job2 = new Job(conf, "Datamart2Requete1_2");
            job2.setJarByClass(Datamart2Requete1.class);

            job2.setMapperClass(MapSort.class);
            job2.setReducerClass(ReduceSort.class);

            job2.setMapOutputKeyClass(DoubleWritable.class);
            job2.setMapOutputValueClass(Text.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(DoubleWritable.class);

            job2.setNumReduceTasks(1);

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