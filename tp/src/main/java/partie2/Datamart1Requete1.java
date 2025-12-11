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
    -- Le service génère le plus de chiffre d'affaire

    SELECT s.nom_service, SUM(f.total_brut) AS chiffre_affaire_total
    FROM facturation f
    JOIN service s ON f.id_service = s.id_service
    GROUP BY s.nom_service
    ORDER BY chiffre_affaire_total DESC;
*/

public class Datamart1Requete1 {
    private static final String INPUT_PATH_CUSTOMERS = "input-aws/facturation.csv";
    private static final String INPUT_PATH_ORDERS = "input-aws/service.csv";
    private static final String OUTPUT_PATH = "output/partie2/Datamart1/Requete1/";
    private static final Logger LOG = Logger.getLogger(Datamart1Requete1.class.getName());

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
            if (key.toString().equals("0")) return;

            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();

            String line = value.toString();
            String[] words = line.split(",");

            if (filename.contains("facturation")) {
                Text idService = new Text(words[1]);
                Text totalBrut = new Text("FACT:" + words[5]);

                context.write(idService, totalBrut);
            } else if (filename.contains("service")) {
                Text idService = new Text(words[0]);
                Text nomService = new Text("SERV:" + words[2]);

                context.write(idService, nomService);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String nomService = "";
            double sum = 0.0;

            for (Text val : values) {
                String strVal = val.toString();

                if (strVal.startsWith("SERV:")) {
                    nomService = strVal.substring(5);
                } else if (strVal.startsWith("FACT:")) {
                    try {
                        String montantStr = strVal.substring(5);
                        sum += Double.parseDouble(montantStr);
                    } catch (NumberFormatException e) {
                        LOG.warning(e.getMessage());
                    }
                }
            }

            if (!nomService.isEmpty() && sum != 0) {
                context.write(new Text(nomService), new Text(String.valueOf(sum)));
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

        Job job1 = new Job(conf, "Datamart1Requete1_1");
        job1.setJarByClass(Datamart1Requete1.class);

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
            Job job2 = new Job(conf, "Datamart1Requete1_2");
            job2.setJarByClass(Datamart1Requete1.class);

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