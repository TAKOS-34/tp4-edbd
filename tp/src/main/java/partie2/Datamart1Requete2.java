package partie2;
import java.io.IOException;
import java.time.Instant;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
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

/*
    -- La localisation qui coûte le plus cher

    SELECT r.nom_az, SUM(a.cout_total) AS cout_operationnel_total
    FROM actifs a
    JOIN localisation r ON a.id_localisation = r.id_localisation
    GROUP BY r.nom_az
    ORDER BY cout_operationnel_total DESC
*/

public class Datamart1Requete2 {
    private static final String INPUT_PATH_CUSTOMERS = "input-aws/facturation.csv";
    private static final String INPUT_PATH_ORDERS = "input-aws/utilisateurs_dynamique.csv";
    private static final String OUTPUT_PATH = "output/partie2/Datamart1/Requete2/";
    private static final Logger LOG = Logger.getLogger(Datamart1Requete2.class.getName());

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

            if (filename.contains("facturation")) {
                Text idUtilisateur = new Text(words[0]);
                Text totalBrut = new Text("FACT:" + words[5]);

                context.write(idUtilisateur, totalBrut);
            } else if (filename.contains("utilisateurs_dynamique") && words.length > 7 && "Y".equals(words[7])) {
                Text idUtilisateur = new Text(words[0]);
                Text paysUtilisateurs = new Text("USER:" + words[4]);

                context.write(idUtilisateur, paysUtilisateurs);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String pays = "";
            double sum = 0.0;

            for (Text val : values) {
                String strVal = val.toString();

                if (strVal.startsWith("USER:")) {
                    pays = strVal.substring(5);
                } else if (strVal.startsWith("FACT:")) {
                    try {
                        String montantStr = strVal.substring(5);
                        sum += Double.parseDouble(montantStr);
                    } catch (NumberFormatException e) {
                        LOG.warning(e.getMessage());
                    }
                }
            }

            if (!pays.isEmpty() && sum != 0) {
                context.write(new Text(pays), new Text(String.valueOf(sum)));
            }
        }
    }

    public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split("\t");

            if (words.length == 2) {
                Text pays = new Text(words[0]);
                Text totalBrut = new Text(words[1]);

                context.write(pays, totalBrut);
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
        job1.setJarByClass(Datamart1Requete2.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setMapperClass(Map.class);
        job1.setReducerClass(Reduce.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        Path input = new Path(INPUT_PATH_CUSTOMERS);
        Path input2 = new Path(INPUT_PATH_ORDERS);
        FileInputFormat.setInputPaths(job1, input, input2);

        FileOutputFormat.setOutputPath(job1, tempPath);

        if (job1.waitForCompletion(true)) {
            Job job2 = new Job(conf, "Datamart1Requete2_2");
            job2.setJarByClass(Datamart1Requete2.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            job2.setMapperClass(Map2.class);
            job2.setReducerClass(Reduce2.class);

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