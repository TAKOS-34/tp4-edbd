package partie2;
import java.io.IOException;
import java.time.Instant;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import org.apache.hadoop.conf.Configuration;
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
    -- Le service le plus populaire (même s'il est pas le plus rentable)

    SELECT s.nom_service, COUNT(DISTINCT f.id_utilisateur) AS nombre_utilisateurs
    FROM facturation f
    JOIN service s ON f.id_service = s.id_service
    GROUP BY s.nom_service
    ORDER BY nombre_utilisateurs DESC
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

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "Datamart1Requete3");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path input = new Path(INPUT_PATH_CUSTOMERS);
        Path input2 = new Path(INPUT_PATH_ORDERS);
        FileInputFormat.setInputPaths(job, input, input2);

        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        job.waitForCompletion(true);
    }
}