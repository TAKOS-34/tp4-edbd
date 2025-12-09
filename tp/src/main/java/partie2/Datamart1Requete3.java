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

public class Datamart1Requete3 {
    private static final String INPUT_PATH_CUSTOMERS = "input-aws/facturation.csv";
    private static final String INPUT_PATH_ORDERS = "input-aws/service.csv";
    private static final String OUTPUT_PATH = "output/partie2/Datamart1/Requete3/";
    private static final Logger LOG = Logger.getLogger(Datamart1Requete3.class.getName());

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
            int sum = 0;

            for (Text val : values) {
                String strVal = val.toString();

                if (strVal.startsWith("SERV:")) {
                    nomService = strVal.substring(5);
                } else if (strVal.startsWith("FACT:")) {
                    sum++;
                }
            }

            if (!nomService.isEmpty() && sum != 0) {
                context.write(new Text(nomService), new Text(String.valueOf(sum)));
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