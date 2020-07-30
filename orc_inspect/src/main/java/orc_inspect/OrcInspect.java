package orc_inspect;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import javax.inject.Inject;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.HelpOption;
import io.airlift.airline.Option;
import io.airlift.airline.SingleCommand;
import io.prestosql.orc.FileOrcDataSource;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcReader;
import io.prestosql.orc.OrcReaderOptions;
import io.prestosql.orc.metadata.Footer;
import io.prestosql.orc.metadata.Metadata;
import io.prestosql.orc.metadata.StripeInformation;
import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.metadata.OrcType;
import io.prestosql.orc.metadata.OrcColumnId;
import io.prestosql.orc.metadata.statistics.ColumnStatistics;
import io.prestosql.orc.metadata.statistics.StripeStatistics;

@Command(name = "orc-inspect", description = "inspect orc file")
public class OrcInspect {
    @Inject
    public HelpOption helpOption;

    @Arguments(description = "source file to inspect")
    public String source;

    @Option(name = { "-v", "--verbose" }, description = "verbose output")
    public boolean verbose;

    public void run() throws IOException {
        if (source == null) {
            System.err.println("source ORC file was not specified");
            System.exit(1);
        }

        OrcReader reader = createOrcReader(new File(source));

        Footer footer = reader.getFooter();
        Metadata metadata = reader.getMetadata();

        System.out.println("numberOfRows\t" + footer.getNumberOfRows());
        System.out.println("rowsInRowGroup\t" + footer.getRowsInRowGroup());

        // schema
        ColumnMetadata<OrcType> types = footer.getTypes();
        Optional<ColumnMetadata<ColumnStatistics>> stats = footer.getFileStats();
        printSchema(types, stats);
        System.out.println();

        // stripes
        List<StripeInformation> stripes = footer.getStripes();
        System.out.println("numberOfStrips\t" + stripes.size());
        for (int i = 0; i < stripes.size(); i++) {
            System.out.println(String.format("stripe[%d]\t%s", i, stripes.get(i)));
        }

        List<Optional<StripeStatistics>> stripeStats = metadata.getStripeStatsList();
        for (int i = 0; i < stripeStats.size(); i++) {
            if (stripeStats.get(i).isPresent()) {
                System.out.println("stripeStats[" + i + "]");
                StripeStatistics stripeStat = stripeStats.get(i).get();
                System.out.println("retainedSize\t" + stripeStat.getRetainedSizeInBytes());
                for (int j = 0; j < stripeStat.getColumnStatistics().size(); j++) {
                    System.out.println("striptStats[" + i + "][" + j + "]\t" +
                                       stripeStat.getColumnStatistics().get(new OrcColumnId(j)));
                }
            }
        }
        System.out.println();

        System.out.println("userMetadata\t" + footer.getUserMetadata().keySet());

        System.out.println("compression_kind\t" + reader.getCompressionKind());
        System.out.println("compression_block_size\t" + reader.getBufferSize());
    }

    private OrcReader createOrcReader(File targetFile) throws IOException {
        OrcReaderOptions options = new OrcReaderOptions();
        final OrcDataSource orcDataSource = new FileOrcDataSource(targetFile, options);
        return new OrcReader(orcDataSource, options);
    }

    private void printSchema(ColumnMetadata<OrcType> types, Optional<ColumnMetadata<ColumnStatistics>> stats) {
        OrcType root = types.get(new OrcColumnId(0));
        System.out.println("numberOfColumns\t" + root.getFieldCount());

        for (int i = 0; i < root.getFieldCount(); i++) {
            String statsStr = "";
            if (stats.isPresent()) {
                statsStr = stats.get().get(root.getFieldTypeIndex(i)).toString();
            }
            System.out.println(String.format(
                "column[%d]\tname = %s\ttype = %s\tstats = %s",
                i,
                root.getFieldName(i),
                types.get(root.getFieldTypeIndex(i)).getOrcTypeKind(),
                statsStr
            ));
        }
    }

    public static void main(String[] args) throws IOException {
        OrcInspect inspecter = SingleCommand.singleCommand(OrcInspect.class).parse(args);

        inspecter.run();
    }
}
