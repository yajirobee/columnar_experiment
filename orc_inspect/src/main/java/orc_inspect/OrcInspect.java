package orc_inspect;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Map.Entry;

import javax.inject.Inject;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.HelpOption;
import io.airlift.airline.Option;
import io.airlift.airline.SingleCommand;
import io.airlift.slice.Slice;
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
        System.out.println();

        // schema
        System.out.println("schema");
        ColumnMetadata<OrcType> types = footer.getTypes();
        Optional<ColumnMetadata<ColumnStatistics>> stats = footer.getFileStats();
        List<Column> columns = new SchemaVisitor(types, stats).scanSchema();

        // stripes
        System.out.println("stripes");
        List<StripeInformation> stripes = footer.getStripes();
        List<Optional<StripeStatistics>> stripeStats = metadata.getStripeStatsList();
        System.out.println("numberOfStrips\t" + stripes.size());
        assert (stripes.size() == stripeStats.size());
        for (int i = 0; i < stripes.size(); i++) {
            System.out.println(String.format("stripe[%d]\t%s", i, stripes.get(i)));

            if (stripeStats.get(i).isPresent()) {
                System.out.println("stripeStats");
                StripeStatistics stripeStat = stripeStats.get(i).get();
                System.out.println("retainedSize\t" + stripeStat.getRetainedSizeInBytes());
                printStripeColumnStats(columns, stripeStat.getColumnStatistics());
            }
        }

        System.out.println("userMetadata");
        for (Entry<String, Slice> entry: footer.getUserMetadata().entrySet()) {
            System.out.println(entry.getKey() + " = " + entry.getValue());
        }
        System.out.println();

        System.out.println("compressionKind\t" + reader.getCompressionKind());
        System.out.println("compressionBlockSize\t" + reader.getBufferSize());
    }

    private OrcReader createOrcReader(final File targetFile) throws IOException {
        final OrcReaderOptions options = new OrcReaderOptions();
        final OrcDataSource orcDataSource = new FileOrcDataSource(targetFile, options);
        return new OrcReader(orcDataSource, options);
    }

    static class Column {
        final public OrcColumnId id;
        final public String name;
        final public int depth;

        public Column(final OrcColumnId id, final String name, final int depth) {
            this.id = id;
            this.name = name;
            this.depth = depth;
        }
    }

    static class SchemaVisitor {
        final private ColumnMetadata<OrcType> types;
        final private Optional<ColumnMetadata<ColumnStatistics>> stats;

        public SchemaVisitor(
            final ColumnMetadata<OrcType> types,
            final Optional<ColumnMetadata<ColumnStatistics>> stats
        ) {
            this.types = types;
            this.stats = stats;
        }

        public List<Column> scanSchema() {
            OrcType root = types.get(new OrcColumnId(0));
            System.out.println("numberOfColumns\t" + root.getFieldCount());
            List<Column> columns = visitType(root, 0);
            printSchema(columns);
            return columns;
        }

        private List<Column> visitType(final OrcType type, final int depth) {
            List<Column> columns = new ArrayList<>();
            for (int i = 0; i < type.getFieldCount(); i++) {
                OrcColumnId columnId = type.getFieldTypeIndex(i);
                OrcType childType = types.get(columnId);

                columns.add(new Column(columnId, type.getFieldName(i), depth));
                if (childType.getFieldCount() > 0) {
                    columns.addAll(visitType(childType, depth + 1));
                }
            }
            return columns;
        }

        private void printSchema(final List<Column> columns) {
            StringBuilder builder = new StringBuilder();
            int nColumns = 0;
            for (Column column: columns) {
                String statsStr = "";
                if (stats.isPresent()) {
                    statsStr = stats.get().get(column.id).toString();
                }
                if (column.depth == 0) {
                    builder.append(String.format("column[%d]\t", nColumns++));
                }
                OrcType childType = types.get(column.id);
                builder.append(String.format("%sname = %s\ttype = %s\tstats = %s\n",
                                             " ".repeat(column.depth * 2),
                                             column.name,
                                             childType.getOrcTypeKind(),
                                             statsStr));

            }
            System.out.println(builder.toString());
        }
    }

    private void printStripeColumnStats(
        final List<Column> columns,
        final ColumnMetadata<ColumnStatistics> stats
    ) {
        StringBuilder builder = new StringBuilder();
        for (Column column: columns) {
            builder.append(String.format("%sname = %s\tstats = %s\n", " ".repeat(column.depth * 2),
                    column.name, stats.get(column.id).toString()));
        }
        System.out.println(builder.toString());
    }

    public static void main(String[] args) throws IOException {
        OrcInspect inspecter = SingleCommand.singleCommand(OrcInspect.class).parse(args);

        inspecter.run();
    }
}
