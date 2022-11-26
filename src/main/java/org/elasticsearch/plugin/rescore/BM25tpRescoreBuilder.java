package org.elasticsearch.plugin.rescore;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.*;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Query;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.index.Term;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.util.BytesRef;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.Rescorer;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.index.analysis.IndexAnalyzers;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Objects;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;

import static java.util.Collections.singletonList;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class BM25tpRescoreBuilder extends RescorerBuilder<BM25tpRescoreBuilder> {
    public static final String NAME = "bm25tp";

    private final String factorField;
    private final String query;
    private final String analyzerName;
    private final float decay;
    private final float minimum_should_match;

    public BM25tpRescoreBuilder(String query, String factorField, String analyzerName, float decay, float minimum_should_match) {
        this.query = query;
        this.factorField = factorField;
        this.analyzerName = analyzerName;
        this.decay = decay;
        this.minimum_should_match = minimum_should_match;
    }

    BM25tpRescoreBuilder(StreamInput in) throws IOException {
        super(in);
        query = in.readString();
        factorField = in.readString();
        analyzerName = in.readString();
        decay = in.readFloat();
        minimum_should_match = in.readFloat();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(query);
        out.writeString(factorField);
        out.writeString(analyzerName);
        out.writeFloat(decay);
        out.writeFloat(minimum_should_match);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public RescorerBuilder<BM25tpRescoreBuilder> rewrite(QueryRewriteContext ctx) throws IOException {
        return this;
    }

    // private static final ParseField FACTOR = new ParseField("factor");
    private static final ParseField QUERY = new ParseField("query");
    private static final ParseField FACTOR_FIELD = new ParseField("factor_field");
    private static final ParseField ANALYZER_NAME = new ParseField("analyzer");
    private static final ParseField DECAY = new ParseField("decay");
    private static final ParseField MINIMUM_SHOULD_MATCH = new ParseField("minimum_should_match");

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(QUERY.getPreferredName(), query);
        builder.field(FACTOR_FIELD.getPreferredName(), factorField);
        builder.field(ANALYZER_NAME.getPreferredName(), analyzerName);
        builder.field(DECAY.getPreferredName(), decay);
        builder.field(MINIMUM_SHOULD_MATCH.getPreferredName(), minimum_should_match);
    }

    private static final ConstructingObjectParser<BM25tpRescoreBuilder, Void> PARSER = new ConstructingObjectParser<>(
            NAME,
            args -> new BM25tpRescoreBuilder((String) args[0], (String) args[1], (String) args[2], (Float) args[3], (Float) args[4]));
    static {
        PARSER.declareString(constructorArg(), QUERY);
        PARSER.declareString(constructorArg(), FACTOR_FIELD);
        PARSER.declareString(constructorArg(), ANALYZER_NAME);
        PARSER.declareFloat(constructorArg(), DECAY);
        PARSER.declareFloat(constructorArg(), MINIMUM_SHOULD_MATCH);
    }

    public static BM25tpRescoreBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public RescoreContext innerBuildContext(int windowSize, SearchExecutionContext context) throws IOException {
        // IndexFieldData<?> factorField =
        // this.factorField == null ? null :
        // context.getForField(context.getFieldType(this.factorField));
        IndexAnalyzers indexAnalyzers = context.getIndexAnalyzers();
        Analyzer analyzer = indexAnalyzers.get(this.analyzerName).analyzer();
        return new BM25tpRescoreContext(windowSize, this.factorField, this.query, analyzer, decay, minimum_should_match);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        BM25tpRescoreBuilder other = (BM25tpRescoreBuilder) obj;
        return query == other.query
                && Objects.equals(factorField, other.factorField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), query, factorField);
    }

    String query() {
        return query;
    }

    @Nullable
    String factorField() {
        return factorField;
    }
}
