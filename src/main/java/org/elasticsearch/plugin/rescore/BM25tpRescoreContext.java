package org.elasticsearch.plugin.rescore;

import org.elasticsearch.search.rescore.RescoreContext;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.index.Term;
import org.apache.lucene.analysis.Analyzer;

class BM25tpRescoreContext extends RescoreContext {
    final String factorField;
    final String query;
    final Analyzer analyzer;
    final float decay;
    final float minimum_should_match;
    // @Nullable
    // private final IndexFieldData<?> factorField;

    BM25tpRescoreContext(int windowSize, String factorField, String query, Analyzer analyzer, float decay, float minimum_should_match) {
        super(windowSize, BM25tpRescorer.INSTANCE);
        this.factorField = factorField;
        this.query = query;
        this.analyzer = analyzer;
        this.decay = decay;
        this.minimum_should_match = minimum_should_match;
    }
}
