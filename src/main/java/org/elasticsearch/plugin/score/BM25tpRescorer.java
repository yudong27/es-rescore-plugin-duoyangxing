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
import java.util.Set;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

class BM25tpRescorer implements Rescorer {
    public static final BM25tpRescorer INSTANCE = new BM25tpRescorer();
    // private final Logger logger = LogManager.getLogger(BM25tpRescorer.class);

    public Map<String, Integer> getTokens(String text, String factorField, Analyzer analyzer) {
        try {
            // Analyzer analyzer = new StandardAnalyzer();
            TokenStream tokenStream = analyzer.tokenStream(factorField, text);
            CharTermAttribute cta = tokenStream.addAttribute(CharTermAttribute.class);
            tokenStream.reset();
            Map<String, Integer> words = new HashMap<>();
            while (tokenStream.incrementToken()) {
                // System.out.print("[" + cta + "]");
                // words.add(cta.toString());
                String k = cta.toString();
                words.put(k, words.getOrDefault(k, 0)+1);
            }
            tokenStream.close();
            return words;
        } catch (IOException e) {
            throw new IllegalArgumentException("query [" + text + "] can not be analyzed");
        }
    }

    private Map<String, Double> getTokenIDF(long N, Map<String, Integer> queryTokens, String fieldName, IndexReader reader)  throws IOException {
        // idf 和doc无关，应该在循环外拿到
        HashMap<String, Integer> termDocFreq = new HashMap<String, Integer>();
        HashMap<String, Double> termIdf = new HashMap<String, Double>();
        for (String termstr : queryTokens.keySet()) {
            int docfreq = reader.docFreq(new Term(fieldName, termstr));
            termDocFreq.put(termstr, docfreq);
            double idf = Math.log(1.0 + (N - docfreq + 0.5) / (docfreq + 0.5));
            termIdf.put(termstr, idf);
        }
        return termIdf;
    }

    private static class TermPos {
        public String term;
        public int pos;

        public TermPos(String t, int p) {
            this.term = t;
            this.pos = p;
        }
    }

    private double calcBM25(Map<String, Integer> tokens, Map<String, Double> idf, Map<String, Double> tf, double avgdl,
            double dl, double boost, Map<String, Integer> tokenCount, float decay) {
        double k1 = 1.2;
        double b = 0.75;
        double K = k1 * (1.0 - b + b * dl / avgdl);
        double score = 0.0;
        //StringBuilder sb = new StringBuilder();
        //sb.append("TermScore -->  ");
        for (Map.Entry<String, Double> pair : idf.entrySet()) {
            String k = pair.getKey();
            if(tf.containsKey(k)) {
                double idf_score = pair.getValue();
                double freq = Math.min((double)tokens.get(k), tf.get(k));
                double tf_score = freq/(freq+K);
                double decay_factor = Math.exp(-tokenCount.getOrDefault(k, 0)*decay);
                double term_score = decay_factor * boost * idf_score * tf_score * tf.get(k);
                tokenCount.put(k, tokenCount.getOrDefault(k, 0)+1);
                score += term_score;
                //sb.append(k).append(": ").append(term_score)
                //  .append(" tf:").append(tf_score)
                //  .append(" idf:").append(idf_score)
                //  .append(" boost:").append(boost*tf.get(k))
                //  .append(" tokenCount:").append(tokenCount.get(k))
                //  .append(" decay_factor:").append(decay_factor)
                //  .append(", ");
            }
        }

        //logger.info("  score:"+score);
        //logger.info(sb);
        return score;
    }

    private double calcBM25TP(TermPos[] termPosArr, Map<String, Integer> tokens, Map<String, Double> idf, double avgdl,
            double dl, double boost, Map<String, Integer> tokenCount, float decay) {
        double k1 = 1.2;
        double b = 0.75;
        double K = k1 * (1.0 - b + b * dl / avgdl);
        int length = termPosArr.length;
        if (length <= 1 || tokens.size() <= 1)
            return 0.0;
        HashMap<String, Double> acc = new HashMap<>();
        for (String s : tokens.keySet()) {
            acc.put(s, 0.0);
        }
        for (int i = 0; i < length - 1; i++) {
            String t1 = termPosArr[i].term;
            String t2 = termPosArr[i + 1].term;
            if (!tokens.containsKey(t1) || !tokens.containsKey(t2))
                continue;
            if (termPosArr[i].term == termPosArr[i + 1].term)
                continue;
            double dis = (double) (termPosArr[i + 1].pos - termPosArr[i].pos);
            dis = dis * dis;
            double v1 = acc.get(t1);
            v1 += idf.get(t2) / dis;
            acc.put(t1, v1);
            double v2 = acc.get(t2);
            v2 += idf.get(t1) / dis;
            acc.put(t2, v2);
        }
        double score_acc = 0.0;
        for (Map.Entry<String, Double> pair : acc.entrySet()) {
            String tok = pair.getKey();
            double scoreTP = pair.getValue();
            double decayFactor = Math.exp(-tokenCount.getOrDefault(tok, 0)*decay);
            score_acc += decayFactor*Math.min(1.0, idf.getOrDefault(tok, 0.0)) * scoreTP * (k1 + 1.0) / (scoreTP + K);
        }
        return score_acc * boost;
    }

    private Set<Integer> getTopDocIds(TopDocs topDocs) {
        Set<Integer> topDocIds = new HashSet<>();
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
            int docid = topDocs.scoreDocs[i].doc;
            topDocIds.add(docid);
        }
        return topDocIds;
    }

    @Override
    public TopDocs rescore(TopDocs topDocs, IndexSearcher searcher, RescoreContext rescoreContext) throws IOException {
        double boost = 2.2;
        BM25tpRescoreContext context = (BM25tpRescoreContext) rescoreContext;

        IndexReader reader = searcher.getIndexReader();
        long sumDocFrq = reader.getSumDocFreq(context.factorField); // sum of doc count of terms
        long totalTermFrq = reader.getSumTotalTermFreq(context.factorField); // Returns the sum of
                                                                             // TermsEnum#totalTermFreq for all terms in
                                                                             // this field.
        long N = reader.numDocs();
        double avgdl = (double) totalTermFrq / N;
        Set<Integer> topDocIds = getTopDocIds(topDocs);
        //logger.info("doc_num:" + N + " SumDocFreq:" + sumDocFrq + " TotalTermFrq:" + totalTermFrq + " avgdl:" + avgdl
        //        + " topdocs.size:" + topDocIds.size());
        int end = Math.min(topDocs.scoreDocs.length, rescoreContext.getWindowSize());
        Map<String, Integer> termwords = getTokens(context.query, context.factorField, context.analyzer);
        Map<String, Double> termIdf = getTokenIDF(N, termwords, context.factorField, reader);
        Map<Integer, Float> docIdToScore = new HashMap<>();
        Map<String, Integer> tokenCount = new HashMap<>();
        int termWordCount = 0;
        for(Integer tc: termwords.values()) {
            termWordCount += tc;
        }
        //StringBuilder sb1 = new StringBuilder();
        //sb1.append("termwords:").append(termwords.size()).append("-->");
        //for (String t : termwords) {
        //    sb1.append(t).append(", ");
        //}
        //for (Map.Entry<String, Double> pair : termIdf.entrySet()) {
        //    sb1.append(pair.getKey()).append(" idf:").append(pair.getValue()).append(", ");
        //}
        //logger.info(sb1);

        ScoreDoc[] sortedByDocId = new ScoreDoc[end];
        System.arraycopy(topDocs.scoreDocs, 0, sortedByDocId, 0, end);
        // Arrays.sort(sortedByDocId, (a, b) -> a.doc - b.doc); // Safe because doc ids >= 0

        Iterator<LeafReaderContext> leaves = searcher.getIndexReader().leaves().iterator();
        LeafReaderContext leaf = null;
        SortedNumericDoubleValues data = null;
        int endDoc = 0;
        String doc_text = null;
        for (int i = 0; i < end; i++) {
            // logger.info("===============" + i + "===========================");
            if (sortedByDocId[i].doc >= endDoc) {
                do {
                    leaf = leaves.next();
                    endDoc = leaf.docBase + leaf.reader().maxDoc();
                } while (sortedByDocId[i].doc >= endDoc);
            }
            // 需要tf， idf， pos信息
            Map<String, Double> termFreqInDoc = new HashMap<>();
            ArrayList<TermPos> termPoss = new ArrayList<>();
            //StringBuilder sb = new StringBuilder();
            //sb.append("i=").append(i).append("; ");
            //sb.append("docbase=").append(leaf.docBase).append("; ");
            //sb.append("docid=").append(sortedByDocId[i].doc).append("; ");
            Terms vector = reader.getTermVector(sortedByDocId[i].doc, context.factorField); // Read the document's
                                                                                            // document vector.
            //sb.append(" termsize=").append(vector.size()).append("; ");
            //sb.append(" sumdocfreq=").append(vector.getSumDocFreq()).append("; ");
            //sb.append(" getSumTotalTermFreq=").append(vector.getSumTotalTermFreq()).append("; ");
            //sb.append(" termWordCount=").append(termWordCount).append("; ");
            //sb.append(" decay=").append(context.decay);
            Document doc = reader.document(sortedByDocId[i].doc);
            // doc_text = doc.get(context.factorField);
            TermsEnum terms = vector.iterator(); // 本doc里term迭代

            PostingsEnum positions = null;
            BytesRef term;
            long termCount = 0;

            while ((term = terms.next()) != null) {
                String termstr = term.utf8ToString(); // Get the text string of the term.
                long freq = terms.totalTermFreq(); // 确实是本doc里term的出现次数
                //float termboost = terms.getBoost();
                termCount += freq;
                if (termwords.containsKey(termstr)) {
                    termFreqInDoc.put(termstr, (double)freq);
                    positions = terms.postings(positions, PostingsEnum.POSITIONS);
                    positions.nextDoc(); // you still need to move the cursor
                    // now accessing the occurrence position of the terms by iteratively calling
                    // nextPosition()
                    // Each time you call posting.nextDoc(), it moves the cursor of the posting list
                    // to the next position
                    // and returns the docid of the current entry (document). Note that this is an
                    // internal Lucene docid.
                    // It returns PostingsEnum.NO_MORE_DOCS if you have reached the end of the
                    // posting list.

                    //sb.append("term=").append(termstr).append(",");
                    //sb.append("freq=").append(freq).append(",");
                    //sb.append("position:[");
                    for (int ii = 0; ii < freq; ii++) {
                        int p = positions.nextPosition();
                        //sb.append(p).append(",");
                        termPoss.add(new TermPos(termstr, p));
                    }
                    //sb.append("]");
                    //sb.append(" || ");
                } else {
                    //sb.append(termstr).append(":").append(freq).append(" || ");
                    ;
                }

            }

            // LeafFieldData fd = context.factorField.load(leaf);
            // if (false == (fd instanceof LeafNumericFieldData)) {
            // throw new IllegalArgumentException("[" + context.factorField.getFieldName() +
            // "] is not a number");
            // }
            // data = ((LeafNumericFieldData) fd).getDoubleValues();
            // if (data.docValueCount() > 1) {
            // throw new IllegalArgumentException("document [" + sortedByDocId[i].doc
            // + "] has more than one value for [" + context.factorField.getFieldName() +
            // "]");
            // }
            double termfreqs = 0.0;
            for(double t: termFreqInDoc.values()) {
                termfreqs += t;
            }
            if(termCount * context.minimum_should_match > termfreqs) {
                sortedByDocId[i].score = (float)0.0;
                continue;
            }
            TermPos[] termPosArr = new TermPos[termPoss.size()];
            termPoss.toArray(termPosArr);
            Arrays.sort(termPosArr, (a, b) -> a.pos - b.pos); // Safe because doc ids >= 0
            double bm25tp_score = calcBM25TP(termPosArr, termwords, termIdf, avgdl, (double)termWordCount, boost, tokenCount, context.decay);
            double bm25_score = calcBM25(termwords, termIdf, termFreqInDoc, avgdl, (double)termWordCount, boost, tokenCount, context.decay);
            //StringBuilder sb2 = new StringBuilder();
            //for (int kk = 0; kk < termPosArr.length; kk++) {
            //    sb2.append(termPosArr[kk].term + ":" + termPosArr[kk].pos + ", ");
            //}
            //sb2.append(doc_text);
            //logger.info(sb2);
            //sb.append(" termCount=").append(termCount).append(";");

            //sb.append(" bm25=").append(sortedByDocId[i].score).append(" bm25tp_score=").append(bm25tp_score)
            //        .append(" bm25_score=").append(bm25_score);
            //logger.info(sb);
            sortedByDocId[i].score = (float)(bm25tp_score + bm25_score); // 取代原有得分
            docIdToScore.put(sortedByDocId[i].doc, sortedByDocId[i].score);
        }
        for (int i = 0; i < end; i++) {
            if (docIdToScore.containsKey(topDocs.scoreDocs[i].doc)) {
                topDocs.scoreDocs[i].score = docIdToScore.get(topDocs.scoreDocs[i].doc);
            }
        }
        // Sort by score descending, then docID ascending, just like lucene's
        // QueryRescorer
        Arrays.sort(topDocs.scoreDocs, (a, b) -> {
            if (a.score > b.score) {
                return -1;
            }
            if (a.score < b.score) {
                return 1;
            }
            // Safe because doc ids >= 0
            return a.doc - b.doc;
        });
        return topDocs;
    }

    @Override
    public Explanation explain(int topLevelDocId, IndexSearcher searcher, RescoreContext rescoreContext,
            Explanation sourceExplanation) throws IOException {
        BM25tpRescoreContext context = (BM25tpRescoreContext) rescoreContext;
        // Note that this is inaccurate because it ignores factor field
        IndexReader reader = searcher.getIndexReader();
        Document doc = reader.document(topLevelDocId);
        Explanation tmp2 = Explanation.match(1, doc.get("sentence") + ", " + doc.get("title"));
        Explanation tmp = Explanation.match(123, "test");
        Explanation tmp1 = Explanation.match(topLevelDocId, "docid");
        Explanation bm25factor = Explanation.match(0.0, "BM25TP", tmp2, tmp, tmp1, sourceExplanation);
        return bm25factor;
    }

}
