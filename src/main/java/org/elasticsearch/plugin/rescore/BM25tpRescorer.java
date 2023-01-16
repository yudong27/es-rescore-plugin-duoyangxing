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
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
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

    public BM25tpTermInfo getTokens(String text, String factorField, Analyzer analyzer) {
        try {
            // Analyzer analyzer = new StandardAnalyzer();
            TokenStream tokenStream = analyzer.tokenStream(factorField, text);
            OffsetAttribute offsetAttribute = tokenStream.addAttribute(OffsetAttribute.class);
            CharTermAttribute cta = tokenStream.addAttribute(CharTermAttribute.class);
            tokenStream.reset();
            // Map<String, Integer> words = new HashMap<>();
            BM25tpTermInfo words = new BM25tpTermInfo();
            // Map<Integer, String> wordsMap = new HashMap<>();
            while (tokenStream.incrementToken()) {
                // System.out.print("[" + cta + "]");
                // words.add(cta.toString());
                int startOffset = offsetAttribute.startOffset();
                int endOffset = offsetAttribute.endOffset();
                String k = cta.toString();
                words.addTermPos(k, startOffset);
            }
            tokenStream.close();
            words.sortAndRawText("q");
            return words;
        } catch (IOException e) {
            throw new IllegalArgumentException("query [" + text + "] can not be analyzed");
        }
    }

    private Map<String, Double> getQueryTokenIDF(long N, Map<String, Integer> queryTokens, String fieldName,
            IndexReader reader) throws IOException {
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

    private double calcBM25(BM25tpDocInfo docInfo, double boost, Map<String, Integer> tokenCount, float decay) {
        double k1 = 1.2;
        double b = 0.75;
        double K = k1 * (1.0 - b + b * docInfo.docTermsInfo.size() / docInfo.avgdl);
        Map<String, Integer> tokens = docInfo.queryTermsInfo.queryTokenCountMap;
        Map<String, Double> idf = docInfo.termIdf;
        Map<String, Long> tf = docInfo.docTermFreq;
        double score = 0.0;
        // StringBuilder sb = new StringBuilder();
        // sb.append("TermScore --> ");
        for (Map.Entry<String, Double> pair : idf.entrySet()) {
            String k = pair.getKey();
            if (tf.containsKey(k)) {
                double idf_score = pair.getValue();
                double freq = (double) Math.min(tokens.get(k), tf.get(k));
                double tf_score = freq / (freq + K);
                double decay_factor = 1.0;
                if (tokenCount != null) {
                    decay_factor = Math.exp(-tokenCount.getOrDefault(k, 0) * decay);
                    tokenCount.put(k, tokenCount.getOrDefault(k, 0) + 1);
                }
                double term_score = decay_factor * boost * idf_score * tf_score * tf.get(k);
                score += term_score;
                // sb.append(k).append(": ").append(term_score)
                // .append(" tf:").append(tf_score)
                // .append(" idf:").append(idf_score)
                // .append(" boost:").append(boost*tf.get(k))
                // .append(" tokenCount:").append(tokenCount.get(k))
                // .append(" decay_factor:").append(decay_factor)
                // .append(", ");
            }
        }

        // logger.info(" score:"+score);
        // logger.info(sb);
        return score;
    }

    private int calcTermAdjacentCharMatch(BM25tpDocInfo docInfo, int char_window_size) {
        // 计算term邻近单字命中度，即如果词语没有命中，单字怎么命中
        // 这在错个别字的情况下，缓解没有命中的问题
        // 此时，termHit应该已经填好了
        //int charHitCount = 0;
        double charHitScore = 0.0;
        for (int i = 0; i < docInfo.docTermsInfo.size(); i++) {
            String dt = docInfo.docTermsInfo.getTerm(i);
            for(int j = 0; j < docInfo.queryTermsInfo.size(); j++) {
                String qt = docInfo.queryTermsInfo.getTerm(j);
                if(dt.equals(qt)) {
                    // 单词匹配，看左右是否有term未匹配，但char匹配的
                    charHitScore += docInfo.docTermRightHit(i, j, char_window_size);
                    charHitScore += docInfo.docTermLeftHit(i, j, char_window_size);
                    //logger.info("========== Adjacent: docIndex="+i+"/"+dt+" queryIndex="+j+" charHitScore="+charHitScore);
                }
            }
        }
        //docInfo.charHitCount = charHitCount;
        //docInfo.char_boost = 1.0 + (double)charHitCount/(double)docInfo.;
        docInfo.charHitScore = charHitScore * 2.2;
        return 0;
    }

    private double calcTermProximity(BM25tpDocInfo docInfo, double boost, 
                                    Map<String, Integer> tokenUsedCount, float decay) {
        //StringBuilder sb = new StringBuilder();
        //sb.append("TermProximity docId:").append(docInfo.docid).append(", ");
        double k1 = 1.2;
        double b = 0.75;
        double K = k1 * (1.0 - b + b * docInfo.docTermsInfo.size() / docInfo.avgdl);
        Map<String, Integer> tokens = docInfo.queryTermsInfo.queryTokenCountMap;
        Map<String, Double> idf = docInfo.termIdf;
        // query或者doc包含的term太少了
        if (docInfo.docTermsInfo.size() <= 1 || tokens.size() <= 1)
            return 0.0;
        HashMap<String, Double> acc = new HashMap<>();
        for (String s : tokens.keySet()) {
            acc.put(s, 0.0);
        }

        for (int i = 0; i < docInfo.docTermsInfo.size(); i++) {
            if(docInfo.getTermHit(i) == 0) continue;
            int next_i = docInfo.getNextTermHit(i);
            if(next_i == -1) continue;
            String t1 = docInfo.getTerm(i);
            String t2 = docInfo.getTerm(next_i);
            //logger.info(i + " t1:"+t1+" "+next_i+" t2:"+t2);
            if (!tokens.containsKey(t1) || !tokens.containsKey(t2))
                continue;
            if (t1 == t2)
                continue;
            
            // 查找在query里面的匹配情况
            for(int m=0, n=next_i-i; n<docInfo.queryTermsInfo.size(); m++, n++) {
                if(docInfo.queryTermsInfo.getTerm(m).equals(t1) &&
                    docInfo.queryTermsInfo.getTerm(n).equals(t2)) {
                    boost += 0.1;
                    //logger.info("query m="+m+ " n="+n+" boost:"+boost);
                    break;
                }
            }
            double dis = (double) (next_i - i);
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
            double decayFactor = 1.0;
            if (tokenUsedCount != null) {
                decayFactor = Math.exp(-tokenUsedCount.getOrDefault(tok, 0) * decay);
            }
            score_acc += decayFactor * Math.min(1.0, idf.getOrDefault(tok, 0.0)) * scoreTP * (k1 + 1.0) / (scoreTP + K);
        }
        docInfo.boost = boost;
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

    private int getDocInfo(BM25tpDocInfo[] docInfos, IndexSearcher searcher, String factorField) 
        throws IOException {
        IndexReader reader = searcher.getIndexReader();
        Iterator<LeafReaderContext> leaves = reader.leaves().iterator();
        LeafReaderContext leaf = null;
        int endDoc = 0;
        for (int i = 0; i < docInfos.length; i++) {
            // logger.info("Get Doc Info: =========" + i + "===========================");
            if (docInfos[i].docid >= endDoc) {
                do {
                    leaf = leaves.next();
                    endDoc = leaf.docBase + leaf.reader().maxDoc();
                } while (docInfos[i].docid >= endDoc);
            }
            // 需要tf， idf， pos信息
            // Read the document's document vector.
            Terms vector = reader.getTermVector(docInfos[i].docid, factorField); 
            TermsEnum terms = vector.iterator(); // 本doc里term迭代

            PostingsEnum positions = null;
            PostingsEnum pe = null;
            BytesRef term;
            long termCount = 0;

            while ((term = terms.next()) != null) {
                String termstr = term.utf8ToString(); // Get the text string of the term.
                long freq = terms.totalTermFreq(); // 确实是本doc里term的出现次数
                // logger.info("term:"+termstr+" freq:"+freq);
                termCount += freq;
                docInfos[i].docTermFreq.put(termstr, freq);
                // positions = terms.postings(positions, PostingsEnum.POSITIONS);
                // positions.nextDoc(); 
                pe = terms.postings(pe, PostingsEnum.OFFSETS);
                pe.nextDoc();
                // you still need to move the cursor
                // now accessing the occurrence position of the terms by iteratively calling
                // nextPosition() Each time you call posting.nextDoc(), it moves the cursor of the posting list
                // to the next position and returns the docid of the current entry (document). Note that this is an
                // internal Lucene docid. It returns PostingsEnum.NO_MORE_DOCS if you have reached the end of the
                // posting list.

                for (int ii = 0; ii < freq; ii++) {
                    //int p = positions.nextPosition();
                    pe.nextPosition();
                    if (pe.startOffset() >= 0) {
                        int offset = pe.startOffset();
                        if (docInfos[i].queryTermsInfo.has(termstr)) {
                            docInfos[i].docTermsInfo.addTermPos(termstr, offset, 1); //命中
                            docInfos[i].docTermCount += 1;
                        } else {
                            docInfos[i].docTermsInfo.addTermPos(termstr, offset, 0); //没命中
                        }
                    }
                }
            }
            docInfos[i].docTermCount = termCount;
            docInfos[i].docTermsInfo.sortAndRawText("d");
            docInfos[i].docString = docInfos[i].docTermsInfo.rawText;
            docInfos[i].setCharHit();
            // logger.info(docInfos[i].docTermsInfo.toString());
        }
        return 0;
    }

    @Override
    public TopDocs rescore(TopDocs topDocs, IndexSearcher searcher, RescoreContext rescoreContext) throws IOException {
        double boost = 2.2;
        BM25tpRescoreContext context = (BM25tpRescoreContext) rescoreContext;

        IndexReader reader = searcher.getIndexReader();
        long sumDocFrq = reader.getSumDocFreq(context.factorField); // sum of doc count of terms
        // Returns the sum of TermsEnum#totalTermFreq for all terms in this field.
        long totalTermFrq = reader.getSumTotalTermFreq(context.factorField); 

        long N = reader.numDocs();
        double avgdl = (double) totalTermFrq / N;
        Set<Integer> topDocIds = getTopDocIds(topDocs);
        //logger.info("========================================BM25tpRescorer====================================");
        //logger.info("doc_num:" + N + " SumDocFreq:" + sumDocFrq + " TotalTermFrq:" + totalTermFrq + " avgdl:" + avgdl + " topdocs.size:" + topDocIds.size());
        int end = Math.min(topDocs.scoreDocs.length, rescoreContext.getWindowSize());
        // 对query进行分词，转化成ArrayList
        BM25tpTermInfo queryTermsInfo = getTokens(context.query, context.factorField, context.analyzer);
        //logger.info(context.query + " => " + queryTermsInfo);

        // 计算每个term的idf
        Map<String, Double> termIdf = getQueryTokenIDF(N, queryTermsInfo.queryTokenCountMap, context.factorField, reader);
        Map<Integer, Float> docIdToScore = new HashMap<>();

        // ScoreDoc[] sortedByDocId = new ScoreDoc[end];
        BM25tpDocInfo[] docInfos = new BM25tpDocInfo[end];
        for (int i = 0; i < end; i++) {
            docInfos[i] = new BM25tpDocInfo();
            docInfos[i].docid = topDocs.scoreDocs[i].doc;
            docInfos[i].bm25ScoreOriginal = topDocs.scoreDocs[i].score;
            docInfos[i].query = context.query;
            docInfos[i].avgdl = avgdl;
            docInfos[i].termIdf = termIdf;
            docInfos[i].queryTermsInfo = queryTermsInfo; // query分词结果
            docInfos[i].bm25OrderOriginal = i;
            docInfos[i].N = N;
            docInfos[i].fieldName = context.factorField;
            docInfos[i].reader = reader;
        }
        // System.arraycopy(topDocs.scoreDocs, 0, sortedByDocId, 0, end);
        Arrays.sort(docInfos, (a, b) -> a.docid - b.docid); // Safe because doc ids >= 0
        getDocInfo(docInfos, searcher, context.factorField);

        // 首先计算每个doc的term proximity得分，和原来的bm25得分求和后排序
        for (int i = 0; i < docInfos.length; i++) {
            //logger.info("Calc tp_score and adjacet score ==========" + i + "================");
            // logger.info(docInfos[i].docTermsInfo.toString());
            if(docInfos[i].docCharHitCount * 2.0 < docInfos[i].docString.length()) {
                // 要求char命中率大于50%
                docInfos[i].bm25tpScoreNoDecay = (double)-1.0;
                continue;
            }
            double tp_score = calcTermProximity(docInfos[i], boost, null, 1.0f);
            // logger.info("START CALC TERM ADJACENT CHAR MATCH");
            calcTermAdjacentCharMatch(docInfos[i], 3);
            docInfos[i].termProximityScore = tp_score;
            docInfos[i].bm25tpScoreNoDecay = docInfos[i].termProximityScore 
                + docInfos[i].charHitScore + docInfos[i].bm25ScoreOriginal;
        }
        // 按照bm25*char_boost+term_proximity得分排序
        Arrays.sort(docInfos, (a, b) -> {
            if (a.bm25tpScoreNoDecay == b.bm25tpScoreNoDecay) {
                if(a.bm25OrderOriginal < b.bm25OrderOriginal)
                    return -1;
                return 1;
            }
            if (a.bm25tpScoreNoDecay > b.bm25tpScoreNoDecay) {
                return -1;
            }
            return 1;
        });
        for(int i=0;i<docInfos.length;i++) {
            docInfos[i].bm25OrderWithTermProximity = i;
        }
        // 加上decay项计算得分
        Map<String, Integer> tokenUsedCount = new HashMap<>();
        for (int i = 0; i < docInfos.length; i++) {
            // termHitCount还没计算，这段先取消
            // TODO: 计算termHitCount, 并整理这段逻辑
            //if (docInfos[i].termCount * context.minimum_should_match > docInfos[i].termHitCount) {
            //    docInfos[i].bm25tpScoreWithDecay = (float) 0.0;
            //    continue;
            // }
            if(docInfos[i].bm25tpScoreNoDecay < 0.0) {
                docInfos[i].bm25tpScoreWithDecay = (float)-1.0;
                continue;
            }
            double tp_score_decay = calcTermProximity(docInfos[i], boost, tokenUsedCount, context.decay);
            double bm25_score_decay = calcBM25(docInfos[i], boost, tokenUsedCount, context.decay);

            docInfos[i].bm25tpScoreWithDecay = (float) (tp_score_decay 
                        + docInfos[i].charHitScore + bm25_score_decay ); // 取代原有得分
            docIdToScore.put(docInfos[i].docid, docInfos[i].bm25tpScoreWithDecay);
        }
        Arrays.sort(docInfos, (a, b) -> {
            if (a.bm25tpScoreWithDecay > b.bm25tpScoreWithDecay) {
                return -1;
            }
            return 1;
        });
        ArrayList<ScoreDoc> newScoreDocs = new ArrayList();
        for(int i=0;i<docInfos.length;i++) {
            docInfos[i].bm25OrderWithDecay = i;
            //logger.info(docInfos[i].toString());
            if(docInfos[i].bm25tpScoreWithDecay > 0.0) {
                newScoreDocs.add(new ScoreDoc(docInfos[i].docid,
                    docInfos[i].bm25tpScoreWithDecay));
            }
        }
        if(newScoreDocs.size() == 0) {
            newScoreDocs.add(new ScoreDoc(docInfos[0].docid,
                docInfos[0].bm25tpScoreWithDecay));
        }
        // float base_score = topDocs.scoreDocs[0].score; 
        // 设置一个基础分，让新的得分比原来都大，不改变rescore window size之后的顺序
        // 不在rescore窗口内，或者不符合过滤条件的，都舍弃
        ScoreDoc[] scoreDocs = null;
        scoreDocs = newScoreDocs.toArray(new ScoreDoc[newScoreDocs.size()]);
        TopDocs newTopDocs = new TopDocs(topDocs.totalHits, scoreDocs);
        return newTopDocs;
        // for (int i = 0; i < end; i++) {
        //     if (docIdToScore.containsKey(topDocs.scoreDocs[i].doc)) {
        //         topDocs.scoreDocs[i].score = docIdToScore.get(topDocs.scoreDocs[i].doc);
        //     }
        // }
        // // Sort by score descending, then docID ascending, just like lucene's
        // // QueryRescorer
        // Arrays.sort(topDocs.scoreDocs, (a, b) -> {
        //     if (a.score > b.score) {
        //         return -1;
        //     }
        //     if (a.score < b.score) {
        //         return 1;
        //     }
        //     // Safe because doc ids >= 0
        //     return a.doc - b.doc;
        // });
        // return topDocs;
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
