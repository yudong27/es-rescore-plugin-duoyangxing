package org.elasticsearch.plugin.rescore;

import org.apache.lucene.index.*;

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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class BM25tpDocInfo {
    public int docid;
    public String docString;
    public float bm25ScoreOriginal; // es计算的bm25得分，不知道boost的具体算法，有时和我用bm25公式计算的结果不一致
    public double termProximityScore; // 不包含decay的term proximity项
    public double bm25tpScoreNoDecay; // bm25+tp+char没有decay时的得分，用于pass1排序
    public float bm25tpScoreWithDecay; // 包含decay的bm25+term_proximity+char项，用于pass2排序
    public double boost;
    // public double char_boost; // term左右单字对bm25算法的boost
    public double charHitScore; // 单个字命中的得分，目前是term idf除总长度加和
    // public int charHitCount;

    public int bm25OrderOriginal; // rescore之前的序号
    public int bm25OrderWithTermProximity; // 原始得分+term proximity之后的序号
    public int bm25OrderWithDecay; // 加上衰减项之后的序号

    public long N; // doc数量，用于计算idf
    public IndexReader reader;// 用于读取一些信息
    public String fieldName; // 用于读取此field的内容
    public double avgdl;
    public long docTermCount = 0; // 当前doc内term总数量，不管有没有命中query
    public long docTermHitCount = 0; // 当前doc内命中query term的数量
    public long docCharHitCount = 0; // 单字命中率
    public Map<String, Long> docTermFreq; // 当前doc里每个term的数量，不管有没有命中query

    public String query;
    public BM25tpTermInfo queryTermsInfo; // query分词结果及位置
    public Map<String, Double> termIdf; // query分词后term的idf值

    public BM25tpTermInfo docTermsInfo; // 保存在doc里面的term位置
    public int[] termHitOrNot; // 记录DocString里每个char是否被命中过

    // private final Logger logger = LogManager.getLogger(BM25tpDocInfo.class);

    public BM25tpDocInfo() {
        docTermFreq = new HashMap<>();
        docTermsInfo = new BM25tpTermInfo();
        // termPos = new ArrayList<>();
        bm25ScoreOriginal = -1.0f;
        termProximityScore = -1.0;
        bm25tpScoreWithDecay = -1.0f;
        boost = -1.0;
        bm25OrderOriginal = -1;
        bm25OrderWithTermProximity = -1;
        bm25OrderWithDecay = -1;
        docString = "";
        docTermHitCount = 0;
        docCharHitCount = 0;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("BM25tpDocInfo --> ");
        sb.append("docid:").append(docid).append(", ");
        sb.append("docString:").append(docString).append(", ");
        sb.append("bm25ScoreOriginal:").append(bm25ScoreOriginal).append(", ");
        sb.append("termProximityScore:").append(termProximityScore).append(", ");
        sb.append("bm25tpScoreNoDecay:").append(bm25tpScoreNoDecay).append(", ");
        sb.append("bm25tpScoreWithDecay:").append(bm25tpScoreWithDecay).append(", ");
        sb.append("boost:").append(boost).append(", ");
        //sb.append("charHitCount:").append(charHitCount).append(", ");
        // sb.append("char_boost:").append(char_boost).append(", ");
        sb.append("charHitScore:").append(charHitScore).append(", ");
        sb.append("bm25OrderOriginal:").append(bm25OrderOriginal).append(", ");
        sb.append("bm25OrderWithTermProximity:").append(bm25OrderWithTermProximity).append(", ");
        sb.append("bm25OrderWithDecay:").append(bm25OrderWithDecay).append(", ");
        sb.append("termFreqInDoc:").append(docTermFreq.size()).append(", ");
        sb.append("docTokenArr:").append(docTermsInfo.toString()).append(", ");
        return sb.toString();
    }

    public void setCharHit() {
        termHitOrNot = new int[docString.length()];
        for(int i=0;i<docTermsInfo.size();i++) {
            int startoffset = docTermsInfo.getOffset(i);
            int endoffset = startoffset + docTermsInfo.getTermLength(i);
            for(int j=startoffset; j< endoffset;j++) {
                termHitOrNot[j] = 1;
            }
        }
        for(int i=0;i<docString.length();i++) {
            if(query.indexOf(docString.charAt(i)) != -1) {
                docCharHitCount++;
            }
        }
    }
    public String getTerm(int i) {
        return docTermsInfo.getTerm(i);
    }

    public int getOffset(int i) {
        return docTermsInfo.getOffset(i);
    }
    public int getTermHit(int i) {
        return docTermsInfo.getHit(i);
    }

    public int getNextTermHit(int i) {
        int endoffset = docTermsInfo.getOffset(i) + docTermsInfo.getTermLength(i);
        for(int j=i+1; j<docTermsInfo.size(); j++){
            // logger.info("current_i:"+i+" maybe_next_i:"+j+" offset:"+docTermsInfo.getOffset(j)+" endoffset:"+endoffset);
            if(docTermsInfo.getHit(j) == 1 &&
                docTermsInfo.getOffset(j) >= endoffset
            )
                return j;
        }
        return -1;
    }
    private double getTokenIDF(String token){
        double idf = 0.0;
        try{
            int docfreq = reader.docFreq(new Term(fieldName, token));
            idf = Math.log(1.0 + (N - docfreq + 0.5) / (docfreq + 0.5));
        } catch (IOException e) {
            ;
        }
        return idf;
    }

    public double docTermRightHit(int docIndex, int queryIndex, int char_window_size) {
        double score = 0.0;
        int deoffset = docTermsInfo.getOffset(docIndex) + docTermsInfo.getTermLength(docIndex);
        int qeoffset = queryTermsInfo.getOffset(queryIndex) + queryTermsInfo.getTermLength(queryIndex);
        for(int j = 0; j < char_window_size; j++) {
            if(deoffset + j >= docString.length()) break;
            if(qeoffset + j >= query.length()) break;
            int hitIndex = docTermsInfo.getHitted(deoffset+j);
            if(hitIndex < 0) continue;
            String hitTerm = docTermsInfo.getTerm(hitIndex);
            // 此char对应的term没有命中query，获取其idf
            //logger.info("Right docCharOffset:"+(deoffset+j)+"/"+docString.charAt(deoffset +j)
            //            +" queryCharOffset:"+(qeoffset+j)+"/"+query.charAt(qeoffset +j) + " hitIndex:"+hitIndex
            //            +" term:"+hitTerm);
            if(queryTermsInfo.has(hitTerm)) continue;
            if(docString.charAt(deoffset + j) == query.charAt(qeoffset + j)) {
                double idf = getTokenIDF(hitTerm);
                score += idf/docString.length();
            }
        }
        return score;
    }

     public double docTermLeftHit(int docIndex, int queryIndex, int char_window_size) {
        double score = 0.0;
        int deoffset = docTermsInfo.getOffset(docIndex) - 1;
        int qeoffset = queryTermsInfo.getOffset(queryIndex) - 1;
        for(int j=0; j<char_window_size; j++) {
            if(deoffset - j < 0) break;
            if(qeoffset - j < 0) break;
            int hitIndex = docTermsInfo.getHitted(deoffset - j);
            if(hitIndex < 0) continue;
            String hitTerm = docTermsInfo.getTerm(hitIndex);
            //logger.info("Left docCharOffset:"+(deoffset-j)+"/"+docString.charAt(deoffset - j)
            //            +" queryCharOffset:"+(qeoffset-j)+"/"+query.charAt(qeoffset - j)+" hitIndex:"+hitIndex
            //            +" term:"+hitTerm);
            if(queryTermsInfo.has(hitTerm)) continue;
            if(docString.charAt(deoffset - j) == query.charAt(qeoffset - j)) {
                double idf = getTokenIDF(hitTerm);
                score += idf/docString.length();
            }
        }
        return score;
    }


}