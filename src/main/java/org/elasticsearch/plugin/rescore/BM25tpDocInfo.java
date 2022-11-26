package org.elasticsearch.plugin.rescore;

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

class BM25tpDocInfo {
    public int docid;
    public float bm25ScoreOriginal; // es计算的bm25得分，不知道boost的具体算法，有时和我用bm25公式计算的结果不一致
    public double termProximityScore; // 不包含decay的term proximity项
    public float bm25ScoreByPlugin; // 
    public float bm25tpScoreWithDecay; // 包含decay的bm25+term_proximity项
    public double boost;

    public int bm25OrderOriginal; // rescore之前的序号
    public int bm25OrderWithTermProximity; // 原始得分+term proximity之后的序号
    public int bm25OrderWithDecay; // 加上衰减项之后的序号

    public double avgdl;
    public long termCount; // 当前doc内term数量
    public String docString;
    public double termfreqs = 0.0; // 当前doc内命中query term的数量
    public Map<String, Double> termFreqInDoc;
    public Map<String, Integer> queryTokenCountMap;
    public ArrayList<String> queryTokenArr;
    public int queryTokenCount = 0;
    public Map<String, Double> termIdf;
    public TermPos[] termPosArr;

    private static class TermPos {
        public String term;
        public int pos;

        public TermPos(String t, int p) {
            this.term = t;
            this.pos = p;
        }
    }

    public ArrayList<TermPos> termPos;

    public BM25tpDocInfo() {
        termFreqInDoc = new HashMap<>();
        termPos = new ArrayList<>();
        bm25ScoreOriginal = -1.0f;
        termProximityScore = -1.0;
        bm25tpScoreWithDecay = -1.0f;
        boost = -1.0;
        bm25OrderOriginal = -1;
        bm25OrderWithTermProximity = -1;
        bm25OrderWithDecay = -1;
        termCount = -1;
        termfreqs = -1;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("BM25tpDocInfo --> ");
        sb.append("docid:").append(docid).append(", ");
        sb.append("docString:").append(docString).append(", ");
        sb.append("bm25ScoreOriginal:").append(bm25ScoreOriginal).append(", ");
        sb.append("termProximityScore:").append(termProximityScore).append(", ");
        sb.append("bm25tpScoreWithDecay:").append(bm25tpScoreWithDecay).append(", ");
        sb.append("boost:").append(boost).append(", ");
        sb.append("bm25OrderOriginal:").append(bm25OrderOriginal).append(", ");
        sb.append("bm25OrderWithTermProximity:").append(bm25OrderWithTermProximity).append(", ");
        sb.append("bm25OrderWithDecay:").append(bm25OrderWithDecay).append(", ");
        sb.append("doctermCount:").append(termCount).append(", ");
        sb.append("doctermfreqs:").append(termfreqs).append(", ");
        sb.append("termFreqInDoc:").append(termFreqInDoc.size()).append(", ");
        sb.append("queryTokenArr:").append(queryTokenArr.size()).append(", ");
        sb.append("termPosArr").append(termPosArr.length).append(", ");
        sb.append("[");
        for(int i=0;i<termPosArr.length;i++) {
            sb.append(termPosArr[i].term).append(":").append(termPosArr[i].pos).append(" ");
        }
        sb.append("], ");
        return sb.toString();
    }
    public void addTermPos(String t, int p) {
        termPos.add(new TermPos(t, p));
    }

    public void setTermFreqs() {
        for (double t : termFreqInDoc.values()) {
            termfreqs += t;
        }
    }
    public void setTermPosArr() {
            termPosArr = new TermPos[termPos.size()];
            termPos.toArray(termPosArr);
            Arrays.sort(termPosArr, (a, b) -> a.pos - b.pos); // Safe because doc ids >= 0
    }

    public int getTermPosSize() {
        return termPosArr.length;
    }

    public String getTerm(int i) {
        return termPosArr[i].term;
    }

    public int getPos(int i) {
        return termPosArr[i].pos;
    }
}