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
import java.util.Collections;
import java.util.Comparator;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BM25tpTermInfo {
    private static class TermPos {
        public String term; // term词
        // public int pos; // term位置
        public int hit = -1; // term有没有命中,0表示没命中，1表示命中
        public int offset = -1; // term在doc或者query里的char偏移量

        public TermPos(String t, int offset) {
            this.term = t;
            this.offset = offset;
        }
        public TermPos(String t, int offset, int hit ) {
            this.term = t;
            this.offset = offset;
            this.hit = hit;
        }
    }

    ArrayList<TermPos> termPosArr = new ArrayList<TermPos>();
    Map<String, Integer> queryTokenCountMap = new HashMap<String, Integer>();
    public String rawText = "";
    public int[] charHit; // 每个位置的char有无命中，不命中为-1， 命中为当前词的index
    // private final Logger logger = LogManager.getLogger(BM25tpTermInfo.class);

    public BM25tpTermInfo() {}

    public void addTermPos(String t, int offset) {
        termPosArr.add(new TermPos(t, offset));
        queryTokenCountMap.put(t, queryTokenCountMap.getOrDefault(t, 0) + 1);
    }
    public void addTermPos(String t, int offset, int hit) {
        termPosArr.add(new TermPos(t, offset, hit));
        queryTokenCountMap.put(t, queryTokenCountMap.getOrDefault(t, 0) + 1);
    }

    public void sortAndRawText(String pad) {
        Collections.sort(termPosArr, new Comparator<TermPos>() {
        @Override
            public int compare(TermPos a, TermPos b) {
                if (a.offset != b.offset) {
                    return a.offset - b.offset;
                }
                return a.term.length() - b.term.length();
            }
        });
        int size = termPosArr.size();
        int length = termPosArr.get(size-1).offset + termPosArr.get(size-1).term.length();
        StringBuffer sb = new StringBuffer(length);
        charHit = new int[length];
        // logger.info("termPosArr.size="+size+" length="+length);
        for(int i=0;i<length;i++) {
            sb.append(pad);
            charHit[i] = -1; 
        }
        // bad case: 总要求 分成了 总要 要求
        for(int i=0;i<termPosArr.size();i++) {
            for(int j=0;j<termPosArr.get(i).term.length();j++) {
                int coffset = j+termPosArr.get(i).offset;
                sb.setCharAt(coffset, termPosArr.get(i).term.charAt(j));
                if(charHit[coffset] == -1) {
                    charHit[coffset] = i;
                }
            }
        }
        rawText = sb.toString();
    }

    public int getHitted(int offset) {
        return charHit[offset];
    }
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("TokenToText: ").append(rawText);
        sb.append(" [");
        for(int i=0;i<termPosArr.size();i++) {
            sb.append(" term:").append(termPosArr.get(i).term);
            sb.append(" hit:").append(termPosArr.get(i).hit);
            sb.append(" offset:").append(termPosArr.get(i).offset);
            sb.append(" hit:");
            for(int j=0;j<termPosArr.get(i).term.length();j++) {
                int of = termPosArr.get(i).offset + j;
                sb.append(charHit[of]).append(",");
            }
            sb.append(" // ");
        }
        sb.append("],");
        return sb.toString();
    }

    public boolean has(String t) {
        return queryTokenCountMap.containsKey(t);
    }

    public int size() {
        return termPosArr.size();
    }

    public String getTerm(int i) {
        return termPosArr.get(i).term;
    }

    public int getTermLength(int i) {
        return termPosArr.get(i).term.length();
    }

    public int getOffset(int i) {
        return termPosArr.get(i).offset;
    }

    public int getHit(int i) {
        return termPosArr.get(i).hit;
    }
}
