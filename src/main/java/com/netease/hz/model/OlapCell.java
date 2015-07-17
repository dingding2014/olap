package com.netease.hz.model;

import com.google.gson.Gson;
import org.olap4j.Position;
import org.olap4j.metadata.Member;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The presentation wrapper of cell class of CellSet returned by mdx query
 * Created by zhifei on 3/25/15.
 */
public class OlapCell {
    private Map<String, Set<String>> coordinate = new HashMap<String, Set<String>>();
    private Object v;

    /**
     * The para positions is the coordinates of this value
     * @param positions
     * @param value
     */
    public OlapCell(Position[] positions, Object value) {
        v = value;
        for(int i=0; i<positions.length; i++) {
            Set<String> set = new HashSet<String>();

            for(Member member : positions[i].getMembers()) {
                set.add(member.getUniqueName());
            }

            coordinate.put(String.valueOf(i),set);

        }

    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

}
