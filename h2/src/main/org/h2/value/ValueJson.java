package org.h2.value;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

import org.h2.message.DbException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

public class ValueJson extends Value {
    
    private JsonNode json;
    private String string;
    
    private static final ObjectMapper mapper = new ObjectMapper();
    
    ValueJson(String str) throws IOException {
        char c = str.charAt(0);
        if ('{' == c) {
            ObjectNode node = (ObjectNode) mapper.readTree(str);
            this.json = node;
        } else if ('[' == c) {
            ObjectMapper mapper = new ObjectMapper();
            ArrayNode arr = (ArrayNode) mapper.readTree(str);
            this.json = arr;
        } else {
            TextNode stringVal = new TextNode(str);
            this.json = stringVal;
        }
        this.string = str.replaceAll("\n", "");
    }
    
    ValueJson(JsonNode json) {
        this.json = json;
        this.string = json.toString();
    }
    
    public static ValueJson get(String str) throws IOException {
        return new ValueJson(str);
    }
    
    public static Value get(JsonNode json){
        return new ValueJson(json);
    }

    @Override
    public String getSQL() {
        return String.format("'%s'::JSON", getString());
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_JSON;
    }

    @Override
    public int getValueType() {
        return JSON;
    }

    @Override
    public String getString() {
        return string;
    }

    @Override
    public JsonNode getObject() {
        return json;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public int hashCode() {
        return json.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ValueJson) {
            return this.json.equals(((ValueJson) other).getObject());
        } else if (other instanceof JsonNode) {
            return this.json.equals(other);
        }
        return false;
    }

    @Override
    public Value subtract(Value v) {
        JsonNode json = this.json;
        String toSub = v.getString();
        if (json.isObject() && json.has(toSub)) {
            /* Used copy to save original object */
            ObjectNode result = json.deepCopy();
            String key = v.getString();
            result.remove(key);
            return get(result);
        } else if (json.isArray()) {
            ArrayNode result = mapper.createArrayNode();
            String value = v.getString();
            for (JsonNode node : json) {
                if (!node.asText().equals(value)) {
                    result.add(node);
                }
            }
            return get(result);
        } else {
            return this;
        }
    }
    
    @Override
    public Value add(Value v) {
        JsonNode l = this.json.deepCopy();
        JsonNode r = ((ValueJson) v).getObject();
        if (l.isArray()) {
            ArrayNode lA = (ArrayNode) l;
            if (r.isArray()) {
                ArrayNode rA = (ArrayNode) r;
                lA.addAll(rA);
            } else {
                lA.add(r);
            }
            return get(lA);
        } else if (l.isObject() && r.isObject()){
           ObjectNode lO = (ObjectNode) l;
           ObjectNode rO = (ObjectNode) r;
           lO.putAll(rO);
           return get(lO);
        } else {
            ArrayNode result = mapper.createArrayNode();
            result.add(l).add(r);
            return get(result);
        }
            
    }

    @Override
    public int compareTypeSafe(Value v, CompareMode mode) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder) {
        // TODO Auto-generated method stub
        return null;
    }
}
