/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.HashMap;
import java.util.Map;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.URIResolver;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stax.StAXResult;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.h2.message.DbException;
import org.h2.message.TraceObject;
import org.h2.value.Value;
import org.w3c.dom.Node;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Represents a SQLXML value.
 */
public final class JdbcSQLXML extends JdbcLob implements SQLXML {

    private static final Map<String,Boolean> secureFeatureMap = new HashMap<>();
    private static final EntityResolver NOOP_ENTITY_RESOLVER = (pubId, sysId) -> new InputSource(new StringReader(""));
    private static final URIResolver NOOP_URI_RESOLVER = (href, base) -> new StreamSource(new StringReader(""));

    static {
        secureFeatureMap.put(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        secureFeatureMap.put("http://apache.org/xml/features/disallow-doctype-decl", true);
        secureFeatureMap.put("http://xml.org/sax/features/external-general-entities", false);
        secureFeatureMap.put("http://xml.org/sax/features/external-parameter-entities", false);
        secureFeatureMap.put("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
    }

    private DOMResult domResult;

    /**
     * Underlying stream for SAXResult, StAXResult, and StreamResult.
     */
    private Closeable closable;

    /**
     * INTERNAL
     * @param conn to use
     * @param value for this JdbcSQLXML
     * @param state of the LOB
     * @param id of the trace object
     */
    public JdbcSQLXML(JdbcConnection conn, Value value, State state, int id) {
        super(conn, value, state, TraceObject.SQLXML, id);
    }

    @Override
    void checkReadable() throws SQLException, IOException {
        checkClosed();
        if (state == State.SET_CALLED) {
            if (domResult != null) {
                Node node = domResult.getNode();
                domResult = null;
                TransformerFactory factory = TransformerFactory.newInstance();
                try {
                    Transformer transformer = factory.newTransformer();
                    DOMSource domSource = new DOMSource(node);
                    StringWriter stringWriter = new StringWriter();
                    StreamResult streamResult = new StreamResult(stringWriter);
                    transformer.transform(domSource, streamResult);
                    completeWrite(conn.createClob(new StringReader(stringWriter.toString()), -1));
                } catch (Exception e) {
                    throw logAndConvert(e);
                }
                return;
            } else if (closable != null) {
                closable.close();
                closable = null;
                return;
            }
            throw DbException.getUnsupportedException("Stream setter is not yet closed.");
        }
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        return super.getBinaryStream();
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        return super.getCharacterStream();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Source> T getSource(Class<T> sourceClass) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode(
                        "getSource(" + (sourceClass != null ? sourceClass.getSimpleName() + ".class" : "null") + ')');
            }
            checkReadable();
            // see https://cheatsheetseries.owasp.org/cheatsheets/XML_External_Entity_Prevention_Cheat_Sheet.html
            if (sourceClass == null || sourceClass == DOMSource.class) {
                DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                for (Map.Entry<String,Boolean> entry : secureFeatureMap.entrySet()) {
                    try {
                        dbf.setFeature(entry.getKey(), entry.getValue());
                    } catch (Exception ignore) {/**/}
                }
                dbf.setXIncludeAware(false);
                dbf.setExpandEntityReferences(false);
                dbf.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
                DocumentBuilder db = dbf.newDocumentBuilder();
                db.setEntityResolver(NOOP_ENTITY_RESOLVER);
                return (T) new DOMSource(db.parse(new InputSource(value.getInputStream())));
            } else if (sourceClass == SAXSource.class) {
                SAXParserFactory spf = SAXParserFactory.newInstance();
                for (Map.Entry<String,Boolean> entry : secureFeatureMap.entrySet()) {
                    try {
                        spf.setFeature(entry.getKey(), entry.getValue());
                    } catch (Exception ignore) {/**/}
                }
                XMLReader reader = spf.newSAXParser().getXMLReader();
                reader.setEntityResolver(NOOP_ENTITY_RESOLVER);
                return (T) new SAXSource(reader, new InputSource(value.getInputStream()));
            } else if (sourceClass == StAXSource.class) {
                XMLInputFactory xif = XMLInputFactory.newInstance();
                xif.setProperty(XMLInputFactory.SUPPORT_DTD, false);
                xif.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
                xif.setProperty("javax.xml.stream.isSupportingExternalEntities", false);
                return (T) new StAXSource(xif.createXMLStreamReader(value.getInputStream()));
            } else if (sourceClass == StreamSource.class) {
                TransformerFactory tf = TransformerFactory.newInstance();
                tf.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
                tf.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
                tf.setURIResolver(NOOP_URI_RESOLVER);
                tf.newTransformer().transform(new StreamSource(value.getInputStream()),
                                                new SAXResult(new DefaultHandler()));
                return (T) new StreamSource(value.getInputStream());
            }
            throw unsupported(sourceClass.getName());
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    @Override
    public String getString() throws SQLException {
        try {
            debugCodeCall("getString");
            checkReadable();
            return value.getString();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    @Override
    public OutputStream setBinaryStream() throws SQLException {
        try {
            debugCodeCall("setBinaryStream");
            checkEditable();
            state = State.SET_CALLED;
            return new BufferedOutputStream(setClobOutputStreamImpl());
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    @Override
    public Writer setCharacterStream() throws SQLException {
        try {
            debugCodeCall("setCharacterStream");
            checkEditable();
            state = State.SET_CALLED;
            return setCharacterStreamImpl();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Result> T setResult(Class<T> resultClass) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode(
                        "setResult(" + (resultClass != null ? resultClass.getSimpleName() + ".class" : "null") + ')');
            }
            checkEditable();
            if (resultClass == null || resultClass == DOMResult.class) {
                domResult = new DOMResult();
                state = State.SET_CALLED;
                return (T) domResult;
            } else if (resultClass == SAXResult.class) {
                SAXTransformerFactory transformerFactory = (SAXTransformerFactory) TransformerFactory.newInstance();
                TransformerHandler transformerHandler = transformerFactory.newTransformerHandler();
                Writer writer = setCharacterStreamImpl();
                transformerHandler.setResult(new StreamResult(writer));
                SAXResult saxResult = new SAXResult(transformerHandler);
                closable = writer;
                state = State.SET_CALLED;
                return (T) saxResult;
            } else if (resultClass == StAXResult.class) {
                XMLOutputFactory xof = XMLOutputFactory.newInstance();
                Writer writer = setCharacterStreamImpl();
                StAXResult staxResult = new StAXResult(xof.createXMLStreamWriter(writer));
                closable = writer;
                state = State.SET_CALLED;
                return (T) staxResult;
            } else if (StreamResult.class.equals(resultClass)) {
                Writer writer = setCharacterStreamImpl();
                StreamResult streamResult = new StreamResult(writer);
                closable = writer;
                state = State.SET_CALLED;
                return (T) streamResult;
            }
            throw unsupported(resultClass.getName());
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    @Override
    public void setString(String value) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCodeCall("getSource", value);
            }
            checkEditable();
            completeWrite(conn.createClob(new StringReader(value), -1));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

}
