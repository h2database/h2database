/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.server;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Vector;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.RequestDispatcher;
import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import jakarta.servlet.http.HttpUpgradeHandler;
import jakarta.servlet.http.Part;

import org.h2.server.web.JakartaWebServlet;
import org.h2.test.TestBase;
import org.h2.test.TestDb;
import org.h2.util.Utils10;

/**
 * Tests the Jakarta Web Servlet for the H2 Console.
 */
public class TestJakartaWeb extends TestDb {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws Exception {
        testServlet();
    }

    private void testServlet() throws Exception {
        JakartaWebServlet servlet = new JakartaWebServlet();
        final HashMap<String, String> configMap = new HashMap<>();
        configMap.put("ifExists", "");
        configMap.put("", "");
        ServletConfig config = new ServletConfig() {

            @Override
            public String getServletName() {
                return "H2Console";
            }

            @Override
            public Enumeration<String> getInitParameterNames() {
                return new Vector<>(configMap.keySet()).elements();
            }

            @Override
            public String getInitParameter(String name) {
                return configMap.get(name);
            }

            @Override
            public ServletContext getServletContext() {
                return null;
            }

        };
        servlet.init(config);


        TestHttpServletRequest request = new TestHttpServletRequest();
        request.setPathInfo("/");
        TestHttpServletResponse response = new TestHttpServletResponse();
        TestServletOutputStream out = new TestServletOutputStream();
        response.setServletOutputStream(out);
        servlet.doGet(request, response);
        assertContains(out.toString(), "location.href = 'login.jsp");
        servlet.destroy();
    }

    /**
     * A HTTP servlet request for testing.
     */
    static class TestHttpServletRequest implements HttpServletRequest {

        private String pathInfo;

        void setPathInfo(String pathInfo) {
            this.pathInfo = pathInfo;
        }

        @Override
        public Object getAttribute(String name) {
            return null;
        }

        @Override
        public Enumeration<String> getAttributeNames() {
            return new Vector<String>().elements();
        }

        @Override
        public String getCharacterEncoding() {
            return null;
        }

        @Override
        public int getContentLength() {
            return 0;
        }

        @Override
        public String getContentType() {
            return null;
        }

        @Override
        public ServletInputStream getInputStream() throws IOException {
            return null;
        }

        @Override
        public String getLocalAddr() {
            return null;
        }

        @Override
        public String getLocalName() {
            return null;
        }

        @Override
        public int getLocalPort() {
            return 0;
        }

        @Override
        public Locale getLocale() {
            return null;
        }

        @Override
        public Enumeration<Locale> getLocales() {
            return null;
        }

        @Override
        public String getParameter(String name) {
            return null;
        }

        @Override
        public Map<String, String[]> getParameterMap() {
            return null;
        }

        @Override
        public Enumeration<String> getParameterNames() {
            return new Vector<String>().elements();
        }

        @Override
        public String[] getParameterValues(String name) {
            return null;
        }

        @Override
        public String getProtocol() {
            return null;
        }

        @Override
        public BufferedReader getReader() throws IOException {
            return null;
        }

        @Override
        @Deprecated
        public String getRealPath(String path) {
            return null;
        }

        @Override
        public String getRemoteAddr() {
            return null;
        }

        @Override
        public String getRemoteHost() {
            return null;
        }

        @Override
        public int getRemotePort() {
            return 0;
        }

        @Override
        public RequestDispatcher getRequestDispatcher(String name) {
            return null;
        }

        @Override
        public String getScheme() {
            return "http";
        }

        @Override
        public String getServerName() {
            return null;
        }

        @Override
        public int getServerPort() {
            return 80;
        }

        @Override
        public boolean isSecure() {
            return false;
        }

        @Override
        public void removeAttribute(String name) {
            // ignore
        }

        @Override
        public void setAttribute(String name, Object value) {
            // ignore
        }

        @Override
        public void setCharacterEncoding(String encoding)
                throws UnsupportedEncodingException {
            // ignore
        }

        @Override
        public String getAuthType() {
            return null;
        }

        @Override
        public String getContextPath() {
            return null;
        }

        @Override
        public Cookie[] getCookies() {
            return null;
        }

        @Override
        public long getDateHeader(String x) {
            return 0;
        }

        @Override
        public String getHeader(String name) {
            return null;
        }

        @Override
        public Enumeration<String> getHeaderNames() {
            return null;
        }

        @Override
        public Enumeration<String> getHeaders(String name) {
            return null;
        }

        @Override
        public int getIntHeader(String name) {
            return 0;
        }

        @Override
        public String getMethod() {
            return null;
        }

        @Override
        public String getPathInfo() {
            return pathInfo;
        }

        @Override
        public String getPathTranslated() {
            return null;
        }

        @Override
        public String getQueryString() {
            return null;
        }

        @Override
        public String getRemoteUser() {
            return null;
        }

        @Override
        public String getRequestURI() {
            return null;
        }

        @Override
        public StringBuffer getRequestURL() {
            return null;
        }

        @Override
        public String getRequestedSessionId() {
            return null;
        }

        @Override
        public String getServletPath() {
            return null;
        }

        @Override
        public HttpSession getSession() {
            return null;
        }

        @Override
        public HttpSession getSession(boolean x) {
            return null;
        }

        @Override
        public Principal getUserPrincipal() {
            return null;
        }

        @Override
        public boolean isRequestedSessionIdFromCookie() {
            return false;
        }

        @Override
        public boolean isRequestedSessionIdFromURL() {
            return false;
        }

        @Override
        @Deprecated
        public boolean isRequestedSessionIdFromUrl() {
            return false;
        }

        @Override
        public boolean isRequestedSessionIdValid() {
            return false;
        }

        @Override
        public boolean isUserInRole(String x) {
            return false;
        }

        @Override
        public java.util.Collection<Part> getParts() {
            return null;
        }

        @Override
        public Part getPart(String name) {
            return null;
        }

        @Override
        public boolean authenticate(HttpServletResponse response) {
            return false;
        }

        @Override
        public void login(String username, String password) {
            // ignore
        }

        @Override
        public void logout() {
            // ignore
        }

        @Override
        public ServletContext getServletContext() {
            return null;
        }

        @Override
        public AsyncContext startAsync() {
            return null;
        }

        @Override
        public AsyncContext startAsync(
                ServletRequest servletRequest,
                ServletResponse servletResponse) {
            return null;
        }

        @Override
        public boolean isAsyncStarted() {
            return false;
        }

        @Override
        public boolean isAsyncSupported() {
            return false;
        }

        @Override
        public AsyncContext getAsyncContext() {
            return null;
        }

        @Override
        public DispatcherType getDispatcherType() {
            return null;
        }

        @Override
        public long getContentLengthLong() {
            return 0;
        }

        @Override
        public String changeSessionId() {
            return null;
        }

        @Override
        public <T extends HttpUpgradeHandler> T upgrade(Class<T> handlerClass)
                throws IOException, ServletException {
            return null;
        }

    }

    /**
     * A HTTP servlet response for testing.
     */
    static class TestHttpServletResponse implements HttpServletResponse {

        ServletOutputStream servletOutputStream;

        void setServletOutputStream(ServletOutputStream servletOutputStream) {
            this.servletOutputStream = servletOutputStream;
        }

        @Override
        public void flushBuffer() throws IOException {
            // ignore
        }

        @Override
        public int getBufferSize() {
            return 0;
        }

        @Override
        public String getCharacterEncoding() {
            return null;
        }

        @Override
        public String getContentType() {
            return null;
        }

        @Override
        public Locale getLocale() {
            return null;
        }

        @Override
        public ServletOutputStream getOutputStream() throws IOException {
            return servletOutputStream;
        }

        @Override
        public PrintWriter getWriter() throws IOException {
            return null;
        }

        @Override
        public boolean isCommitted() {
            return false;
        }

        @Override
        public void reset() {
            // ignore
        }

        @Override
        public void resetBuffer() {
            // ignore
        }

        @Override
        public void setBufferSize(int arg0) {
            // ignore
        }

        @Override
        public void setCharacterEncoding(String arg0) {
            // ignore
        }

        @Override
        public void setContentLength(int arg0) {
            // ignore
        }

        @Override
        public void setContentLengthLong(long arg0) {
            // ignore
        }

        @Override
        public void setContentType(String arg0) {
            // ignore
        }

        @Override
        public void setLocale(Locale arg0) {
            // ignore
        }

        @Override
        public void addCookie(Cookie arg0) {
            // ignore
        }

        @Override
        public void addDateHeader(String arg0, long arg1) {
            // ignore
        }

        @Override
        public void addHeader(String arg0, String arg1) {
            // ignore
        }

        @Override
        public void addIntHeader(String arg0, int arg1) {
            // ignore
        }

        @Override
        public boolean containsHeader(String arg0) {
            return false;
        }

        @Override
        public String encodeRedirectURL(String arg0) {
            return null;
        }

        @Override
        @Deprecated
        public String encodeRedirectUrl(String arg0) {
            return null;
        }

        @Override
        public String encodeURL(String arg0) {
            return null;
        }

        @Override
        @Deprecated
        public String encodeUrl(String arg0) {
            return null;
        }

        @Override
        public void sendError(int arg0) throws IOException {
            // ignore
        }

        @Override
        public void sendError(int arg0, String arg1) throws IOException {
            // ignore
        }

        @Override
        public void sendRedirect(String arg0) throws IOException {
            // ignore
        }

        @Override
        public void setDateHeader(String arg0, long arg1) {
            // ignore
        }

        @Override
        public void setHeader(String arg0, String arg1) {
            // ignore
        }

        @Override
        public void setIntHeader(String arg0, int arg1) {
            // ignore
        }

        @Override
        public void setStatus(int arg0) {
            // ignore
        }

        @Override
        @Deprecated
        public void setStatus(int arg0, String arg1) {
            // ignore
        }

        @Override
        public int getStatus() {
            return 0;
        }

        @Override
        public String getHeader(String name) {
            return null;
        }

        @Override
        public java.util.Collection<String> getHeaders(String name) {
            return null;
        }

        @Override
        public java.util.Collection<String> getHeaderNames() {
            return null;
        }

    }

    /**
     * A servlet output stream for testing.
     */
    static class TestServletOutputStream extends ServletOutputStream {

        private final ByteArrayOutputStream buff = new ByteArrayOutputStream();

        @Override
        public void write(int b) throws IOException {
            buff.write(b);
        }

        @Override
        public String toString() {
            return Utils10.byteArrayOutputStreamToString(buff, StandardCharsets.UTF_8);
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setWriteListener(WriteListener writeListener) {
            // ignore
        }

    }

}
