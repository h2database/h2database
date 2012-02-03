/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import org.h2.security.BlockCipher;
import org.h2.security.CipherFactory;
import org.h2.security.SHA256;
import org.h2.test.TestBase;
import org.h2.util.Utils;

/**
 * Tests various security primitives.
 */
public class TestSecurity extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() {
        testSHA();
        testAES();
        testXTEA();
    }

    private void testSHA() {
        SHA256 sha = new SHA256();
        testOneSHA(sha);
    }

    private String getHashString(SHA256 sha, byte[] data) {
        byte[] result = sha.getHash(data, true);
        if (data.length > 0) {
            assertEquals(0, data[0]);
        }
        return Utils.convertBytesToString(result);
    }

    private void testOneSHA(SHA256 sha) {
        assertEquals("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                getHashString(sha, new byte[] {}));
        assertEquals("68aa2e2ee5dff96e3355e6c7ee373e3d6a4e17f75f9518d843709c0c9bc3e3d4",
                getHashString(sha, new byte[] { 0x19 }));
        assertEquals("175ee69b02ba9b58e2b0a5fd13819cea573f3940a94f825128cf4209beabb4e8",
                getHashString(
                        sha,
                        new byte[] { (byte) 0xe3, (byte) 0xd7, 0x25, 0x70, (byte) 0xdc, (byte) 0xdd, 0x78, 0x7c, (byte) 0xe3,
                                (byte) 0x88, 0x7a, (byte) 0xb2, (byte) 0xcd, 0x68, 0x46, 0x52 }));
        checkSHA256("", "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855");
        checkSHA256("a", "CA978112CA1BBDCAFAC231B39A23DC4DA786EFF8147C4E72B9807785AFEE48BB");
        checkSHA256("abc", "BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD");
        checkSHA256("message digest", "F7846F55CF23E14EEBEAB5B4E1550CAD5B509E3348FBC4EFA3A1413D393CB650");
        checkSHA256("abcdefghijklmnopqrstuvwxyz", "71C480DF93D6AE2F1EFAD1447C66C9525E316218CF51FC8D9ED832F2DAF18B73");
        checkSHA256("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq", "248D6A61D20638B8E5C026930C3E6039A33CE45964FF2167F6ECEDD419DB06C1");
        checkSHA256("12345678901234567890123456789012345678901234567890123456789012345678901234567890", "F371BC4A311F2B009EEF952DD83CA80E2B60026C8E935592D0F9C308453C813E");
        StringBuilder buff = new StringBuilder(1000000);
        buff.append('a');
        checkSHA256(buff.toString(), "CA978112CA1BBDCAFAC231B39A23DC4DA786EFF8147C4E72B9807785AFEE48BB");
    }

    private void checkSHA256(String message, String expected) {
        SHA256 sha = new SHA256();
        String hash = Utils.convertBytesToString(sha.getHash(message.getBytes(), true)).toUpperCase();
        assertEquals(expected, hash);
    }

    private void testXTEA() {
        byte[] test = new byte[4096];
        BlockCipher xtea = CipherFactory.getBlockCipher("XTEA");
        xtea.setKey("abcdefghijklmnop".getBytes());
        for (int i = 0; i < 10; i++) {
            xtea.decrypt(test, 0, test.length);
        }
    }

    private void testAES() {
        BlockCipher test = CipherFactory.getBlockCipher("AES");
        test.setKey(Utils.convertStringToBytes("000102030405060708090A0B0C0D0E0F"));

        byte[] in = new byte[128];
        byte[] enc = new byte[128];
        test.encrypt(enc, 0, 128);
        test.decrypt(enc, 0, 128);
        if (Utils.compareNotNull(in, enc) != 0) {
            throw new AssertionError();
        }

        for (int i = 0; i < 10; i++) {
            test.encrypt(in, 0, 128);
            test.decrypt(enc, 0, 128);
        }
    }

}
