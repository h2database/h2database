/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.h2.engine.ConnectionInfo;
import org.h2.engine.Constants;
import org.h2.jdbc.JdbcConnection;
import org.h2.util.IOUtils;
import org.h2.util.StringUtils;

/**
 * Upgrade utility.
 */
public final class Upgrade {

    private static final String[] CHECKSUMS = {
            /* 1.2.120 */ "6fca37906aa3916ba609f47258c4abb4c749cd51aa28718a2339d9aa234a480c",
            /* 1.2.121 */ "3233d38ee11e15243f66c98ad388da9f12cf038a203cf507415081e3329ac4f4",
            /* 1.2.122 */ "7451e9f234f32fd9f07e4e5e682c0595806a803de656228a43887a525019ea74",
            /* 1.2.123 */ "5a4dfaf211d32860623fdc5627f12a9cf8446b9cfabc742e7c0bad26835a8bb1",
            /* 1.2.124 */ "f75efcaf9ccb91d94de920322c32328435e9705c19cc06b510c5f09c0a6245bf",
            /* 1.2.125 */ "0ca368055dd72d539084c916642147780c944b90d98d2306da86814b174d1145",
            /* 1.2.126 */ "4d9143f5b80f8878ca56edc383ae6d0a183a3b5879e83228dbacbe288007455c",
            /* 1.2.127 */ "3df7aedd564cf61a464f4e95ec364eb7bb2b51d36863ed54edeb6ff2fed7b376",
            /* 1.2.128 */ "7e8af7b5eca6334013fc024dab02e173a017b2d1c22c8481ed64a6af873d0819",
            /* 1.2.129 */ "9a705009830ae80a368b1b66c8ba63071845fe25d8f6b0964aa14a3f31b46bdd",
            /* 1.2.130 */ "8810d72867508b033a68830024e7fe7dd5a99e6f5bbb38c5a933aeb23badff00",
            /* 1.2.131 */ "c8debc05829db1db2e6b6507a3f0561e1f72bd966d36f322bdf294baca29ed22",
            /* 1.2.132 */ "75819d4adbf76d66af904e76b52b57afe26e9bc0e15aceed4e3c72cd7586b0d3",
            /* 1.2.133 */ "c9ea3e95e77ae560322bca37d51601ae4b1d07ae90988af1e9fe1ceda80cd9ce",
            /* 1.2.134 */ "1f4753d8d862d7d22d234625f617d3d7e91b73799c89b8a6036895f944a863eb",
            /* 1.2.135 */ "eed53fcd3cf6e1159c90e57ef2b4cbd1fa3aff7a936988bb018af6fc17a2b6d9",
            /* 1.2.136 */ "d3101d540ed004493952732d28bdf90a7968990bab7a2e04d16805469aa4eedd",
            /* 1.2.137 */ "035dd78af874ada48339b90e8e4f1ffba0f32bb0fa37dec37ed523afa96a9c32",
            /* 1.2.138 */ "1d03156b22b40812e39cca4d250eededfed4db8476bfbae78d60a24975cbe6d8",
            /* 1.2.139 */ "8102cc96257d71caeff04f02c97020ae39268a32c1f0aa8fcdfda4e948ce48c8",
            /* 1.2.140 */ "134ceafcae6ca661d8acd64c8e67d30f6ead609065dba9f6d3a0cde0d7bef6e3",
            /* 1.2.141 */ "e453faccaaf7d8fe4eb8be744549c4a2395c7b3dcfcbc19173588c3756baff1e",
            /* 1.2.142 */ "5973b4b467f1e0a69cf8c7b02d03d9dcadb4171d8a9635c85442a5829200e76f",
            /* 1.2.143 */ "711cc225d8fe5325458c3947dda2093ef3a1cd4923e916082b27e87e41ca6735",
            /* 1.2.144 */ "682f6997495a8389f4881b93cb8224685b9c6cbed487bcb445712402e52a4b80",
            /* 1.2.145 */ "1407913cc6ba2f8c2928e8ad544c232273365d6eb66fdf84ec4213abf71449d5",
            /* 1.3.146 */ "7756a89f10d5d5df23936bbb613de8b481e32d1099e5228968046fee62fee882",
            /* 1.2.147 */ "2649d19db9eebbddc826029d236886dfece9404cd108ca590e82d3fd7d888278",
            /* 1.3.148 */ "66f9389748f176c11c66c201a3737ebad0b1f4ace37cc2cd3da8962c92c72128",
            /* 1.3.149 */ "7c3e3b93ffaf617393126870be7f8e1708bbe8e05b931c51c638a8cb03f79a36",
            /* 1.3.150 */ "1d6dc1095d3d4b105a99034ab61ab5943c4dbb31551e7b244b403cb3c324964f",
            /* 1.3.151 */ "8eabfde7cf64cedb7c25dc25ee7fe75a633c5cbeb18a1060da2045293fd53b14",
            /* 1.3.152 */ "a9840c6024f8570ad3aa4d54388b4dd605640cb5ab163c444a123f7d4739aa09",
            /* 1.3.153 */ "33d80491417eb117a0d64442dc3e60b78cf014ad099bb36a55d3835bb69e6248",
            /* 1.3.154 */ "f153d03466acc00b66e699213fe092277e457502b5caf48c417ed3745f50eaac",
            /* 1.3.155 */ "244b29d22939b43ecdcd3b0bfd279899df18e3af20a50241278b5b27bcf1a902",
            /* 1.3.156 */ "070f9e4898044880e01232b269fea5285dbf7b814b7092701e755aa7d6941832",
            /* 1.3.157 */ "4666d8f01c661054b973bc0f01f8b20f298d8e134e6fd26d78c74d43eeffd54e",
            /* 1.3.158 */ "b0d95f18474beea619fcfba83f033e5702483457e0f0a1d1ffb4b757c5182582",
            /* 1.3.159 */ "17aa5ced25f13f9adc2820e0ccc3010e3ce55944d10c9e2c0c631b77674d039b",
            /* 1.3.160 */ "7fe66e211202733c52f02a328b55b30975287d9c509751bf87507e6227c6a2a7",
            /* 1.3.161 */ "42e2ebbb7bdf29dd2de4ab16fc8fb511af6337d223afd66a5ee5fe183de05d57",
            /* 1.3.162 */ "89e362f9525adf36d58487ff756ee93254bf92595a7098258a4c030e08e0742e",
            /* 1.3.163 */ "1d1be843af365e8881e22732c8640e2b04c2821a0d7aa61d4152ac3f991bb735",
            /* 1.3.164 */ "dbc88bb8cd8177b5f13b655d6afb525637129369422f0b7be0fe187950ea5132",
            /* 1.3.165 */ "03f60ca37c0124fd2b9b177726396a51853ed0cade444e1674a090b73d341b08",
            /* 1.3.166 */ "35103656071f1ffd1078b1a8c8028c9577297f31c5f8c7dcc845c7b4b6392619",
            /* 1.3.167 */ "fa97521a2e72174485a96276bcf6f573d5e44ca6aba2f62de87b33b5bb0d4b91",
            /* 1.3.168 */ "46d7ff55ccd910def16f9afd21d983f2eb2f9a6850fb501916f6673caebc2694",
            /* 1.3.169 */ "0d99d51b8d7b8e94732d048438b9f555e031ecd52225613d7bea45290571886d",
            /* 1.3.170 */ "0aca5eea86e8619e91ad61b82b77fb9d0e51e939c5603ab8da41be32c6f25664",
            /* 1.3.171 */ "144d4ddb5d9f610b8b26809f1c65f442864cc55136325d3f02d7a93fb878a1db",
            /* 1.3.172 */ "6ca30e38ccaa0c6f4264ef013327ef9ba5303f4be3d8fdbce0c3ae6451178c1e",
            /* 1.3.173 */ "43908ee9db698cb335e2b85375d68a9d03d818869a0542b85d8d4e416619795b",
            /* 1.3.174 */ "990b94cdfc89987281af4168fc2f6c9067be96a8533f5a6eb0f33da4d30d3e4b",
            /* 1.3.175 */ "cc329a8742fb6e7168b00ebd0015816ff0d2462409add7c9d223826486de4691",
            /* 1.3.176 */ "6ae3cc11a8bbaa5bd1d8494e62bccea4d354eaf042da468eac3bc5009fd33b67",
            /* 1.4.177 */ "f281673f3248a4b5cb03fdc0cc39b944fe978366be959d0e8106fcc3197f4705",
            /* 1.4.178 */ "da08fef0b2bc0ff8876f895e17605daf514405a064e3c2c11d2275a19d301be6",
            /* 1.4.179 */ "2b76304ce4256ee9fd61156f9b6ef82c049ffdc8dc89af07fcf59e9532c7e7cd",
            /* 1.4.180 */ "16428fd1e6a3e5baa8067c1c2e777e1e99af68c6ef3ff7fbbf1938937a048a82",
            /* 1.4.181 */ "44673ff2834428fdb7f11dac3b9d679fb3039ea32194a69452971fdd7150a08b",
            /* 1.4.182 */ "1025d0d70a4e899c41bc8fd7370cd3768826e78da91b66fd9357e44d03d79d30",
            /* 1.4.183 */ "b3ff2ebe161976124965a9a841877ec4f6e913dbadcc31af27f1b99f6abd57e9",
            /* 1.4.184 */ "9e47e14d5b4b9ead127b15a33b107ff06f0a7dd3f98b5d6c149e6ccae05dc0a2",
            /* 1.4.185 */ "c4ac74be5971445e270bbd4344be58d9a06dc927223614217e5a87257a7edc03",
            /* 1.4.186 */ "e3b7a39a2b45b61fa1521ef33b3ba676a5a9e1a397bc3ef4fb678d861a1b0ae4",
            /* 1.4.187 */ "6204d0c206443681911fb9e04a3af5198b253b5627d16d8d8d79180d13319212",
            /* 1.4.188 */ "11d6bff477f7ca392288f5f6d42ee61d0ccb63a34c99ba2d91710b2409673897",
            /* 1.4.189 */ "c8dac03b66c8011cca4e44dcc7a8b1c8f8df769927c7672be1704e76f9ee7926",
            /* 1.4.190 */ "23ba495a07bbbb3bd6c3084d10a96dad7a23741b8b6d64b213459a784195a98c",
            /* 1.4.191 */ "e21ea665b74ec0115344b5afda5ec70ea27b528c3f103524e74c9854b1c4a284",
            /* 1.4.192 */ "225b22e9857235c46c93861410b60b8c81c10dc8985f4faf188985ba5445126c",
            /* 1.4.193 */ "b1cf34c64871014aa73580281cc464dfa72450d8860cc0752fc175e87edd6544",
            /* 1.4.194 */ "b5b0c1836cead6831a50bd3e1b6c16fe6e583d4d2b7c4f41b4f838745c27cd01",
            /* 1.4.195 */ "b99ea1f785c62b2a021664e72de696f8ea896f0da392a1c7baa3d4d47020b126",
            /* 1.4.196 */ "0a05f4a0d5b85840148aadce63a423b5d3c36ef44756389b4faad08d2733faf5",
            /* 1.4.197 */ "37f5216e14af2772930dff9b8734353f0a80e89ba3f33e065441de6537c5e842",
            /* 1.4.198 */ "32dd6b149cb722aa4c2dd4d40a74a9cd41e32ac59a4e755a66e5753660d61d46",
            /* 1.4.199 */ "3125a16743bc6b4cfbb61abba783203f1fb68230aa0fdc97898f796f99a5d42e",
            /* 1.4.200 */ "3ad9ac4b6aae9cd9d3ac1c447465e1ed06019b851b893dd6a8d76ddb6d85bca6",
            /* 2.0.202 */ "95090f0609aacb0ee339128ef04077145ef28320ee874ea2e33a692938da5b97",
            /* 2.0.204 */ "712a616409580bd4ac7c10e48f2599cc32ba3a433a1804da619c3f0a5ef66a04",
            /* 2.0.206 */ "3b9607c5673fd8b87e49e3ac46bd88fd3561e863dce673a35234e8b5708f3deb",
            /* 2.0.208 */ null,
            /* 2.1.210 */ "edc57299926297fd9315e04de75f8538c4cb5fe97fd3da2a1e5cee6a4c98b5cd",
            /* 2.1.212 */ "db9284c6ff9bf3bc0087851edbd34563f1180df3ae87c67c5fe2203c0e67a536",
            /* 2.1.214 */ "d623cdc0f61d218cf549a8d09f1c391ff91096116b22e2475475fce4fbe72bd0",
            /* 2.1.216 */ null,
            /* 2.1.218 */ null,
            //
    };

    private static final String REPOSITORY = "https://repo1.maven.org/maven2";

    /**
     * Performs database upgrade from an older version of H2.
     *
     * @param url
     *            the JDBC connection URL
     * @param info
     *            the connection properties ("user", "password", etc).
     * @param version
     *            the old version of H2
     * @return {@code true} on success, {@code false} if URL is a remote or
     *         in-memory URL
     * @throws Exception
     *             on failure
     */
    public static boolean upgrade(String url, Properties info, int version) throws Exception {
        Properties oldInfo = new Properties();
        oldInfo.putAll(info);
        Object password = info.get("password");
        if (password instanceof char[]) {
            oldInfo.put("password", ((char[]) password).clone());
        }
        ConnectionInfo ci = new ConnectionInfo(url, info, null, null);
        if (!ci.isPersistent() || ci.isRemote()) {
            return false;
        }
        String name = ci.getName();
        String script = name + ".script.sql";
        StringBuilder oldUrl = new StringBuilder("jdbc:h2:").append(name).append(";ACCESS_MODE_DATA=r");
        copyProperty(ci, oldUrl, "FILE_LOCK");
        copyProperty(ci, oldUrl, "MV_STORE");
        String cipher = copyProperty(ci, oldUrl, "CIPHER");
        String scriptCommandSuffix = cipher == null ? "" : " CIPHER AES PASSWORD '" + UUID.randomUUID() + "' --hide--";
        java.sql.Driver driver = loadH2(version);
        try (Connection conn = driver.connect(oldUrl.toString(), oldInfo)) {
            conn.createStatement().execute(StringUtils.quoteStringSQL(new StringBuilder("SCRIPT TO "), script)
                    .append(scriptCommandSuffix).toString());
        } finally {
            unloadH2(driver);
        }
        rename(name, false);
        try (JdbcConnection conn = new JdbcConnection(url, info, null, null, false)) {
            StringBuilder builder = StringUtils.quoteStringSQL(new StringBuilder("RUNSCRIPT FROM "), script)
                    .append(scriptCommandSuffix);
            if (version <= 200) {
                builder.append(" FROM_1X");
            }
            conn.createStatement().execute(builder.toString());
        } catch (Throwable t) {
            rename(name, true);
            throw t;
        } finally {
            Files.deleteIfExists(Paths.get(script));
        }
        return true;
    }

    private static void rename(String name, boolean back) throws IOException {
        rename(name, Constants.SUFFIX_MV_FILE, back);
        rename(name, ".lobs.db", back);
    }

    private static void rename(String name, String suffix, boolean back) throws IOException {
        String source = name + suffix;
        String target = source + ".bak";
        if (back) {
            String t = source;
            source = target;
            target = t;
        }
        Path p = Paths.get(source);
        if (Files.exists(p)) {
            Files.move(p, Paths.get(target), StandardCopyOption.ATOMIC_MOVE);
        }
    }

    private static String copyProperty(ConnectionInfo ci, StringBuilder oldUrl, String name) {
        try {
            String value = ci.getProperty(name, null);
            if (value != null) {
                oldUrl.append(';').append(name).append('=').append(value);
            }
            return value;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Loads the specified version of H2 in a separate class loader.
     *
     * @param version
     *            the version to load
     * @return the driver of the specified version
     * @throws IOException
     *             on I/O exception
     * @throws ReflectiveOperationException
     *             on exception during initialization of the driver
     */
    public static java.sql.Driver loadH2(int version) throws IOException, ReflectiveOperationException {
        String prefix;
        if (version >= 201) {
            if ((version & 1) != 0 || version > Constants.BUILD_ID) {
                throw new IllegalArgumentException("version=" + version);
            }
            int major = version / 100;
            int minor = version / 10 % 10;
            prefix = new StringBuilder().append(major).append('.').append(minor).append('.').toString();
        } else if (version >= 177) {
            prefix = "1.4.";
        } else if (version >= 146 && version != 147) {
            prefix = "1.3.";
        } else if (version >= 120) {
            prefix = "1.2.";
        } else {
            throw new IllegalArgumentException("version=" + version);
        }
        String fullVersion = prefix + version;
        byte[] data = downloadUsingMaven("com.h2database", "h2", fullVersion,
                CHECKSUMS[version >= 202 ? (version >>> 1) - 20 : version - 120]);
        ZipInputStream is = new ZipInputStream(new ByteArrayInputStream(data));
        HashMap<String, byte[]> map = new HashMap<>(version >= 198 ? 2048 : 1024);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (ZipEntry ze; (ze = is.getNextEntry()) != null;) {
            if (ze.isDirectory()) {
                continue;
            }
            IOUtils.copy(is, baos);
            map.put(ze.getName(), baos.toByteArray());
            baos.reset();
        }
        ClassLoader cl = new ClassLoader(null) {
            @Override
            protected Class<?> findClass(String name) throws ClassNotFoundException {
                String resourceName = name.replace('.', '/') + ".class";
                byte[] b = map.get(resourceName);
                if (b == null) {
                    return ClassLoader.getSystemClassLoader().loadClass(name);
                }
                return defineClass(name, b, 0, b.length);
            }

            @Override
            public InputStream getResourceAsStream(String name) {
                byte[] b = map.get(name);
                return b != null ? new ByteArrayInputStream(b) : null;
            }
        };
        return (java.sql.Driver) cl.loadClass("org.h2.Driver").getDeclaredMethod("load").invoke(null);
    }

    /**
     * Unloads the specified driver of H2.
     *
     * @param driver
     *            the driver to unload
     * @throws ReflectiveOperationException
     *             on exception
     */
    public static void unloadH2(java.sql.Driver driver) throws ReflectiveOperationException {
        driver.getClass().getDeclaredMethod("unload").invoke(null);
    }

    private static byte[] downloadUsingMaven(String group, String artifact, String version, String sha256Checksum)
            throws IOException {
        String repoFile = group.replace('.', '/') + '/' + artifact + '/' + version + '/' + artifact + '-' + version
                + ".jar";
        Path localMavenDir = Paths.get(System.getProperty("user.home") + "/.m2/repository");
        if (Files.isDirectory(localMavenDir)) {
            Path f = localMavenDir.resolve(repoFile);
            if (!Files.exists(f)) {
                try {
                    ArrayList<String> args = new ArrayList<>();
                    if (System.getProperty("os.name").toLowerCase().contains("windows")) {
                        args.add("cmd");
                        args.add("/C");
                    }
                    args.add("mvn");
                    args.add("org.apache.maven.plugins:maven-dependency-plugin:2.1:get");
                    args.add("-D" + "repoUrl=" + REPOSITORY);
                    args.add("-D" + "artifact=" + group + ':' + artifact + ':' + version);
                    exec(args);
                } catch (RuntimeException e) {
                    System.out.println("Could not download using Maven: " + e.toString());
                }
            }
            if (Files.exists(f)) {
                return check(Files.readAllBytes(f), sha256Checksum, f.toAbsolutePath().toString());
            }
        }
        return download(REPOSITORY + '/' + repoFile, sha256Checksum);
    }

    private static int exec(ArrayList<String> args) {
        try {
            ProcessBuilder pb = new ProcessBuilder();
            pb.command(args.toArray(new String[0]));
            pb.inheritIO();
            Process p = pb.start();
            p.waitFor();
            return p.exitValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] download(String fileURL, String sha256Checksum) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            System.out.println("Downloading " + fileURL);
            URL url = new URL(fileURL);
            InputStream in = new BufferedInputStream(url.openStream());
            long last = System.nanoTime();
            int len = 0;
            while (true) {
                long now = System.nanoTime();
                if (now - last > 1_000_000_000L) {
                    System.out.println("Downloaded " + len + " bytes");
                    last = now;
                }
                int x = in.read();
                len++;
                if (x < 0) {
                    break;
                }
                baos.write(x);
            }
            in.close();
        } catch (IOException e) {
            throw new RuntimeException("Error downloading " + fileURL, e);
        }
        return check(baos.toByteArray(), sha256Checksum, null);
    }

    private static byte[] check(byte[] data, String sha256Checksum, String checksummedFile) {
        String got = getSHA256(data);
        if (sha256Checksum == null) {
            System.out.println('"' + got + '"');
        } else {
            if (!got.equals(sha256Checksum)) {
                StringBuilder builder = new StringBuilder().append("SHA-256 checksum mismatch; got: ").append(got)
                        .append(" expected: ").append(sha256Checksum);
                if (checksummedFile != null) {
                    builder.append(" for file ").append(checksummedFile);
                }
                throw new RuntimeException(builder.toString());
            }
        }
        return data;
    }

    private static String getSHA256(byte[] data) {
        try {
            return StringUtils.convertBytesToHex(MessageDigest.getInstance("SHA-256").digest(data));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private Upgrade() {
    }

}
