import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.hbase.util.Bytes;



import java.net.URL;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;

import java.io.OutputStreamWriter;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;



public class Test {

	public static void main(String[] args) throws NoSuchAlgorithmException {
		// long
		//
		long l = 1234567890L;
		byte[] lb = Bytes.toBytes(l);
		System.out.println("long bytes length: " + lb.length); // returns 8

		String s = "" + l;
		byte[] sb = Bytes.toBytes(s);
		System.out.println("long as string length: " + sb.length); // returns 10

		// hash
		//
		MessageDigest md = MessageDigest.getInstance("MD5");
		byte[] digest = md.digest(Bytes.toBytes(s));
		System.out.println("md5 digest bytes length: " + digest.length); // returns

		String sDigest = new String(digest);
		byte[] sbDigest = Bytes.toBytes(sDigest);
		System.out.println("md5 digest as string length: " + sbDigest.length); // returns
																				// 26(译者注：实测值为22)
	}

}
