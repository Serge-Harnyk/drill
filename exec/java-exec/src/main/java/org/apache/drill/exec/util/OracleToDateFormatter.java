/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to you under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.drill.exec.util;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class OracleToDateFormatter {

    private static final String[] errorMessages = new String[]{
        "Unknown Exception",
        "Unimplemented method called",
        "Underflow Exception",
        "Overflow Exception",
        "Invalid Oracle Number",
        "Bad Oracle Number format",
        "Invalid Oracle Date",
        "Bad Oracle Date format",
        "Year Not in Range",
        "Day of Year Not in Range",
        "Julian Date Not in Range",
        "Invalid Input Number",
        "NLS Not Supported",
        "Invalid Input",
        "Conversion Error"
    };

    // allowed oracle date format patterns
    private static final char ldxfda[][] = {{'A', '.', 'D', '.'}, {'A', '.', 'M', '.'}, {'A', 'D'}, {'A', 'M'}, {'B', '.', 'C', '.'}, {'B', 'C'}, {'C', 'C'}, {'D'}, {'D', 'A', 'Y'}, {'D', 'D'}, {'D', 'D', 'D'}, {'D', 'Y'}, {'E'}, {'E', 'E'}, {'F', 'M'}, {'F', 'X'}, {'H', 'H'}, {'H', 'H', '1', '2'}, {'H', 'H', '2', '4'}, {'I'}, {'I', 'W'}, {'I', 'Y'}, {'I', 'Y', 'Y'}, {'I', 'Y', 'Y', 'Y'}, {'J'}, {'M', 'I'}, {'M', 'M'}, {'M', 'O', 'N'}, {'M', 'O', 'N', 'T', 'H'}, {'P', '.', 'M', '.'}, {'P', 'M'}, {'Q'}, {'R', 'M'}, {'R', 'R'}, {'R', 'R', 'R', 'R'}, {'S', 'C', 'C'}, {'S', 'S'}, {'S', 'S', 'S', 'S', 'S'}, {'S', 'Y', ',', 'Y', 'Y', 'Y'}, {'S', 'Y', 'E', 'A', 'R'}, {'S', 'Y', 'Y', 'Y', 'Y'}, {'W'}, {'W', 'W'}, {'Y'}, {'Y', ',', 'Y', 'Y', 'Y'}, {'Y', 'E', 'A', 'R'}, {'Y', 'Y'}, {'Y', 'Y', 'Y'}, {'Y', 'Y', 'Y', 'Y'}, new char[1]};

    private static final byte ldxfdc[] = {37, 35, 36, 34, 37, 36, 1, 21, 32, 22, 23, 33, 43, 44, 39, 42, 25, 25, 24, 3, 18, 5, 7, 9, 29, 26, 17, 31, 30, 35, 34, 16, 38, 40, 41, 2, 27, 28, 13, 15, 12, 20, 19, 4, 11, 14, 6, 8, 10, 0};

    private static final byte ldxfcdlen[] = {0, 2, 35, 1, 1, 2, 2, 3, 3, 4, 4, 21, 37, 54, -60, -27, 1, 2, 2, 2, 1, 1, 2, 3, 2, 2, 2, 2, 5, 7, -128, -128, -128, -128, -62, -60, -62, -60, -124, 0, 2, 4, 0, -113, -98, -128, -128};

    private static int ldxfdi[] = {0, 4, 6, 7, 12, 14, 0x80000000, 16, 19, 24, 0x80000000, 0x80000000, 25, 0x80000000, 0x80000000, 29, 31, 32, 35, 0x80000000, 0x80000000, 0x80000000, 41, 0x80000000, 43, 0x80000000};

    // auxiliary patterns ???
    private static final char ldxfdx[][] = {{'S', 'P'}, {'S', 'P', 'T', 'H'}, {'T', 'H'}, {'T', 'H', 'S', 'P'}, new char[1]};

    private static final byte ldxfdxc[] = {2, 3, 1, 3, 0};

    private static final byte NULLFMT[] = {0, 16};

    private static final byte ldxpmxa[][] = {{23, 29}, {4, 6, 8, 10, 12, 11, 13}, {25, 24}, {34, 35}, {36, 37}, {30, 31, 17, 38}, {32, 33, 21}, {34, 35, 24}, {12, 13, 36, 37}};

    public static Date fromText(String datestr, String fmt) {
        return fromText(datestr, fmt, "UTC");
    }

    public static Date fromText(String datestr, String fmt, String timezone) {
        return ldxstd(datestr, fmt, timezone);
    }


    private static Date ldxstd(String datestr, String fmt, String timezone) {
        byte fmtBytes[];
        char jodaFormat[] = new char[512];
        int i = 0;
        int k = 0;
        ParsePosition parseposition = new ParsePosition(0);
        SimpleDateFormat simpledateformat = new SimpleDateFormat();
        fmtBytes = ldxsto(fmt);
        ldxsti(fmtBytes);
        for (int j = fmtBytes.length; i < j; ) {
            byte byte0 = fmtBytes[i++];
            byte byte1 = fmtBytes[i++];
            switch (byte0) {
                case 43: // '+'
                case 44: // ','
                    throw new IllegalArgumentException(getErrorMessage((byte) 1)); // Unimplemented method called

                case 41: // ')'
                    throw new IllegalArgumentException(getErrorMessage((byte) 1)); // Unimplemented method called

                case 40: // '('
                    throw new IllegalArgumentException(getErrorMessage((byte) 1)); // Unimplemented method called

                case 4: // '\004'
                    jodaFormat[k++] = 'y';
                    break;

                case 6: // '\006'
                    for (int i1 = 0; i1 < 2; i1++) {
                        jodaFormat[k++] = 'y';
                    }

                    break;

                case 8: // '\b'
                    for (int j1 = 0; j1 < 3; j1++) {
                        jodaFormat[k++] = 'y';
                    }

                    break;

                case 10: // '\n'
                    for (int k1 = 0; k1 < 4; k1++) {
                        jodaFormat[k++] = 'y';
                    }

                    break;

                case 11: // '\013'
                case 12: // '\f'
                case 13: // '\r'
                    throw new IllegalArgumentException(getErrorMessage((byte) 1));

                case 38: // '&'
                    throw new IllegalArgumentException(getErrorMessage((byte) 1));

                case 17: // '\021'
                    jodaFormat[k++] = 'M';
                    jodaFormat[k++] = 'M';
                    break;

                case 31: // '\037'
                    for (int l1 = 0; l1 < 3; l1++) {
                        jodaFormat[k++] = 'M';
                    }

                    break;

                case 30: // '\036'
                    for (int i2 = 0; i2 < 4; i2++) {
                        jodaFormat[k++] = 'M';
                    }

                    break;

                case 21: // '\025'
                case 33: // '!'
                    jodaFormat[k++] = 'E';
                    break;

                case 32: // ' '
                    for (int j2 = 0; j2 < 4; j2++) {
                        jodaFormat[k++] = 'E';
                    }

                    break;

                case 22: // '\026'
                    jodaFormat[k++] = 'd';
                    break;

                case 23: // '\027'
                    jodaFormat[k++] = 'D';
                    break;

                case 29: // '\035'
                    throw new IllegalArgumentException(getErrorMessage((byte) 1));

                case 25: // '\031'
                    jodaFormat[k++] = 'h';
                    break;

                case 24: // '\030'
                    jodaFormat[k++] = 'H';
                    break;

                case 26: // '\032'
                    jodaFormat[k++] = 'm';
                    break;

                case 27: // '\033'
                    jodaFormat[k++] = 's';
                    break;

                case 28: // '\034'
                    throw new IllegalArgumentException(getErrorMessage((byte) 1));

                case 34: // '"'
                case 35: // '#'
                    jodaFormat[k++] = 'a';
                    break;

                case 36: // '$'
                case 37: // '%'
                    jodaFormat[k++] = 'G';
                    break;

                case 39: // '\''
                case 42: // '*'
                    throw new IllegalArgumentException(getErrorMessage((byte) 1));

                case 5: // '\005'
                case 7: // '\007'
                case 9: // '\t'
                case 14: // '\016'
                case 15: // '\017'
                case 16: // '\020'
                case 18: // '\022'
                case 19: // '\023'
                case 20: // '\024'
                default:
                    int l = byte0 - 45;
                    String s3 = new String(fmtBytes, i, l);
                    if (byte1 == 1) {
                        jodaFormat[k++] = '\'';
                        System.arraycopy(s3.toCharArray(), 0, jodaFormat, k, l);
                        k += l;
                        i += l;
                        jodaFormat[k++] = '\'';
                    } else {
                        System.arraycopy(s3.toCharArray(), 0, jodaFormat, k, l);
                        k += l;
                        i += l;
                    }
                    break;
            }
        }

        String jodaFormatString = new String(jodaFormat, 0, k);
        simpledateformat.applyPattern(jodaFormatString);
        simpledateformat.setLenient(false);
        simpledateformat.setTimeZone(TimeZone.getTimeZone(timezone));
        Date date = simpledateformat.parse(datestr, parseposition);
        if (date != null) {
            return date;
        } else {
            throw new IllegalArgumentException(getErrorMessage((byte) 6));
        }
    }

    // Returns the intermediate byte interpretation of date format
    private static byte[] ldxsto(String fmt) {
        int i = 0; // pointer to fmt
        int j = 0;
        int l = 0;
        byte fmtBytes[] = new byte[512];
        byte byte2 = 0;
        char buff[] = new char[256]; // stores the element of the pattern
        if (fmt == null || fmt.compareTo("") == 0) {
            return NULLFMT;
        }
        for (int fmtLength = fmt.length(); i < fmtLength; ) // starting from the end of format string LOOP1
        {
            byte byte1 = 16;    // 0001 0000
            int i1 = 0;         // current pointer to element of ac to write
            do                  // while(l == 39); LOOP1.1
            {
                if (i < fmtLength && fmt.charAt(i) == '|') {
                    i++;   // skipping '|'
                    break;
                }
                int j1 = 0; // count of allocated to ac elements by single LOOP1.1 iteration
                while (i < fmtLength && !Character.isLetterOrDigit(fmt.charAt(i)))  // LOOP1.1.1
                {
                    if (fmt.charAt(i) == '"') {
                        byte2 = 1;
                        while (i != fmtLength && fmt.charAt(++i) != '"') {
                            buff[i1++] = fmt.charAt(i);
                            j1++;
                        }
                        if (fmt.charAt(i) == '"') {
                            i++;
                        }
                    } else {
                        buff[i1++] = fmt.charAt(i++);
                        j1++;
                    }
                }
                // i points to first letter or digit character of fmt or i == length
                // symb3 - symbols allocated to buff during LOOP1.1.1 completion
                // if there are any non-digit or non-letter symbols after '|' by LOOP1.1 iteration
                // save symb3 to result
                if (j1 > 0) {
                    if (j1 > 210) // too much??
                    {
                        throw new IllegalArgumentException(getErrorMessage((byte) 7)); // Bad Oracle Date format
                    }
                    if (Character.isWhitespace(buff[0])) {
                        int k4 = 0;
                        int i2 = j1;
                        int l3 = 0; // count of leading whitespaces in symb3
                        for (; i2 > 0 && Character.isWhitespace(buff[k4]); i2--) // LOOP1.1.2
                        {
                            k4++;
                            l3++;
                        }

                        fmtBytes[j++] = (byte) (45 + l3); // escape count of leading whitespaces in symb3
                        fmtBytes[j++] = 2;               // seems like delimiter
                        byte abyte3[] = (new String(buff, 0, l3)).getBytes();    // array of whitespaces ??
                        System.arraycopy(abyte3, 0, fmtBytes, j, abyte3.length); // copy whitespaces to fmtBytes
                        j += abyte3.length;                                    // complete copying
                        j1 -= l3;                                              // remove leading whitespaces from symb3 ??
                        if (j1 == 0) {
                            continue; // if there is no any whitespaces in the start of symb3 continue LOOP1.1
                        }
                        i += l3 + 1; // skip next l3 + 1 characters in LOOP1 wtf ??
                        i1 = k4;
                    } else {
                        i1 = 0;
                    }
                    // during next LOOP1.1 iteration all symbols
                    if (byte2 != 1) // byte2 == 0 at the start
                    {
                        byte2 = 4; // byte2 == 4 indicates that there was non-empty symb3 in fmt, function scope flag
                    }
                    fmtBytes[j++] = (byte) (45 + j1); // escape the length of symb3
                    fmtBytes[j++] = byte2;           // specific delimiter ???
                    byte abyte1[] = (new String(buff, 0, j1)).getBytes();
                    System.arraycopy(abyte1, 0, fmtBytes, j, abyte1.length); // save symb3 to result
                    j += abyte1.length; // skip symb3 in next iterations
                } else {
                    if (!Character.isLetterOrDigit(fmt.charAt(i))) // unreachable ???
                    {
                        throw new IllegalArgumentException(getErrorMessage((byte) 7)); // Bad Oracle Date format
                    }
                    char c = Character.toUpperCase(fmt.charAt(i));
                    int j2 = c - 65;
                    if (j2 > 25 || ldxfdi[j2] == 0x80000000) // skip invalid characters G, K, L, N, O, T, U, V, X, Z
                    {
                        throw new IllegalArgumentException(getErrorMessage((byte) 7)); // Bad Oracle Date format
                    }
                    int k1 = ldxfdi[j2];
                    j2 = 50;

                    // Searching in the patterns
                    for (; k1 < ldxfda.length; k1++) // LOOP 1.1.3
                    {
                        int i4 = ldxfda[k1].length; // ldxfda[k1] - single pattern
                        int l2 = 0;
                        for (int j3 = i; l2 < i4 && j3 < fmtLength; j3++) {
                            if (Character.toUpperCase(fmt.charAt(j3)) != ldxfda[k1][l2]) {
                                break;
                            }
                            l2++;
                        }

                        if (l2 == i4) // all characters in fmt chunk matches with a k1 pattern
                        {
                            j2 = k1; // memorize the matched pattern
                        }
                        // check if next pattern could be applicable
                        // for example ldxfda[k1] = {'D', 'D'}, ldxfda[k1+1] = {'D', 'D', 'D'}
                        if (ldxfda[k1 + 1][0] != c) {
                            break;
                        }
                    }

                    k1 = j2;
                    if (k1 >= ldxfda.length) {
                        throw new IllegalArgumentException(getErrorMessage((byte) 7)); // Bad Oracle Date format, no pattern found
                    }
                    if (fmtLength - i > 1 && Character.isUpperCase(fmt.charAt(i))) {
                        char c1 = Character.isLetterOrDigit(fmt.charAt(i + 1)) ? fmt.charAt(i + 1) : fmt.charAt(i + 2);
                        if (Character.isLowerCase(c1)) {
                            byte1 |= 0x4; // 0000 0100 - uppercase letter (LOOP1 scope flag)
                        } else {
                            byte1 |= 0x8; // 0000 1000 - lowercase letter or digit (LOOP1 scope flag)
                        }
                    }
                    i += ldxfda[k1].length; // go to next pattern
                    l = ldxfdc[k1];
                    if ((ldxfcdlen[l] & 0xffffff80) == 0) // if pattern ldxfda[k1] is compatible with joda
                    {
                        int l1 = 0;
                        int i3 = -1;
                        for (; l1 < ldxfdx.length; l1++) // loop across auxiliary patterns
                        {
                            int j4 = ldxfdx[l1].length;
                            int k2 = 0;
                            for (int k3 = i; k2 < j4 && k3 < fmtLength; k3++) // loop across auxiliary pattern
                            {
                                if (Character.toUpperCase(fmt.charAt(k3)) != ldxfdx[l1][k2]) {
                                    break;
                                }
                                k2++;
                            }

                            if (k2 == j4) {
                                i3 = l1; // memorize matched pattern
                            }
                        }

                        l1 = i3;
                        if (l1 >= 0 && l1 < ldxfdx.length) {
                            byte1 |= ldxfdxc[l1];
                            i += ldxfdx[l1].length;
                        }
                    }
                    if (512 - j < 2) // prevent buffer underflow
                    {
                        throw new IllegalArgumentException(getErrorMessage((byte) 7)); // Bad Oracle Date format
                    }
                    fmtBytes[j++] = (byte) l;
                    fmtBytes[j++] = byte1;
                }
                if (l == 39) {
                    byte1 = (byte) ((byte1 & 0x10) != 1 ? 16 : 0);
                }
            } while (l == 39);
        }

        byte abyte2[] = new byte[j];
        System.arraycopy(fmtBytes, 0, abyte2, 0, abyte2.length);
        return abyte2;
    }


    // yet another check of pattern
    private static void ldxsti(byte fmtBytes[]) {
        int ai[] = new int[46];
        for (int i = 0; i < fmtBytes.length; i += 2) {
            if (fmtBytes[i] < 45) {
                if (fmtBytes[i] != 42 && fmtBytes[i] != 39 && ai[fmtBytes[i]] != 0) {
                    throw new IllegalArgumentException(getErrorMessage((byte) 7));  // Bad Oracle Date format
                }
                ai[fmtBytes[i]]++;
                switch (fmtBytes[i]) {
                    case 1: // '\001'
                    case 2: // '\002'
                    case 3: // '\003'
                    case 5: // '\005'
                    case 7: // '\007'
                    case 9: // '\t'
                    case 14: // '\016'
                    case 15: // '\017'
                    case 16: // '\020'
                    case 18: // '\022'
                    case 19: // '\023'
                    case 20: // '\024'
                        throw new IllegalArgumentException(getErrorMessage((byte) 7));  // Bad Oracle Date format
                }
            } else {
                i += fmtBytes[i] - 45;
            }
        }

        for (int j = 0; j < ldxpmxa.length; j++) {
            int k = 0;
            for (int l = 0; l < ldxpmxa[j].length; l++) {
                k += ai[ldxpmxa[j][l]];
            }

            if (k > 1) {
                throw new IllegalArgumentException(getErrorMessage((byte) 7));  // Bad Oracle Date format
            }
        }
    }

    private static String getErrorMessage(byte errorCode) {
        return errorCode >= 1 && errorCode <= 14 ? errorMessages[errorCode] : "Unknown exception";
    }
}