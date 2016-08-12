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
package org.apache.drill.common.expression.fn;


import java.util.HashMap;
import java.util.Map;

public class ToDateTypeFunctions {
    private static Map<String, String> TO_DATE_TYPE_FUNC_REPLACEMENT = new HashMap<>();
    static {
        TO_DATE_TYPE_FUNC_REPLACEMENT.put("to_date", "to_date_oracle");
        TO_DATE_TYPE_FUNC_REPLACEMENT.put("to_timestamp", "to_timestamp_oracle");
        TO_DATE_TYPE_FUNC_REPLACEMENT.put("to_time", "to_time_oracle");
    }

    public static boolean isReplacementNeeded(String originalFunction, String toDateFormat) {
        return ToDateFormats.valueOf(toDateFormat.toUpperCase()) == ToDateFormats.ORACLE
            && TO_DATE_TYPE_FUNC_REPLACEMENT.containsKey(originalFunction);
    }

    public static String getReplacingToDateTypeFunction(String originalFunction) {
        return TO_DATE_TYPE_FUNC_REPLACEMENT.get(originalFunction);
    }


    public enum ToDateFormats {
        JODA,
        ORACLE
    }
}
