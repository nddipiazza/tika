/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tika.parser.microsoft.onenote;

import org.apache.tika.TikaTest;
import org.apache.tika.metadata.Metadata;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

public class OneNoteParserTest extends TikaTest {

    //TODO: rename test files testOneNote...
    //test recursive parser wrapper for image files

    /**
     * This is the sample document that is automatically created from onenote 2013.
     */
    @Test
    public void testOneNote2013Doc1() throws Exception {
//        List<Metadata> metadataList = getRecursiveMetadata("Sample1.one");
  //      debug(metadataList);
        Metadata metadata = new Metadata();
        String txt = getText("Sample1.one", metadata);
        assertNoJunk(txt);

        List<String> authors = Arrays.asList(metadata.getValues("authors"));
        assertContains("Olya Veselova\u0000", authors);
        assertContains("Microsoft\u0000", authors);
        assertContains("Scott\u0000", authors);
        assertContains("Scott H. W. Snyder\u0000", authors);

        List<String> mostRecentAuthors = Arrays.asList(metadata.getValues("mostRecentAuthors"));
        assertContains("Microsoft\u0000", mostRecentAuthors);

        List<String> originalAuthors = Arrays.asList(metadata.getValues("originalAuthors"));
        assertContains("Microsoft\u0000", originalAuthors);

        Assert.assertEquals("2011-04-15T05:33:02.069648100Z", metadata.get("lastModifiedTimestamp"));
        Assert.assertEquals(Instant.ofEpochSecond(1446572147), Instant.ofEpochSecond(Long.parseLong(metadata.get("lastModified"))));
    }

    @Test
    public void testOneNote2013Doc2() throws Exception {
        Metadata metadata = new Metadata();
        String txt = getText("Section1SheetTitle.one", metadata);
        assertContains("wow this is neat", txt);
        assertContains("neat info about totally killin it bro", txt);
        assertContains("Section1TextArea1", txt);
        assertContains("Section1HeaderTitle", txt);
        assertContains("Section1TextArea2", txt);
        assertNoJunk(txt);

        List<String> authors = Arrays.asList(metadata.getValues("authors"));
        assertContains("Olya Veselova\u0000", authors);
        assertContains("Microsoft\u0000", authors);
        assertContains("Scott\u0000", authors);
        assertContains("Scott H. W. Snyder\u0000", authors);
        assertContains("ndipiazza\u0000", authors);

        List<String> mostRecentAuthors = Arrays.asList(metadata.getValues("mostRecentAuthors"));
        assertContains("ndipiazza\u0000", mostRecentAuthors);
        assertContains("Microsoft\u0000", mostRecentAuthors);

        List<String> originalAuthors = Arrays.asList(metadata.getValues("originalAuthors"));
        assertContains("Microsoft\u0000", originalAuthors);
        assertContains("ndipiazza\u0000", mostRecentAuthors);

        Assert.assertEquals("2011-04-15T05:33:02.069648100Z", metadata.get("lastModifiedTimestamp"));
        Assert.assertEquals(Instant.ofEpochSecond(1574426628), Instant.ofEpochSecond(Long.parseLong(metadata.get("lastModified"))));
    }

    @Test
    public void testOneNote2013Doc3() throws Exception {
        Metadata metadata = new Metadata();
        String txt = getText("Section2SheetTitle.one", metadata);
        assertContains("awesome information about sports or some crap like that.", txt);
        assertContains("Quit doing horrible things to me. Dang you. ", txt);
        assertContains("Section2TextArea1", txt);
        assertContains("Section2HeaderTitle", txt);
        assertContains("Section2TextArea2", txt);
        assertNoJunk(txt);

        List<String> authors = Arrays.asList(metadata.getValues("authors"));
        assertNotContained("Olya Veselova\u0000", authors);
        assertNotContained("Microsoft\u0000", authors);
        assertNotContained("Scott\u0000", authors);
        assertNotContained("Scott H. W. Snyder\u0000", authors);
        assertContains("ndipiazza\u0000", authors);

        List<String> mostRecentAuthors = Arrays.asList(metadata.getValues("mostRecentAuthors"));
        assertContains("ndipiazza\u0000", mostRecentAuthors);
        assertNotContained("Microsoft\u0000", mostRecentAuthors);

        List<String> originalAuthors = Arrays.asList(metadata.getValues("originalAuthors"));
        assertNotContained("Microsoft\u0000", originalAuthors);
        assertContains("ndipiazza\u0000", mostRecentAuthors);

        Assert.assertEquals("2011-04-15T05:33:02.069648100Z", metadata.get("lastModifiedTimestamp"));
        Assert.assertEquals(Instant.ofEpochSecond(1574426624), Instant.ofEpochSecond(Long.parseLong(metadata.get("lastModified"))));
    }

    @Test
    public void testOneNote2013Doc4() throws Exception {
        Metadata metadata = new Metadata();
        String txt = getText("Section3SheetTitle.one", metadata);

        assertContains("way too much information about poptarts to handle.", txt);
        assertContains("Section3TextArea1", txt);
        assertContains("Section3HeaderTitle", txt);
        assertContains("Section3TextArea2", txt);
        assertNoJunk(txt);

        List<String> authors = Arrays.asList(metadata.getValues("authors"));
        assertNotContained("Olya Veselova\u0000", authors);
        assertNotContained("Microsoft\u0000", authors);
        assertNotContained("Scott\u0000", authors);
        assertNotContained("Scott H. W. Snyder\u0000", authors);
        assertContains("ndipiazza\u0000", authors);

        List<String> mostRecentAuthors = Arrays.asList(metadata.getValues("mostRecentAuthors"));
        assertContains("ndipiazza\u0000", mostRecentAuthors);
        assertNotContained("Microsoft\u0000", mostRecentAuthors);

        List<String> originalAuthors = Arrays.asList(metadata.getValues("originalAuthors"));
        assertNotContained("Microsoft\u0000", originalAuthors);
        assertContains("ndipiazza\u0000", mostRecentAuthors);

        Assert.assertEquals("2011-04-15T05:33:02.069648100Z", metadata.get("lastModifiedTimestamp"));
        Assert.assertEquals(Instant.ofEpochSecond(1574426547), Instant.ofEpochSecond(Long.parseLong(metadata.get("lastModified"))));
    }

    private void assertNoJunk(String txt) {
        //Should not include font names in the text
        assertNotContained("Calibri", txt);
        //Should not include UTF-16 property values that are garbage
        assertNotContained("\u5902", txt);
        assertNotContained("\u83F2", txt);
        assertNotContained("\u432F", txt);
        assertNotContained("\u01E1", txt);

    }
}
