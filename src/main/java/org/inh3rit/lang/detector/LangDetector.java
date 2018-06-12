package org.inh3rit.lang.detector;

import com.google.common.base.Optional;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class LangDetector implements Serializable {

    private static LanguageDetector languageDetector;
    private static TextObjectFactory textObjectFactory;

    static {
        //load all languages:
        List<LanguageProfile> languageProfiles = null;
        try {
            languageProfiles = new LanguageProfileReader().readAllBuiltIn();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //build language detector:
        languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
                .withProfiles(languageProfiles)
                .build();

        //create a text object factory
        textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();
    }

    public static String detect(String text) {
        //query:
        TextObject textObject = textObjectFactory.forText(text);
        Optional<LdLocale> lang = languageDetector.detect(textObject);
        return lang.get().getLanguage();
    }
}
