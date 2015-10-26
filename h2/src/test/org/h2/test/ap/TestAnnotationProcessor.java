package org.h2.test.ap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;

public class TestAnnotationProcessor extends AbstractProcessor {

    public static final String MESSAGES_KEY = TestAnnotationProcessor.class.getName() + "-messages";

    public Set<String> getSupportedAnnotationTypes() {

        for (OutputMessage outputMessage : findMessages()) {
            processingEnv.getMessager().printMessage(outputMessage.kind, outputMessage.message);
        }

        return Collections.emptySet();
    }

    private List<OutputMessage> findMessages() {
        final String messagesStr = System.getProperty(MESSAGES_KEY);
        if (messagesStr == null || messagesStr.isEmpty()) {
            return Collections.emptyList();
        } else {
            final List<OutputMessage> outputMessages = new ArrayList<OutputMessage>();

            for (String msg : messagesStr.split("\\|")) {
                final String[] split = msg.split(",");
                if (split.length == 2) {
                    outputMessages.add(new OutputMessage(Diagnostic.Kind.valueOf(split[0]), split[1]));
                } else {
                    throw new IllegalStateException("Unable to parse messages definition for: '" + messagesStr + "'");
                }
            }

            return outputMessages;
        }
    }

    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.RELEASE_6;
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        return false;
    }

    private static class OutputMessage {
        public final Diagnostic.Kind kind;
        public final String message;

        private OutputMessage(Diagnostic.Kind kind, String message) {
            this.kind = kind;
            this.message = message;
        }
    }
}
