package com.jackniu.kafka.common.security.scram.internals;

import com.jackniu.kafka.common.KafkaException;
import com.jackniu.kafka.common.security.scram.ScramCredential;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ScramFormatter  {
    private static final Pattern EQUAL = Pattern.compile("=", Pattern.LITERAL);
    private static final Pattern COMMA = Pattern.compile(",", Pattern.LITERAL);
    private static final Pattern EQUAL_TWO_C = Pattern.compile("=2C", Pattern.LITERAL);
    private static final Pattern EQUAL_THREE_D = Pattern.compile("=3D", Pattern.LITERAL);

    private final MessageDigest messageDigest;
    private final Mac mac;
    private final SecureRandom random;

    public ScramFormatter(ScramMechanism mechanism) throws NoSuchAlgorithmException {
        this.messageDigest = MessageDigest.getInstance(mechanism.hashAlgorithm());
        this.mac = Mac.getInstance(mechanism.macAlgorithm());
        this.random = new SecureRandom();
    }

    public byte[] hmac(byte[] key, byte[] bytes) throws InvalidKeyException {
        mac.init(new SecretKeySpec(key, mac.getAlgorithm()));
        return mac.doFinal(bytes);
    }

    public byte[] hash(byte[] str) {
        return messageDigest.digest(str);
    }

    public byte[] xor(byte[] first, byte[] second) {
        if (first.length != second.length)
            throw new IllegalArgumentException("Argument arrays must be of the same length");
        byte[] result = new byte[first.length];
        for (int i = 0; i < result.length; i++)
            result[i] = (byte) (first[i] ^ second[i]);
        return result;
    }

    public byte[] hi(byte[] str, byte[] salt, int iterations) throws InvalidKeyException {
        mac.init(new SecretKeySpec(str, mac.getAlgorithm()));
        mac.update(salt);
        byte[] u1 = mac.doFinal(new byte[]{0, 0, 0, 1});
        byte[] prev = u1;
        byte[] result = u1;
        for (int i = 2; i <= iterations; i++) {
            byte[] ui = hmac(str, prev);
            result = xor(result, ui);
            prev = ui;
        }
        return result;
    }

    public byte[] normalize(String str) {
        return toBytes(str);
    }

    public byte[] saltedPassword(String password, byte[] salt, int iterations) throws InvalidKeyException {
        return hi(normalize(password), salt, iterations);
    }

    public byte[] clientKey(byte[] saltedPassword) throws InvalidKeyException {
        return hmac(saltedPassword, toBytes("Client Key"));
    }

    public byte[] storedKey(byte[] clientKey) {
        return hash(clientKey);
    }

    public String saslName(String username) {
        String replace1 = EQUAL.matcher(username).replaceAll(Matcher.quoteReplacement("=3D"));
        return COMMA.matcher(replace1).replaceAll(Matcher.quoteReplacement("=2C"));
    }

    public String username(String saslName) {
        String username = EQUAL_TWO_C.matcher(saslName).replaceAll(Matcher.quoteReplacement(","));
        if (EQUAL_THREE_D.matcher(username).replaceAll(Matcher.quoteReplacement("")).indexOf('=') >= 0) {
            throw new IllegalArgumentException("Invalid username: " + saslName);
        }
        return EQUAL_THREE_D.matcher(username).replaceAll(Matcher.quoteReplacement("="));
    }

    public String authMessage(String clientFirstMessageBare, String serverFirstMessage, String clientFinalMessageWithoutProof) {
        return clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;
    }

    public byte[] clientSignature(byte[] storedKey, ScramMessages.ClientFirstMessage clientFirstMessage, ScramMessages.ServerFirstMessage serverFirstMessage, ScramMessages.ClientFinalMessage clientFinalMessage) throws InvalidKeyException {
        byte[] authMessage = authMessage(clientFirstMessage, serverFirstMessage, clientFinalMessage);
        return hmac(storedKey, authMessage);
    }

    public byte[] clientProof(byte[] saltedPassword, ScramMessages.ClientFirstMessage clientFirstMessage, ScramMessages.ServerFirstMessage serverFirstMessage, ScramMessages.ClientFinalMessage clientFinalMessage) throws InvalidKeyException {
        byte[] clientKey = clientKey(saltedPassword);
        byte[] storedKey = hash(clientKey);
        byte[] clientSignature = hmac(storedKey, authMessage(clientFirstMessage, serverFirstMessage, clientFinalMessage));
        return xor(clientKey, clientSignature);
    }

    private byte[] authMessage(ScramMessages.ClientFirstMessage clientFirstMessage, ScramMessages.ServerFirstMessage serverFirstMessage, ScramMessages.ClientFinalMessage clientFinalMessage) {
        return toBytes(authMessage(clientFirstMessage.clientFirstMessageBare(),
                serverFirstMessage.toMessage(),
                clientFinalMessage.clientFinalMessageWithoutProof()));
    }

    public byte[] storedKey(byte[] clientSignature, byte[] clientProof) {
        return hash(xor(clientSignature, clientProof));
    }

    public byte[] serverKey(byte[] saltedPassword) throws InvalidKeyException {
        return hmac(saltedPassword, toBytes("Server Key"));
    }

    public byte[] serverSignature(byte[] serverKey, ScramMessages.ClientFirstMessage clientFirstMessage, ScramMessages.ServerFirstMessage serverFirstMessage, ScramMessages.ClientFinalMessage clientFinalMessage) throws InvalidKeyException {
        byte[] authMessage = authMessage(clientFirstMessage, serverFirstMessage, clientFinalMessage);
        return hmac(serverKey, authMessage);
    }

    public String secureRandomString() {
        return new BigInteger(130, random).toString(Character.MAX_RADIX);
    }

    public byte[] secureRandomBytes() {
        return toBytes(secureRandomString());
    }

    public byte[] toBytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    public ScramCredential generateCredential(String password, int iterations) {
        try {
            byte[] salt = secureRandomBytes();
            byte[] saltedPassword = saltedPassword(password, salt, iterations);
            byte[] clientKey = clientKey(saltedPassword);
            byte[] storedKey = storedKey(clientKey);
            byte[] serverKey = serverKey(saltedPassword);
            return new ScramCredential(salt, storedKey, serverKey, iterations);
        } catch (InvalidKeyException e) {
            throw new KafkaException("Could not create credential", e);
        }
    }
}