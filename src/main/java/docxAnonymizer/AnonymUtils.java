package docxAnonymizer;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.security.MessageDigest;
import java.math.BigInteger;

public class AnonymUtils {

    private static boolean debug = false;

    public static void setDebug(boolean debug) {
        AnonymUtils.debug = debug;
    }

    public static List<String> preprocess(Elaborator elaborator) {
        return elaborator.preprocess().stream().map(StringBuilder::toString).collect(Collectors.toList());
    }

    public static List<String> getOriginal(Elaborator elaborator) {
        return elaborator.getPlainTexts().getPlainTexts().stream().map(StringBuilder::toString).collect(Collectors.toList());
    }

    public static List<EntryPoint> getEntryPoints(Elaborator elaborator) {
        return elaborator.getPlainTexts().getEntryPoints().stream().map(e -> new EntryPoint(null, e.getIndex_PlainText(), e.getFrom(), e.getTo())).collect(Collectors.toList());
    }

    public static String getKeepUnchanged(Elaborator elaborator){
        return elaborator.getToKeepViaConfig() + elaborator.getKeepUnchanged().stream().map(p -> "|" + p.getRegex()).collect(Collectors.joining());
    }

    public static List<EntryPoint> getUnchangeable(Elaborator elaborator){
        return markUnchangeableEntryPoints(getKeepUnchanged(elaborator), elaborator.getPlainTexts().getPlainTexts().stream().map(StringBuilder::toString).collect(Collectors.toList()));
    }

    public static List<EntryPoint> markUnchangeableEntryPoints(String regex, List<String> plainTexts) {
        List<EntryPoint> unchangeable = new ArrayList<>();
        for(int idx = 0; idx < plainTexts.size(); idx++) {
            String text = plainTexts.get(idx);
            int from = 0;
            boolean continua;
            Matcher matcher;
            do {
                continua = false;
                matcher = Pattern.compile(regex).matcher(text);
                if (matcher.find(from)) {
                    continua = true;
                    unchangeable.add(new EntryPoint(null, idx, matcher.start(), matcher.end()));
                    from = matcher.end() - 1;
                }
            } while (continua);
        }
        return unchangeable;
    }

    public static void postprocess(Elaborator elaborator, List<String> postprocessed, List<EntryPoint> eps){
        elaborator.postprocess(postprocessed.stream().map(StringBuilder::new).collect(Collectors.toList()), eps);
    }

    public static String anonymize(String minimized, List<EntryPoint> entryPoints, List<EntryPoint> unchangeable, Matcher matcher, String id) {
        int charToRemove, charRemoved = 0, currentRemove, charToAdd, gap;
        EntryPoint found;

        charToRemove = matcher.end() - matcher.start();
        charToAdd = id.length();
        if(debug) {
            if(charToRemove >= 0)
                System.out.print("       Rimossi " + charToRemove + " caratteri");
            else
                System.out.print("       Aggiunti " + charToRemove + " caratteri");
            System.out.println(". Assegnato l'ID: " + id);
        }
        found = null;

        //cerco il nodo contenente il nominativo che sto per minimizzare
        for(EntryPoint e : entryPoints) {
            //per il nodo contenente il nome da minimizzare reimposto gli indici
            if(matcher.start() >= e.getFrom() && matcher.start() < e.getTo() && found == null) {
                found = e;
                //calcolo i caratteri compresi tra l'inizio del match e la fine del nodo
                gap = e.getTo() - matcher.start();
                //il nodo contiene tutta la parola
                if(charToRemove <= gap) {
                    charRemoved = charToRemove;
                    charToRemove = 0;
                    e.setTo(e.getTo() - charRemoved + charToAdd);
                }
                //la parola e' presente anche nei nodi successivi
                else {
                    charRemoved = gap;
                    charToRemove -= charRemoved;
                    e.setTo(e.getTo() - charRemoved + charToAdd);
                }
            }
            //per tutti i nodi successivi a quello contenente il nome da minimizzare traslo gli indici
            if(found != null && e != found) {
                //traslo indietro gli indici, di un valore pari ai caratteri rimossi
                e.setFrom(e.getFrom() - charRemoved + charToAdd);
                e.setTo(e.getTo() - charRemoved + charToAdd);
                //verifico se rimuovere altri caratteri
                if(charToRemove != 0) {
                    //calcolo il numero di caratteri contenuti in un nodo
                    gap = e.getTo() - e.getFrom();
                    //il nodo contiene tutta la parola
                    if(charToRemove <= gap) {
                        currentRemove = charToRemove;
                        charRemoved += charToRemove;
                        charToRemove = 0;
                        e.setTo(e.getTo() - currentRemove);
                    }
                    //la parola e' presente anche nei nodi successivi
                    else {
                        currentRemove = gap;
                        charRemoved += gap;
                        charToRemove -= currentRemove;
                        e.setTo(e.getTo() - currentRemove);
                    }
                }
            }
        }
        if(found == null) {
            System.out.println("ASSURDO: matcher.find() ha trovato un match, ma nessun EntryPoint lo contiene");
        }
        // rimuovo un'occorrenza del nominativo alla volta per una gestione corretta degli entryPoints
        String old = minimized;
        minimized = matcher.replaceFirst(id);
        // aggiorno boundaries dei nodi contenente testo da non modificare
        propagateChanges(minimized, old, unchangeable);

        return minimized;
    }

    public static Optional<Matcher> search(String regex, String textValue, List<EntryPoint> unchangeable) {
        Matcher matcher = Pattern.compile(regex).matcher(textValue);
        if(matcher.find() && canChange(matcher, unchangeable)) {
            if(debug) {
                System.out.print("       Start index: " + matcher.start());
                System.out.print(", End index: " + matcher.end()); //lunghezza parola, ossia primo indice libero
                System.out.println(", Found: " + matcher.group());
            }
            return Optional.of(matcher);
        }
        else {
            return Optional.empty();
        }

    }

    public static String getAsUniqueFlatString(List<String> terminiNominativo) {
        String EXTRA = "ĿŀČčŘřŠšŽžĲĳŒœŸŐőŰűḂḃĊċḊḋḞḟĠġṀṁṠṡṪṫĀāĒēĪīŌōŪūİıĞğŞşẀẁẂẃŴŵŶŷǾǿẞ";
        String nominativo = "";

        for(String termine : terminiNominativo)
            nominativo += termine + " ";
        // mantengo solo i caratteri alfabetici
        return nominativo.replaceAll("[^a-zA-ZÀ-ÖØ-öø-ÿ " + EXTRA + "]", "").toUpperCase();
    }

    public static String getUniqueId(String name, int salt) {
        String hashedString = "NoSuchAlgorithmExc";
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest((salt + name).getBytes());
            BigInteger bigInt = new BigInteger(1, digest);
            hashedString = bigInt.toString(36);
        }
        catch(NoSuchAlgorithmException ignored){

        }
        return hashedString.substring(0, 12);
    }

    private static boolean canChange(Matcher matcher, List<EntryPoint> unchangeable) {
        for(EntryPoint e : unchangeable) {
            if((matcher.start() >= e.getFrom() && matcher.start() < e.getTo()) ||
                    (matcher.end() > e.getFrom() && matcher.end() <= e.getTo())) {
                if(debug) {
                    System.out.print("[SKIP] Start index: " + matcher.start());
                    System.out.print(", End index: " + matcher.end()); //lunghezza parola, ossia primo indice libero
                    System.out.println(", Found: " + matcher.group());
                }
                return false;
            }
        }
        return true;
    }

    private static void propagateChanges(String tmp, String old, List<EntryPoint> unchangeable) {
        int diff = tmp.length() - old.length();
        int disparityIndex = 0, upperBound = diff > 0 ? old.length() : tmp.length();
        // la stringa e' nuova solo da un certo indice in poi
        for(int i = 0; i < upperBound; i++) {
            if(old.charAt(i) != tmp.charAt(i))
                break;
            disparityIndex += 1;
        }
        // aggiorno i boundaries solo se nella zona di aggiornamento della stringa
        for(EntryPoint e : unchangeable) {
            if(e.getFrom() >= disparityIndex) {
                e.setFrom(e.getFrom() + diff);
                e.setTo(e.getTo() + diff);
            }
        }
    }

}
