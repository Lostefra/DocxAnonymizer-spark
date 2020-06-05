package docxAnonymizer;

import java.util.List;

public class SequentialWorker extends AbstractWorker {
    private final Elaborator elaborator;

    public SequentialWorker(List<Object> runNodes, List<Persona> persone, List<Persona> keepUnchanged,
                            String keepUnchangedExprFile, Boolean debug) {
        if(debug) {
            Persona.setDebug(true);
            Elaborator.setDebug(true);
        }
        if(keepUnchangedExprFile != null)
            this.elaborator = new Elaborator(runNodes, persone, keepUnchanged, keepUnchangedExprFile);
        else
            this.elaborator = new Elaborator(runNodes, persone, keepUnchanged);
    }

    public void work() {
        elaborator.work();
    }

}
