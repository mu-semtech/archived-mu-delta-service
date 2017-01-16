package delta_service.callback;

import java.util.List;

/**
 * Created by langens-jonathan on 16.01.17.
 *
 * The aim for this class is to make the loading through jackson easier
 */
public class CallBackConfiguration {
    private List<String> potentials;
    private List<String> effectives;

    public List<String> getPotentials() {
        return potentials;
    }

    public void setPotentials(List<String> potentials) {
        this.potentials = potentials;
    }

    public List<String> getEffectives() {
        return effectives;
    }

    public void setEffectives(List<String> effectives) {
        this.effectives = effectives;
    }
}
