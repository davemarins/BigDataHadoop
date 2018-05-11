package lab3;

import java.util.Vector;

public class TopKVector<T extends Comparable<T>> {

    private Vector<T> localK;
    private int k;

    public TopKVector(int k) {
        this.localK = new Vector<>();
        this.k = k;
    }

    public Vector<T> getLocalK() {
        return localK;
    }

    public void updateWithNewElement(T element) {
        if (this.localK.size() < k) {
            this.localK.add(element);
            this.sortAfterInsertNewElement();
        } else if (element.compareTo(this.localK.elementAt(k - 1)) > 0) {
            this.localK.setElementAt(element, k - 1);
            this.sortAfterInsertNewElement();
        }
    }

    private void sortAfterInsertNewElement() {
        T swap;
        for (int pos = this.localK.size() - 1;
             pos > 0 && this.localK.elementAt(pos).compareTo(this.localK.elementAt(pos - 1)) > 0;
             pos--) {
            swap = this.localK.elementAt(pos);
            this.localK.setElementAt(this.localK.elementAt(pos - 1), pos);
            this.localK.setElementAt(swap, pos - 1);
        }
    }

}
