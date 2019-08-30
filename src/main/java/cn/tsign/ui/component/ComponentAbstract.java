package cn.tsign.ui.component;

public abstract class ComponentAbstract {

    public abstract String bulid();

    public String toTemplate() {
        return bulid();
    }

}
