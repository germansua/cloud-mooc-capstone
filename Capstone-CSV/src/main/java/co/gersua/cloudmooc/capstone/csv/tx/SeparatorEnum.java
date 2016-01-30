package co.gersua.cloudmooc.capstone.csv.tx;

public enum SeparatorEnum {

    NEW_LINE("\n"),
    TAB("\t");

    private String value;

    SeparatorEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
